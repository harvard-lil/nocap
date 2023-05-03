#!/usr/bin/env
import pandas as pd
from pandas._libs.missing import NAType
import numpy as np
import multiprocessing as mp
import time
import os
import click
import re
import logging
from sqlitedict import SqliteDict
from tqdm.auto import tqdm
from pathlib import Path
import multiprocessing as mp


log = logging.getLogger()
log.setLevel(logging.DEBUG)
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import csv
csv.field_size_limit(1_000_000)
import pickle
import bz2
import threading

class NoCap:
    def __init__(
        self, opinions_fn, opinion_clusters_fn, courts_fn, dockets_fn, citation_fn
    ):
        log.debug("Initializing")
        self._opinions_fn = opinions_fn
        self._opinion_clusters_fn = opinion_clusters_fn
        self._courts_fn = courts_fn
        self._dockets_fn = dockets_fn
        self._citation_fn = citation_fn

        self._df_courts = self.init_courts_df()
        self._df_opinion_clusters = self.init_opinion_clusters_dict(
            opinion_clusters_fn, "opinion_clusters.sqlite"
        )
        self.init_citation_dict() #self.init_citation_df()
        self._df_dockets = self.init_dockets_dict(dockets_fn, "dockets.sqlite")

        # self._df_opinions = self.init_opinions_df()
        self.DataFrame = pd.core.frame.DataFrame

    @click.command()
    @click.option("-o", help="local path to opinions file", required=True)
    @click.option("-oc", help="local path to opinion clusters file", required=True)
    @click.option("-c", help="local path to courts file ", required=True)
    @click.option("-d", help="local path to dockets file", required=True)
    @click.option("-cm", help="local path to citation map file", required=True)
    def cli(o, oc, c, d, cm):
        NoCap(o, oc, c, d, cm).start()

    def read_csv_as_dfs(
        self,
        filename,
        num_dfs=10,
        max_rows=10**5,
        dtype=None,
        parse_dates=None,
        usecols=None,
        index_col=None,
    ):
        counter = 0
        dfs_opinions = []
        for df in pd.read_csv(
            filename,
            chunksize=max_rows,
            dtype=dtype,
            parse_dates=parse_dates,
            usecols=usecols,
            index_col=index_col,
        ):
            if counter >= num_dfs:
                break
            dfs_opinions.append(df)
            counter = counter + 1
        return dfs_opinions

    # get a potentially large csv and turn it into a DataFrame
    def csv_to_df(
        self,
        filename,
        dtype=None,
        parse_dates=None,
        max_gb=5,
        num_dfs=10**3,
        usecols=None,
        index_col=None,
    ):
        start = time.perf_counter()
        file_size = os.path.getsize(filename)
        file_size_gb = round(file_size / 10**9, 2)
        log.debug(f"Importing {filename} as a dataframe")
        msg = f"File Size is : {str(file_size_gb)} GB"
        log.debug(msg)
        df = None
        if file_size_gb > max_gb:
            df = pd.concat(
                self.read_csv_as_dfs(
                    filename,
                    num_dfs=10**5,
                    max_rows=10**7,
                    dtype=dtype,
                    parse_dates=parse_dates,
                    usecols=usecols,
                    index_col=index_col,
                )
            )
        else:
            df = pd.read_csv(
                filename,
                dtype=dtype,
                parse_dates=parse_dates,
                usecols=usecols,
                index_col=index_col,
            )
        end = time.perf_counter()
        log.debug(f"{filename} read in {str(int((end-start)/60))} minutes")
        return df

    # get cluster client
    def get_cluster_client(self):
        return self._client

    def df_row_by_value(self, df, column, match):
        return df.loc[df[column] == match]

    # accepts a series -- a row from a dataframe
    def get_opinion_text(self, opinion):
        text = ""
        pt = opinion["plain_text"]
        hl = opinion["html"]
        hlb = opinion["html_lawbox"]
        hlc = opinion["html_columbia"]
        xh = opinion["xml_harvard"]
        hla = opinion["html_anon_2020"]

        if isinstance(pt, str):
            text = pt
        elif isinstance(hl, str):
            text = hl
        elif isinstance(hlb, str):
            text = hlb
        elif isinstance(hlc, str):
            text = hlc
        elif isinstance(xh, str):
            text = xh
        elif isinstance(hla, str):
            text = hla
        return text

   #def get_citations(self, opinion_id, df_citations):
       

    # initialize courts df
    def init_courts_df(self, fn=None):
        usecols = ["id", "full_name", "jurisdiction"]
        dtypes = {"fullname": "string", "jurisdiction": "string"}
        return self.csv_to_df(fn or self._courts_fn, dtype=dtypes, usecols=usecols)

    # initialize opinions df
    def init_opinions_df(self, fn=None):
        opinion_dtypes = {
            "download_url": "string",
            "local_path": "string",
            "plain_text": "string",
            "html": "string",
            "html_lawbox": "string",
            "html_columbia": "string",
            "html_anon_2020": "string",
            "html_with_citations": "string",
            "local_path": "string",
        }

        return self.csv_to_df(
            fn or self._opinions_fn, dtype=opinion_dtypes, index_col="id"
        )

    # initialize opinion clusters df
    def init_opinion_clusters_df(self, fn=None):
        usecols = ["id", "judges", "docket_id", "case_name", "case_name_full"]
        dtypes = {"judges": "string", "case_name": "string", "case_name_full": "string"}
        return self.csv_to_df(
            fn or self._opinion_clusters_fn, dtype=dtypes, usecols=usecols
        )

    # initialize dockets df
    def init_dockets_df(self, fn=None):
        my_types = {"court_id": "string", "date_terminated": "string"}

        log.debug("importing dockets")
        file_size = os.path.getsize(self._opinions_fn)
        file_size_gb = round(file_size / 10**9, 2)
        log.debug(f"Importing {fn} as a Dask Dataframe")
        msg = f"File Size is : {str(file_size_gb)} GB"
        log.debug(msg)

        return dd.read_csv(
            fn or self._dockets_fn,
            dtype=my_types,
            usecols=["court_id", "id", "date_terminated"],
            blocksize="32MB",
        )

    @staticmethod
    def bz2_csv_rows(fp):
        with bz2.open(fp, mode="rt", newline="") as bzfp:
            for row in csv.reader(bzfp):
                yield row

    @classmethod
    def bz2_csv_reader(cls, fn):
        keys = []
        for i, row in enumerate(cls.bz2_csv_rows(fn)):
            if i == 0:
                keys = row
            else:
                yield dict(zip(keys, row))

    @staticmethod
    def opinion_clusters_row_to_kv(row):
        fields = ["judges", "docket_id", "case_name", "case_name_full", "headnotes"]
        return row["id"], {f: row[f] for f in fields}

    @staticmethod
    def docket_row_to_kv(row):
        fields = ["court_id", "date_terminated"]
        return row["id"], {f: row[f] for f in fields}

    @staticmethod
    def sqlitedict_reader(sql_fn):
        return SqliteDict(sql_fn, flag="r", journal_mode="OFF", outer_stack=False)

    def build_sqlitedict(self, csv_fn, sql_fn, row_to_kv, batch_size=1_000_000):
        with SqliteDict(sql_fn, journal_mode="OFF", outer_stack=False) as db:
            for i, row in tqdm(enumerate(self.bz2_csv_reader(csv_fn)), smoothing=0):
                k, v = row_to_kv(row)
                db[k] = v
                if i % batch_size == 0:
                    db.commit()
            db.commit()
        return self.sqlitedict_reader(sql_fn)

    def init_dockets_dict(self, fn, sql_fn, batch_size=1_000_000):
        if not Path(sql_fn).exists():
            self.build_sqlitedict(fn, sql_fn, self.docket_row_to_kv, batch_size)
        return self.sqlitedict_reader(sql_fn)

    def init_opinion_clusters_dict(self, fn, sql_fn, batch_size=1_000_000):
        if not Path(sql_fn).exists():
            self.build_sqlitedict(
                fn, sql_fn, self.opinion_clusters_row_to_kv, batch_size
            )
        return self.sqlitedict_reader(sql_fn)

    # initialize citation map df
    def init_citation_df(self, fn=None):
        return self.csv_to_df(fn or self._citation_fn)

    def init_citation_dict(self, fn=None):
        start = time.perf_counter()
        usecols = ['cited_opinion_id', 'citing_opinion_id']
        log.debug('initializing citation csv to be a dict')
        df_dict = self.csv_to_df(fn or self._citation_fn, usecols=usecols).to_dict("records")
        self.cites_to = {}
        self.cited_by = {}
        list(map(lambda x: self.cites_to.setdefault(x['citing_opinion_id'], []).append(x['cited_opinion_id']), df_dict))
        list(map(lambda x: self.cited_by.setdefault(x['cited_opinion_id'], []).append(x['citing_opinion_id']), df_dict))
        log.debug('finished converting citation dataframe into a dict')
        end = time.perf_counter()
        log.debug((end - start) / 60)


    ## Getters
    def get_courts_df(self):
        return self._df_courts

    def get_citation_df(self):
        return self._df_citation

    def get_dockets_df(self):
        return self._df_dockets

    def get_opinions_cluster_df(self):
        return self._df_opinion_clusters

    def get_opinions_df(self):
        pass

    def get_pickled_docket(self, fn="docket"):
        file = open("docket", "rb")
        self._df_dockets = pickle.load(file)
        file.close()
        return self._df_dockets

    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, NAType):
                return ""
            return super(NpEncoder, self).default(obj)

    ## get cluster_row
    def get_cluster_row(self, opinion):
        # get each corresponding row from clusters, dockets, courts based on opinion id
        cluster_id = opinion["cluster_id"]
        return self.get_opinions_cluster_df()[cluster_id]

    ## Process Opinion Dataframe
    def process_df(self, df):
        log.debug("process_df: client.map")
        log.debug("printing mini opinion dataframe as a dict")
        df_dict = df.to_dict("records")

        # get cluster rows
        log.debug("getting cluster rows")
        cluster_rows = list(map(self.get_cluster_row, df_dict))

        # dockets
        log.debug("getting dockets")
        dockets = self.get_dockets_df()
        # get the docekt_id as an int from each cluster_row in orderd as a key to hash into the dockets dict
        docket_rows = list(map(lambda x: dockets[int(x["docket_id"])], cluster_rows))

        # process row
        log.debug("running map(process_row) in parallel")
        return list(map(self.process_row, df_dict, cluster_rows, docket_rows))

    ## Helper function to process each row in the opinions dataframe
    # This is a helper function that connects opinions to courts, dockets, citations etc.,
    def process_row(self, opinion, cluster_row, docket_row) -> dict:
        courts = self.get_courts_df()
        opinion_id = opinion["id"]
        cluster_id = opinion["cluster_id"]

        # get corresponding row from docket df based on cluster opinion id
        # return early if there's
        if not docket_row:
            return
        cid = docket_row["court_id"]
        court_row: self.DataFrame = courts[courts["id"] == cid]
        if court_row.empty:
            return

        # get opinions cited to
        cites_to = self.cites_to[opinion_id] if opinion_id in self.cites_to else []
        cited_by = self.cited_by[opinion_id] if opinion_id in self.cited_by else []

        # judges
        judges = cluster_row["judges"]
        judge_list = [
            judge
            for judge in (judges.split(",") if judges else [])
            if not re.match("[cj]?j\.", judge)
        ]
        #
        ## sometimes date_terminated may be missing and recorded as NaN
        date_terminated = docket_row["date_terminated"]
        ## download url may also be missing
        url = ""  # opinion['download_url']

        obj = {
            "id": cluster_id,
            "url": url,
            "name_abbreviation": cluster_row["case_name"],
            "name": cluster_row["case_name_full"],
            "decision_date": "",  # date_terminated,
            "docket_number": cluster_row["docket_id"],
            "citations": cited_by,
            "cites_to": cites_to,
            "court": {"name": court_row.full_name.iloc[0]},
            "jurisdiction": {"name": court_row.jurisdiction.iloc[0]},
            "casebody": {
                "data": {
                    "judges": judge_list,
                    "head_matter": cluster_row['headnotes'],
                    "opinions": [
                        {
                            "text": self.get_opinion_text(opinion),
                            "author": "",
                            "type": "",
                        }
                    ],
                }
            },
        }
        return json.dumps(obj, cls=self.NpEncoder)

    def start(self):
        start = time.perf_counter()
        max_rows = 1_000
        opinion_dtypes = {
            "download_url": "string",
            "local_path": "string",
            "plain_text": "string",
            "html": "string",
            "html_lawbox": "string",
            "html_columbia": "string",
            "html_anon_2020": "string",
            "html_with_citations": "string",
            "local_path": "string",
        }
        usecols = [
            "id",
            "download_url",
            "local_path",
            "cluster_id",
            "xml_harvard",
            "plain_text",
            "html",
            "html_lawbox",
            "html_columbia",
            "html_anon_2020",
            "local_path",
        ]
        file_size = os.path.getsize(self._opinions_fn)
        file_size_gb = round(file_size / 10**9, 2)
        log.debug(f"Importing {self._opinions_fn} as a dataframe")
        msg = f"File Size is : {str(file_size_gb)} GB"
        log.debug(msg)
        # We can use a with statement to ensure threads are cleaned up promptly
        chunks = pd.read_csv(
            self._opinions_fn,
            chunksize=max_rows,
            dtype=opinion_dtypes,
            parse_dates=None,
            usecols=usecols,
        )

        pbar = tqdm(desc="Processing opinions", smoothing=0)

        N_WORKERS = max_rows / mp.cpu_count()

        N_CHUNKS_PER_BATCH = int(mp.cpu_count() * 1.5)

        lock = threading.Lock()
        with ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
            futures = []
            for i, df in enumerate(chunks):
                futures.append(executor.submit(self.process_df, df))
                if (i + 1) % N_CHUNKS_PER_BATCH == 0:
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            pbar.update(max_rows)
                        except Exception as exc:
                            log.exception(exc)
                        else:
                             with lock:
                                with open('nocap_opinions.jsonl', 'a') as file:
                                  file.write(f'{result}')
        end = time.perf_counter()
        log.debug(f'Finished: {(end - start) / 60}')


if __name__ == "__main__":
    NoCap.cli()
