#!/usr/bin/env 
import pandas as pd
import numpy as np
import dask
import dask.bag as db
from dask.distributed import Client, progress
import dask.dataframe as dd
from dask.distributed import as_completed
import multiprocessing as mp
import time
import timeit
import os
import click
import re
import logging
log = logging.getLogger()
log.setLevel(logging.DEBUG)
import json
import csv

class NoCap:
  def __init__(self, opinions_fn, opinion_clusters_fn, courts_fn, dockets_fn, citation_fn):
    log.debug('Initializing')
    self._opinions_fn = opinions_fn
    self._opinion_clusters_fn = opinion_clusters_fn
    self._courts_fn = courts_fn
    self._dockets_fn = dockets_fn
    self._citation_fn = citation_fn

    self._df_courts = self.init_courts_df()
    self._df_opinion_clusters = self.init_opinion_clusters_df()
    self._df_citation = self.init_citation_df()
    self._df_dockets = self.init_dockets_dict() # self.init_dockets_df()

    #self._df_opinions = self.init_opinions_df()
    self.DataFrame = pd.core.frame.DataFrame

  @click.command()  
  @click.option('-o', help="local path to opinions file", required=True)
  @click.option('-oc', help="local path to opinion clusters file", required=True)
  @click.option('-c', help="local path to courts file ", required=True)
  @click.option('-d', help="local path to dockets file", required=True)
  @click.option('-cm', help="local path to citation map file", required=True)
  def cli(o, oc, c, d, cm):
    NoCap(o, oc, c, d, cm).start()
    
  def read_csv_as_dfs(self, filename, 
                    num_dfs=10, 
                    max_rows=10**5, 
                    dtype=None, 
                    parse_dates=None,
                    usecols=None,
                    index_col=None
                   ):
    counter = 0
    dfs_opinions = []
    for df in pd.read_csv(filename, chunksize=max_rows, dtype=dtype, 
                          parse_dates=parse_dates, usecols=usecols, index_col=index_col):
        if counter >= num_dfs:
            break
        dfs_opinions.append(df)
        counter = counter + 1
    return dfs_opinions

  # get a potentially large csv and turn it into a DataFrame
  def csv_to_df(self, filename, dtype = None, parse_dates = None, max_gb=5, num_dfs=10**3, usecols=None, index_col=None):
    start = time.perf_counter()
    file_size = os.path.getsize(filename)
    file_size_gb = round(file_size/10**9, 2)
    log.debug(f'Importing {filename} as a dataframe')
    msg = f"File Size is : {str(file_size_gb)} GB"
    log.debug(msg)
    df = None
    if file_size_gb > max_gb:
        df = pd.concat(self.read_csv_as_dfs(filename, num_dfs=10**5, max_rows=10**7, 
                                       dtype=dtype, parse_dates=parse_dates, usecols=usecols, index_col=index_col))
    else:
        df = pd.read_csv(filename, dtype=dtype, parse_dates=parse_dates, usecols=usecols, index_col=index_col)
    end = time.perf_counter()
    log.debug(f'{filename} read in {str(int((end-start)/60))} minutes')
    return df

  # get cluster client
  def get_cluster_client(self):
    return self._client

  def df_row_by_value(self, df, column, match):
    return df.loc[df[column] == match]
  
  # accepts a series -- a row from a dataframe
  def get_opinion_text(self, opinion):
    text = ''
    pt = opinion['plain_text']
    hl = opinion['html']
    hlb = opinion['html_lawbox']
    hlc = opinion['html_columbia']
    xh = opinion['xml_harvard']
    hla = opinion['html_anon_2020']
    
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

  def get_citations(self, opinion_id, df_citations):
    cites_to = df_citations[df_citations['citing_opinion_id'] == opinion_id]['cited_opinion_id'].to_list()
    cited_by = df_citations[df_citations['cited_opinion_id'] == opinion_id]['citing_opinion_id'].to_list()
    return {
        'cites_to':cites_to,
        'cited_by':cited_by
    }

  # initialize courts df
  def init_courts_df(self, fn=None):
    usecols = ['id','full_name','jurisdiction']
    dtypes = {
      'fullname': 'string',
      'jurisdiction': 'string'
    }
    return self.csv_to_df(fn or self._courts_fn, dtype=dtypes, usecols=usecols)

  # initialize opinions df
  def init_opinions_df(self, fn=None):
      opinion_dtypes = {
        'download_url': 'string',
        'local_path':'string',
        'plain_text':'string',
        'html':'string',
        'html_lawbox':'string',
        'html_columbia':'string',
        'html_anon_2020':'string',
        'html_with_citations':'string',
        'local_path':'string'
      } 

      return self.csv_to_df(fn or self._opinions_fn, dtype=opinion_dtypes, index_col='id')

  # initialize opinion clusters df
  def init_opinion_clusters_df(self, fn=None):
      usecols = ['id', 'judges', 'docket_id', 'case_name', 'case_name_full']
      dtypes = {
        'judges':'string',
        'case_name':'string',
        'case_name_full':'string'
      }
      return self.csv_to_df(fn or self._opinion_clusters_fn, dtype=dtypes, usecols=usecols)

  # initialize dockets dict
  def init_dockets_dict(self, fn=None):
    dockets_dict = {}
    with open(fn or self._dockets_fn) as dockets:
        reader = csv.DictReader(dockets)
        for row in reader:
            dockets_dict[int(row['id'])] = row['court_id'], row['date_terminated']
    return dockets_dict

  # initialize dockets df
  def init_dockets_df(self, fn=None):
      my_types = {
      'court_id': 'string',
      'date_terminated': 'string'
      } 

      log.debug('importing dockets')
      file_size = os.path.getsize(self._opinions_fn)
      file_size_gb = round(file_size/10**9, 2)
      log.debug(f'Importing {fn} as a Dask Dataframe')
      msg = f"File Size is : {str(file_size_gb)} GB"
      log.debug(msg)

      return dd.read_csv(fn or self._dockets_fn, dtype=my_types, usecols=['court_id', 'id', 'date_terminated'],
                         blocksize="32MB")

  # initialize citation map df
  def init_citation_df(self, fn=None):
      return self.csv_to_df(fn or self._citation_fn)

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
      
  # process
  def process_row(self, opinion) -> dict:
    log.debug('process_row')
    dockets = self.get_dockets_dict() # self.get_dockets_df()
    courts = self.get_courts_df()
    citations = self.get_citation_df()
    opinion_id = opinion['id']
    cluster_id = opinion['cluster_id']

    # get each corresponding row from clusters, dockets, courts based on opinion id
    cluster_row: self.DataFrame = self.df_row_by_value(self.get_opinions_cluster_df(), 'id', cluster_id)

    # get corresponding row from docket df based on cluster opinion id
    docket_id = int(cluster_row['docket_id'])
    docket_row: Dict = dockets[docket_id] #dockets[dockets['id'] == docket_id].compute()

    # return early if there's 
    if not docket_row:
        return 
    cid = docket_row['court_id']
    court_row: self.DataFrame = courts[courts['id'] == cid]
    if court_row.empty:
        return

    # get opinions cited to
    citation_info = self.get_citations(opinion_id, citations)
    cites_to = citation_info['cites_to']
    cited_by = citation_info['cited_by']

    # judges
    judges = cluster_row.judges
    judge_list = [
        judge
        for judge in
            (judges.iloc[0].split(',') if judges.notna().bool() else [])
        if not re.match('[cj]?j\.', judge)
    ]
    #    
    ## sometimes date_terminated may be missing and recorded as NaN
    date_terminated = docket_row['date_terminated']

    obj = {
        'id': cluster_id,
        'url': opinion['download_url'],
        'name_abbreviation': cluster_row.case_name.iloc[0],
        'name' : cluster_row.case_name_full.iloc[0],
        'decision_date': date_terminated,
        'docket_number' : cluster_row.docket_id.iloc[0],
        'citations' : cited_by,
        'cites_to' : cites_to,
        'court' : {'name': court_row.full_name.iloc[0]},
        'jurisdiction' : {'name': court_row.jurisdiction.iloc[0]},
        'casebody' : {'data': {
            'judges': judge_list,
            'head_matter':'', #Ask CL about copyright,
            'opinions': [{
                'text': self.get_opinion_text(opinion), 
                'author': '', 'type': ''}]
            }
        }
    }
    return obj

  def process_df(self, df):
    log.debug('process_df: client.map')
    df_dict = df.to_dict('records')
    b = db.from_sequence(df_dict, npartitions=10)
    results = db.map(self.process_row, b).map(json.dumps).to_textfiles('data/*.jsonl')
    print(results)
    log.debug('printing mini opinion dataframe as a dict')
    #log.debug(df_dict)
    #pdb.set_trace()
    #futures = client.map(self.process_row, df)
    
    #for future, result in as_completed(futures, with_results=True):

  def test(self):
    print('test')
  
  def start(self):
    start = time.perf_counter()
    max_rows = 50
    opinion_dtypes = {
        'download_url': 'string',
        'local_path':'string',
        'plain_text':'string',
        'html':'string',
        'html_lawbox':'string',
        'html_columbia':'string',
        'html_anon_2020':'string',
        'html_with_citations':'string',
        'local_path':'string'
    }
    usecols = [
           'id',
           'download_url', 
           'local_path',
           'cluster_id',
           'xml_harvard',
           'plain_text',
           'html',
           'html_lawbox',
           'html_columbia',
           'html_anon_2020',
           'local_path'           
          ]
    file_size = os.path.getsize(self._opinions_fn)
    file_size_gb = round(file_size/10**9, 2)
    log.debug(f'Importing {self._opinions_fn} as a dataframe')
    msg = f"File Size is : {str(file_size_gb)} GB"
    log.debug(msg)
    #client = Client(threads_per_worker=4, n_workers = int(mp.cpu_count() * .40))
    for df in pd.read_csv(self._opinions_fn, chunksize=max_rows, dtype=opinion_dtypes, parse_dates=None, usecols=usecols):
      log.debug(f'Now reading {len(df)} opinions')
      self.process_df(df)
    end = time.perf_counter()
    log.debug((end-start)/60)

if __name__ == '__main__':
  NoCap.cli()
