#!/usr/bin/env 
import pandas as pd
from pandas._libs.missing import NAType
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
import pickle

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
    self._df_dockets = self.get_pickled_docket() #self.init_dockets_dict() # self.init_dockets_df()

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

  def init_opinion_clusters_dict(self, fn=None):
    fn = fn or self._opinion_clusters_fn
    file_size = os.path.getsize(fn)
    file_size_gb = round(file_size/10**9, 2)
    log.debug(f'Importing {fn} as a Dict')
    msg = f"File Size is : {str(file_size_gb)} GB"
    log.debug(msg)

    start = time.perf_counter()
    cluster_dict = {}
    with open(fn) as dockets:
        reader = csv.DictReader(dockets)
        for row in reader:
            cluster_dict[int(row['id'])] = row['judges'], row['docket_id'], row['case_name'], row['case_name_full']
    end = time.perf_counter()
    log.debug(f'{fn} read in {str(int((end-start)/60))} minutes')

  # initialize opinion clusters df
  def init_opinion_clusters_df(self, fn=None):
      usecols = ['id', 'judges', 'docket_id', 'case_name', 'case_name_full']
      dtypes = {
        'judges':'string',
        'case_name':'string',
        'case_name_full':'string'
      }
      return self.csv_to_df(fn or self._opinion_clusters_fn, dtype=dtypes, usecols=usecols)

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


  # initialize dockets dict
  def init_dockets_dict(self, fn=None):
    fn = fn or self._dockets_fn
    file_size = os.path.getsize(fn)
    file_size_gb = round(file_size/10**9, 2)
    log.debug(f'Importing {fn} as a Dict')
    msg = f"File Size is : {str(file_size_gb)} GB"
    log.debug(msg)

    start = time.perf_counter()
    dockets_dict = {}
    with open(fn) as dockets:
        reader = csv.DictReader(dockets)
        for row in reader:
            dockets_dict[int(row['id'])] = row['court_id'], row['date_terminated']

    end = time.perf_counter()
    log.debug(f'{fn} read in {str(int((end-start)/60))} minutes')

    return dockets_dict
  
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

  def get_pickled_docket(self, fn='docket'):
    file = open('docket', 'rb')
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
            return ''
        return super(NpEncoder, self).default(obj)


  ## get cluster_row
  def get_cluster_row(self, opinion):
    # get each corresponding row from clusters, dockets, courts based on opinion id
    cluster_id = opinion['cluster_id']
    return self.df_row_by_value(self.get_opinions_cluster_df(), 'id', cluster_id)
   

  ## Process Opinion Dataframe
  def process_df(self, df):
    log.debug('process_df: client.map')
    log.debug('printing mini opinion dataframe as a dict')
    df_dict = df.to_dict('records')
    npartitions = mp.cpu_count()
    bag_opinions = db.from_sequence(df_dict, npartitions=npartitions)
    
    # get cluster rows
    log.debug('getting cluster rows')
    cluster_rows = list(map(self.get_cluster_row, df_dict))
    bag_clusters = db.from_sequence(cluster_rows, npartitions=npartitions)

    # dockets
    log.debug('getting dockets')
    dockets = self.get_dockets_df()
    # get the docekt_id as an int from each cluster_row in orderd as a key to hash into the dockets dict 
    docket_rows = list(map(lambda x: dockets[int(x['docket_id'])], cluster_rows))
    bag_dockets = db.from_sequence(docket_rows, npartitions=npartitions)


    # process row
    log.debug('running map(process_row) in parallel')
    # , bag_clusters, bag_dockets
    # db.map(print, b).compute()
    #import pdb; pdb.set_trace() 

    #docket_obj = list(map(
    # results = list(map(self.process_row, df_dict, cluster_rows, docket_rows))
    db.map(self.process_row, bag_opinions, bag_clusters, bag_dockets).to_textfiles('data/nocap/nc*.jsonl')
    # with open('nocap.jsonl', 'a') as f:
    #  f.write(f'{results}\n')
    # print(results)
 
  ## Helper function to process each row in the opinions dataframe
  # This is a helper function that connects opinions to courts, dockets, citations etc.,
  def process_row(self, opinion, cluster_row, docket_row) -> dict:
    
    courts = self.get_courts_df()
    citations = self.get_citation_df()
    opinion_id = opinion['id']
    cluster_id = opinion['cluster_id']
    
    # get corresponding row from docket df based on cluster opinion id
       
    # return early if there's 
    if not docket_row:
        return
    court_id_index = 0
    cid = docket_row[court_id_index]
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
    date_terminated_index = 1
    date_terminated = docket_row[date_terminated_index]
    ## download url may also be missing
    url = '' # opinion['download_url']

    obj = {
        'id': cluster_id,
        'url': url,
        'name_abbreviation': cluster_row.case_name.iloc[0],
        'name' : cluster_row.case_name_full.iloc[0],
        'decision_date': '', #date_terminated,
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
    return json.dumps(obj, cls=self.NpEncoder)

    
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
    # client = Client(threads_per_worker=4, n_workers = 1) #int(mp.cpu_count() * .30))
    # client
    for df in pd.read_csv(self._opinions_fn, chunksize=max_rows, dtype=opinion_dtypes, parse_dates=None, usecols=usecols):
      log.debug(f'Now reading {len(df)} opinions')
      
      self.process_df(df)
    end = time.perf_counter()
    log.debug((end-start)/60)

if __name__ == '__main__':
  NoCap.cli()
