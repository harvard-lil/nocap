#!/usr/bin/env 
import pandas as pd
import numpy as np
import dask
from dask.distributed import Client, progress
import multiprocessing as mp
import time
import timeit
import os
import click

class NoCap:
  def __init__(self, opinions_fn, opinion_clusters_fn, courts_fn, dockets_fn, citation_fn):
    self._opinions_fn = opinions_fn
    self._opinion_clusters_fn = opinion_clusters_fn
    self._courts_fn = courts_fn
    self._dockets_fn = dockets_fn
    self._citation_fn = citation_fn

    self._df_courts = self.init_courts_df()
    self._df_opinion_clusters = self.init_opinion_clusters_df()
    self._df_citation = self.init_citation_df()
    self._df_dockets = self.init_dockets_df()

    #self._df_opinions = self.init_opinions_df()
    self.DataFrame = pd.core.frame.DataFrame

    self._client = None

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
    print(f'Importing {filename} as a dataframe')
    print("File Size is :", file_size_gb, "GB")
    df = None
    if file_size_gb > max_gb:
        df = pd.concat(self.read_csv_as_dfs(filename, num_dfs=10**5, max_rows=10**7, 
                                       dtype=dtype, parse_dates=parse_dates, usecols=usecols, index_col=None))
    else:
        df = pd.read_csv(filename, dtype=dtype, parse_dates=parse_dates, usecols=usecols, index_col=index_col)
    end = time.perf_counter()
    print(f'{filename} read in {int((end-start)/60)} minutes')
    return df

  # get cluster client
  def get_cluster_client(self):
    return self._client

  def df_row_by_value(self, df, column, match):
    return df.loc[df[column] == match]
  
  # accepts a series -- a row from a dataframe
  def get_opinion_text(self, opinion):
    text = ''
    pt = opinion.plain_text
    hl = opinion.html
    hlb = opinion.html_lawbox
    hlc = opinion.html_columbia
    xh = opinion.xml_harvard
    hla = opinion.html_anon_2020
    
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
    return self.csv_to_df(fn or self._courts_fn, usecols=usecols, index_col='id').sort_index()

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
      return self.csv_to_df(fn or self._opinion_clusters_fn, usecols=usecols, index_col='id').sort_index()

  # initialize dockets df
  def init_dockets_df(self, fn=None):
      parse_dates = [
      'date_terminated',
      ]

      my_types = {
      'court_id': 'string'
      } 

      return self.csv_to_df(fn or self._dockets_fn, dtype=my_types, parse_dates=parse_dates, usecols=['court_id', 'id', 'date_terminated'], index_col='id').sort_index()

  # initialize citation map df
  def init_citation_df(self, fn=None):
      return self.csv_to_df(fn or self._citation_fn, index_col='id').sort_index()

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
  def process(self, opinion) -> dict:
    dockets = self.get_dockets_df()
    courts = self.get_courts_df()
    citations = self.get_citation_df()
    opinion_id = opinion['id']
    cluster_id = opinion['cluster_id']

    # get each corresponding row from clusters, dockets, courts based on opinion id
    cluster_row: self.DataFrame = self.df_row_by_value(self.get_opinions_cluster_df(), 'id', cluster_id)

    # get corresponding row from docket df based on cluster opinion id
    docket_id = int(cluster_row['docket_id'])
    docket_row: self.DataFrame = dockets[dockets['id'] == docket_id]

    # return early if there's 
    if docket_row.empty:
        return 
    court_row: self.DataFrame = courts[courts['id'] == docket_row['court_id'].iloc[0]]
    if court_row.empty:
        return

    # get opinions cited to
    citation_info = self.get_citations(opinion_id, citations)
    cites_to = citation_info['cites_to']
    cited_by = citation_info['cited_by']

    #judges
    judges = cluster_row.judges
        
    obj = {
        'id': cluster_id,
        'url': opinion['download_url'],
        'name_abbreviation': cluster_row.case_name.iloc[0],
        'name' : cluster_row.case_name_full.iloc[0],
        'decision_date': docket_row.date_terminated.iloc[0],
        'docket_number' : cluster_row.docket_id.iloc[0],
        'citations' : cited_by,
        'cites_to' : cites_to,
        'court' : {'name': court_row.full_name.iloc[0]},
        'jurisdiction' : {'name': court_row.jurisdiction.iloc[0]},
        'casebody' : {'data': {
            'judges': judges.iloc[0].split(',') if judges.notna().bool() else [],
            'head_matter':'', #Ask CL about copyright,
            'opinions': [{
                'text': self.get_opinion_text(opinion), 
                'author': '', 'type': ''}]
            }
        }
    }
    return obj

  def _taxonify(self, df):
    df[[
            'id',
            'local_path',
            'download_url',
            'cluster_id',
            'xml_harvard',
            'plain_text',
            'html',
            'html_lawbox',
            'html_columbia',
            'html_anon_2020'
          ]].apply(
            lambda row: print(f'{self.process(row)}\n'),
            axis=1,
          )  

  def start(self):
    start = time.perf_counter()
    max_rows = 500
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
    file_size = os.path.getsize(self._opinions_fn)
    file_size_gb = round(file_size/10**9, 2)
    print(f'Importing {self._opinions_fn} as a dataframe')
    print("File Size is :", file_size_gb, "GB")
    self._client = Client(threads_per_worker=4, n_workers = int(mp.cpu_count()/2))
    print(self._client)
    lazy_results = []
    for df in pd.read_csv(self._opinions_fn, chunksize=max_rows, dtype=opinion_dtypes, parse_dates=None, usecols=None):
      #print('Now reading opinions')
      for index, row in df[[
            'id',
            'local_path',
            'download_url',
            'cluster_id',
            'xml_harvard',
            'plain_text',
            'html',
            'html_lawbox',
            'html_columbia',
            'html_anon_2020'
          ]].iterrows():

          lazy_result = dask.delayed(self.process)(row)
          lazy_results.append(lazy_result)
    results = dask.compute(*lazy_results)
    print(*results, sep='\n')
    
    end = time.perf_counter()
    print((end-start)/60)


if __name__ == '__main__':
  NoCap.cli()
   

