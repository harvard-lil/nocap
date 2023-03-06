#!/usr/bin/env 
import pandas as pd
import numpy as np
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

  @click.command()  
  @click.option('-o', help="local path to opinions file", required=True)
  @click.option('-oc', help="local path to opinion clusters file", required=True)
  @click.option('-c', help="local path to courts file ", required=True)
  @click.option('-d', help="local path to dockets file", required=True)
  @click.option('-cm', help="local path to citation map file", required=True)
  def cli(self, o, oc, c, d, cm):
    nc = NoCap(o, oc, c, d, cm)
    click.echo('hi')

  def read_csv_as_dfs(self, filename, 
                    num_dfs=10, 
                    max_rows=10**5, 
                    dtype=None, 
                    parse_dates=None,
                    usecols=None
                   ):
    counter = 0
    dfs_opinions = []
    for df in pd.read_csv(filename, chunksize=max_rows, dtype=dtype, 
                          parse_dates=parse_dates, usecols=usecols):
        if counter >= num_dfs:
            break
        dfs_opinions.append(df)
        counter = counter + 1
    return dfs_opinions

  # get a potentially large csv and turn it into a DataFrame
  def csv_to_df(self, filename, dtype = None, parse_dates = None, max_gb=5, num_dfs=10**3):
    start = time.perf_counter()
    file_size = os.path.getsize(filename)
    file_size_gb = round(file_size/10**9, 2)
    print(f'Importing {filename} as a dataframe')
    print("File Size is :", file_size_gb, "GB")
    df = None
    if file_size_gb > max_gb:
        df = pd.concat(self.read_csv_as_dfs(filename, num_dfs=10**5, max_rows=10**7, 
                                       dtype=dtype, parse_dates=parse_dates))
    else:
        df = pd.read_csv(filename, dtype=dtype, parse_dates=parse_dates)
    end = time.perf_counter()
    print(f'{filename} read in {int((end-start)/60)} minutes')
    return df

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
    return self.csv_to_df(fn or self._courts_fn)

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

      return self.csv_to_df(fn or self._opinions_fn, dtype=opinion_dtypes)

  # initialize opinion clusters df
  def init_opinion_clusters_df(self, fn=None):
      return self.csv_to_df(fn or self._opinion_clusters_fn)

  # initialize dockets df
  def init_dockets_df(self, fn=None):
      parse_dates = [
      'date_cert_granted', 
      'date_cert_denied', 
      'date_argued',
      'date_reargued',
      'date_reargument_denied',
      'date_filed',
      'date_terminated',
      'date_last_filing',
      'date_blocked'
      ]

      my_types = {
      'appeal_from_str': 'string',
      'assigned_to_str': 'string',
      'referred_to_str': 'string',
      'case_name_short' : 'string',
      'case_name': 'string',
      'case_name_full': 'string',
      'court_id': 'string',
      'cause':'string',
      'nature_of_suit':'string',
      'jury_demand':'string',
      'jurisdiction_type':'string',
      'appellate_fee_status':'string',
      'appellate_case_type_information':'string',
      'mdl_status':'string',
      'filepath_ia':'string',
      } 

      return self.csv_to_df(fn or self._dockets_fn, dtype=my_types, parse_dates=parse_dates)

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

  def start(self):
    start = time.perf_counter()
    max_rows = 10000
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

    for df in pd.read_csv(self._opinions_fn, chunksize=max_rows, dtype=opinion_dtypes, parse_dates=None, usecols=None):
      print('Now reading opinions')
      json = df[[
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
    end = time.perf_counter()
    print((end-start)/60)


if __name__ == '__main__':
  NoCap.cli()
   

