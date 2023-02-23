#!/usr/bin/env python

import pandas as pd
import numpy as np
import time
import timeit
import os

class NoCap:
  def __init__(self, opinions_fn, opinion_clusters_fn, courts_fn, dockets_fn, citation_fn):
    self._opinions_fn = opinions_fn
    self._opinion_clusters_fn = opinion_clusters_fn
    self._courts_fn = courts_fn
    self._dockets_fn = dockets_fn
    self._citation_fn = citation_fn

    self._df_courts = self.init_courts_df()
    self._df_opinions = self.init_opinions_df()
    self._df_opinion_clusters = self.init_opinion_clusters_df()
    self._df_dockets = self.init_dockets_df()
    self._df_citation = self.init_citation_df()

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
  def csv_to_df(self, filename, dtype = None, parse_dates = None, max_gb=5):
    start = time.perf_counter()
    file_size = os.path.getsize(filename)
    file_size_gb = round(file_size/10**9, 2)
    print("File Size is :", file_size_gb, "GB")
    print(f'Importing {filename} as a dataframe')
    df = None
    if file_size_gb > max_gb:
        df = pd.concat(read_csv_as_dfs(filename, num_dfs=10**5, max_rows=10**7, 
                                       dtype=dtype, parse_dates=parse_dates))
    else:
        df = pd.read_csv(filename, dtype=dtype, parse_dates=parse_dates)
    end = time.perf_counter()
    print(f'{filename} read in {int((end-start)/60)} minutes')
    return df

  def df_row_by_value(self, df, column, match):
    return df.loc[df[column] == match]

  def get_columns_series(self, df):
    return [df[col] for col in list(df.columns)]

  def col_value(self, col):
    if not col.isna():
        return col.to_numpy()[0]

  def is_pd_series(self, col):
    return isinstance(col, pd.core.series.Series)

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
    self._df_courts = self.csv_to_df(courts_fn or self._courts_fn)

  # initialize opinions df
  def init_opinions_df(self, fn=None):
      self._df_opinions = self.csv_to_df(fn or self._opinions_fn)

  # initialize opinion clusters df
  def init_opinion_clusters_df(self, fn=None):
      self._df_op_clusters = self.csv_to_df(fn or self._opinion_clusters_fn)

  # initialize dockets df
  def init_dockets_df(self, fn=None):
      self._df_dockets = self.csv_to_df(fn or self._dockets_fn)

  # initialize citation map df
  def init_citation_df(self):
      self._df_citations = self.csv_to_df(self._citation_fn)
