#!/usr/bin/env python

import pandas as pd
import numpy as np
import time
import timeit
import os

DataFrame = pd.core.frame.DataFrame

## Helper Functions
# for reading large chunkfiles
def read_csv_as_dfs(filename, 
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
def csv_to_df(filename, dtype = None, parse_dates = None, max_gb=5):
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


## Read the csv files
### Courts
courts_filename = 'courts-2022-12-31.csv'
df_courts = csv_to_df(courts_filename)

### Dockets
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

dockets_filename = 'dockets-2022-12-31.csv'
file_size = os.path.getsize(dockets_filename)
dfs_dockets = csv_to_df(dockets_filename, dtype=my_types, parse_dates=parse_dates)

### Opinion Clusters
opinion_clusters_filename ='opinion-clusters-2022-12-31.csv'
df_opinion_clusters = csv_to_df(opinion_clusters_filename)

### Citation Map
citation_map_filename = 'citation-map-2022-12-31.csv'
df_citation_map = csv_to_df(citation_map_filename)

### Get Opinions

# Read a million rows divided into 10 data frames
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
opinions_filename = 'opinions-2022-12-31.csv'
dfs_opinions = read_csv_as_dfs(opinions_filename, num_dfs=num_dfs, dtype=opinion_dtypes)
df_opinions = dfs_opinions[0]
#df_opinions = pd.concat(dfs_opinions)

### CourtListener

def df_row_by_value(df, column, match):
    return df.loc[df[column] == match]

def get_columns_series(df):
    return [df[col] for col in list(df.columns)]

def col_value(col):
    if not col.isna():
        return col.to_numpy()[0]

def is_pd_series(col):
    return isinstance(col, pd.core.series.Series)

# accepts a series -- a row from a dataframe
def get_opinion_text(opinion):
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

def get_citations(opinion_id, df_citations):
    cites_to = df_citations[df_citations['citing_opinion_id'] == opinion_id]['cited_opinion_id'].to_list()
    cited_by = df_citations[df_citations['cited_opinion_id'] == opinion_id]['citing_opinion_id'].to_list()
    return {
        'cites_to':cites_to,
        'cited_by':cited_by
    }
    
# accepts dataframes
# opinion is a dataframe row! i.e. a dataframe with one row in it excluding headers
def process(taxonomy:dict, opinion: DataFrame, opinion_clusters: DataFrame, courts: DataFrame, 
            dockets: DataFrame, citations: DataFrame) -> dict:
    opinion_id = opinion['id']
    cluster_id = opinion['cluster_id']
    # get each corresponding row from clusters, dockets, courts based on opinion id
    cluster_row: DataFrame = df_row_by_value(opinion_clusters, 'id', cluster_id)
    # get corresponding row from docket df based on cluster opinion id
    docket_id = int(cluster_row['docket_id'])
    docket_row: DataFrame = dockets[dockets['id'] == docket_id]
    # return early if there's 
    if docket_row.empty:
        return 
    court_row: DataFrame = courts[courts['id'] == docket_row['court_id'].iloc[0]]
    if court_row.empty:
        return
    #get opinions cited to
    citation_info = get_citations(opinion_id, citations)
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
                'text': get_opinion_text(opinion), 
                'author': '', 'type': ''}]
            }
        }

    }
    return obj

start = time.perf_counter()
# hard coding 100 for now
json = df_opinions.sample(100)[[
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
        lambda row: process(
            taxonomy, 
            row, # force row to be a DataFrame than series
            df_opinion_clusters, 
            df_courts, 
            dfs_dockets, 
            df_citation_map)
          ,
        axis=1,
        )
end = time.perf_counter()
print((end-start)/60)

def main():
  pass


if __name__ == "__main__":
  main()
