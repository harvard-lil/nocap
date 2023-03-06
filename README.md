# Easy Bulk export, no cap
This repository provides scripts and notebooks that make it easy to export data in bulk from CourtListener's freely available downloads.

##
* [x] Create first version of notebook suitable for Data Scientists
  * [x] Create the appropriate _dtypes_ to optimize panda storage
  * [x] Select necessary cols _usecols_, for example 'created_by' date field indicating a database _insert_ isn't necessary
  * [x] Read the _opinions.csv_ (190+gb) chunk at a time from disk while converting into JSON
* [ ] Create a standalone script that can be piped to other tools
  * [x] Create PyPi library using [Poetry](https://python-poetry.org/): [package](https://pypi.org/project/lil-nocap)
  * [x] Output script using [json lines](https://jsonlines.org/examples/) format
* [ ] Improve speed by using [DASK DataFrame](https://docs.dask.org/en/stable/dataframe.html)

