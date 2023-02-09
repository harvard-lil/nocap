# Easy Bulk export, no cap
This repository provides scripts and notebooks that make it easy to export data in bulk from CourtListener's freely available downloads.

##
* [ ] Create first version of notebook suitable for Data Scientists
  * [ ] Create the appropriate _dtypes_ to optimize panda storage
  * [ ] Select necessary cols _usecols_, for example 'created_by' date field indicating a database _insert_ isn't necessary
  * [ ] Read the _opinions.csv_ (30+gb) chunk at a time from disk while converting into JSON
* [ ] Create a standalone script that can be piped to other tools
  * [ ] Create pip library using [Poetry](https://python-poetry.org/)
  * [ ] Output script using [json lines](https://jsonlines.org/examples/) format
