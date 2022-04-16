# ids-project
Analysis using Censys and ANT datasets to analyze effectiveness of ping as a measurement tool for IP Census  
## Requirements  
This project has a bunch of moving parts, so make sure you have all the required permissions for datasets/APIs and libraries.  
### Datasets  
1. ANT ISI IP History dataset: I used [this](https://ant.isi.edu/datasets/readmes/internet_address_history_it95w-20210727.README.txt) dataset. Make sure you have the dataset downloaded and uncompressed.  
2. Censys: Request research access for Censys on [this](https://support.censys.io/hc/en-us/articles/360038761891-Research-Access-to-Censys-Data) page.   
### APIs  
1. BiqQuery: Make sure you have a Google Cloud Platform account and it is enabled for BigQuery. 
  - Serive account for BigQuery: I found it helpful to create a service account for performing queries with Python scripts. Google has very good [documentation](https://cloud.google.com/bigquery/docs/authentication) on this.  
  - If you are creating datasets/tables, make sure you choose one data center and stick to it.  
### Libraries  
1. Python libraries for data processing:  
```
$ pip3 install google-cloud-bigquery  
$ *python -m pip install "dask[dataframe]"   
$ pip3 install pandas matplotlib numpy    
$ pip3 install google-api-python-client google-auth-httplib2 google-auth-oauthlib  
$ pip3 install notebook
```
