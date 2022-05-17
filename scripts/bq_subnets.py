from google.cloud import bigquery
import os
from google.oauth2 import service_account
import time
import sys
import json

project_id = 'censys-research-340718'
#table_id = 'ant_isi_ip_history.ip_history'
data_dir = 'address_history'
credentials = service_account.Credentials.from_service_account_file('credentials.json')
client = bigquery.Client(credentials=credentials)

table_id=project_id+'.censys_ips.subnets_20210223'
job_config = bigquery.QueryJobConfig(destination=table_id)
sql = """
    select distinct ipv4 as ip, concat(split(ipv4, '.')[SAFE_OFFSET(0)], '.', split(ipv4, '.')[SAFE_OFFSET(1)], '.', split(ipv4, '.')[SAFE_OFFSET(2)], '.0') as ip_subnet 
 FROM `censys-research-340718.censys_ips.snapshot_20210223`; 
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.

print("Query results loaded to the table {}".format(table_id))
