from google.cloud import bigquery
import os
from google.oauth2 import service_account
import time
import sys
import json

project_id = 'censys-research-340718'
table_id = 'ant_isi_ip_history.ip_history'
data_dir = 'address_history'
credentials = service_account.Credentials.from_service_account_file('credentials.json')
client = bigquery.Client(credentials=credentials)

def get_schema(filename):
    with open(filename) as fh:
        schemajson = json.load(fh)
        schema = []
        for rec in schemajson:
            schema.append(bigquery.SchemaField(rec['name'], rec['type'], rec['mode']))
    print('schema has ', len(schema), 'columns')
    return schema

def create_table(schema):
    table = bigquery.Table(project_id + '.' + table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )

def upload_censys_ips(folder):
    table = 'censys_ips.snapshot_20201124'
    files = [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
    print(files)
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV)
    for f in files:
        print(f)
        with open(f, 'rb') as source_file:
            job = client.load_table_from_file(source_file, table, job_config=job_config)
        job.result()

def upload_data(schema):
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, schema=schema)
    fail_list = []
    while True:
        files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if os.path.isfile(os.path.join(data_dir, f))]
        files.sort(key=lambda x: os.path.getmtime(x))
        if len(files) == 0:
            break
        print('got ', len(files), 'files')
        for f in files:
            try:    
                if f in fail_list:
                    continue
    
                print('processing', f)
                with open(f, 'rb') as source_file:
                    job = client.load_table_from_file(source_file, table_id,job_config=job_config)
                job.result()
                time.sleep(60)
                table = client.get_table(table_id)
                print('table now has ', table.num_rows, ' rows')
                print('removing file after processing ', f)
                os.remove(f)
            except:
                print(f, 'failed')
                fail_list.append(f)
                continue
    
    with open('fail.json', 'w') as fh:
        json.dump(fail_list, f)
def main():
    qtype = sys.argv[1]
    if qtype =='create':
       schemafile = sys.argv[2]
       schema = get_schema(schemafile)
       create_table(schema)
    elif qtype=='upload':
      schemafile = sys.argv[2]
      schema = get_schema(schemafile)
      upload_data(schema)
    else:
        upload_censys_ips(sys.argv[2])

if __name__=='__main__':
    main()
