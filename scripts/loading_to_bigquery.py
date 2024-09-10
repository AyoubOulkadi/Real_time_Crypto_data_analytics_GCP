import os
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from utils.config import key_file_path, gcs_uri, dataset_id, table_id 

# Set the path to your JSON key file and initialize BigQuery client
def initialize_bigquery_client(keyfile_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keyfile_path
    return bigquery.Client()

# Load data from GCS to BigQuery
def load_data_to_bigquery(client, gcs_uri, dataset_id, table_id, schema):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,  # If your data has a header row
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_uri(
        gcs_uri, f"{client.project}.{dataset_id}.{table_id}", job_config=job_config
    )
    job.result()
    print(f"Data loaded from {gcs_uri} to {client.project}.{dataset_id}.{table_id}")

# Run SQL queries on BigQuery
def run_sql_queries(client):
    sql_code = """
    ALTER TABLE `orbital-surge-359121.Crytpodataset.Cryptotable`
    RENAME COLUMN NUMBERTRADES TO NUMBER_TRADES ;
    ALTER TABLE `orbital-surge-359121.Crytpodataset.Cryptotable`
    RENAME COLUMN QUOTEASSETVOLUME TO QUOTE_ASSET_VOLUME; 
    ALTER TABLE `orbital-surge-359121.Crytpodataset.Cryptotable`
    RENAME COLUMN TAKERBUYBASEASSETVOLUME TO TAKER_BUY_BASE_ASSET_VOLUME;
    ALTER TABLE `orbital-surge-359121.Crytpodataset.Cryptotable`
    RENAME COLUMN TAKERBUYQUOTEASSETVOLUME TO TAKER_BUY_QUOTE_ASSET_VOLUME;
    """
    
    sql_code1 = """
    ALTER TABLE `orbital-surge-359121.Crytpodataset.Cryptotable`
    ADD COLUMN Date_column TIMESTAMP;

    UPDATE `orbital-surge-359121.Crytpodataset.Cryptotable`
    SET Date_column = TIMESTAMP_MILLIS(timestamp)
    WHERE TRUE;
    """
    
    sql_code2 = """
    ALTER TABLE `orbital-surge-359121.Crytpodataset.Cryptotable`
    DROP COLUMN timestamp;
    """
    
    queries = [sql_code, sql_code1, sql_code2]
    
    for sql in queries:
        query_job = client.query(sql)
        query_job.result()
        print(f"Executed SQL: {sql}")

def main():
    client = initialize_bigquery_client(keyfile_path)
    load_data_to_bigquery(client, gcs_uri, dataset_id, table_id, schema)
    run_sql_queries(client)

if __name__ == "__main__":
    main()
