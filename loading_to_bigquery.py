from google.cloud import bigquery
import os
import csv
import time


# Set the path to your JSON key file
keyfile_path = "orbital-surge-359121-1dd5e9f4ccac.json"

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keyfile_path

# Set your Google Cloud project ID
project_id = "orbital-surge-359121"  

# Initialize a BigQuery client
client = bigquery.Client(project=project_id)

# Set the GCS URI for the data to be loaded
gcs_uri = "gs://ayoubcryptodata/cryptodata1.csv"

# Set the BigQuery dataset and table information
dataset_id = "Crytopdataset"  
table_id = "Cryptotable"  


# Define the schema of the table (optional if schema is not inferred)
schema = [
    bigquery.SchemaField("timestamp", "INTEGER"),
    bigquery.SchemaField("open", "FLOAT"),
    bigquery.SchemaField("high", "FLOAT"),
    bigquery.SchemaField("low", "FLOAT"),
    bigquery.SchemaField("close", "FLOAT"),
    bigquery.SchemaField("volume", "INTEGER"),
    bigquery.SchemaField("CLOSETIME", "INTEGER"),
    bigquery.SchemaField("QUOTEASSETVOLUME", "FLOAT"),
    bigquery.SchemaField("NUMBERTRADES", "INTEGER"),
    bigquery.SchemaField("TAKERBUYBASEASSETVOLUME", "FLOAT"),
    bigquery.SchemaField("TAKERBUYQUOTEASSETVOLUME", "FLOAT"),
    bigquery.SchemaField("IGNORE", "INTEGER", "NULLABLE")
]

# Configure the job for data loading
job_config = bigquery.LoadJobConfig(
    schema=schema,
    skip_leading_rows=1,  # If your data has a header row
    source_format=bigquery.SourceFormat.CSV,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # or WRITE_TRUNCATE, etc.
)

# Start the BigQuery job to load data from GCS
job = client.load_table_from_uri(
    gcs_uri, f"{project_id}.{dataset_id}.{table_id}", job_config=job_config
)

# Wait for the job to complete
job.result()

print(f"Data loaded from {gcs_uri} to {project_id}.{dataset_id}.{table_id}")

# SQL code to execute after data loading
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

-- Step 2: Update the new column with converted values from the integer column
UPDATE `orbital-surge-359121.Crytpodataset.Cryptotable`
SET Date_column = TIMESTAMP_MILLIS(timestamp)
WHERE TRUE;
"""
sql_code2= """

ALTER TABLE `orbital-surge-359121.Crytpodataset.Cryptotable`
DROP COLUMN timestamp ;

"""

# Run the SQL query
query_job = client.query(sql_code)
query_job = client.query(sql_code1)
query_job = client.query(sql_code2)



time.sleep(15)

# Wait for the query to complete
query_job.result()

# Print the results if needed
for row in query_job:
    # Process the query results here
    print(row)