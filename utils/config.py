class Config:
    BINANCE_API_KEY = "your-binance-api-key"
    GCP_PROJECT_ID = "orbital-surge-359121"
    PUBSUB_TOPIC_NAME = "real_time_data"
    GCS_BUCKET_NAME = "ayoubcryptodata"
    input_file = 'gs://ayoubcryptodata/cryptodata.csv'
    output_file = 'gs://ayoubcryptodata/cryptodata1'
    keyfile_path = "orbital-surge-359121-1dd5e9f4ccac.json"
    gcs_uri = "gs://ayoubcryptodata/cryptodata1.csv"
    dataset_id = "Crytopdataset"
    table_id = "Cryptotable"
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
