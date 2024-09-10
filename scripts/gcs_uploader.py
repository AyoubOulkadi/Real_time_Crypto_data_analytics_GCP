import pandas as pd
from google.cloud import storage
import logging

# Set up logging
logger = logging.getLogger(__name__)

def upload_to_gcs(bucket_name, file_name, data, column_headers=None):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    if column_headers:
        csv_data = ','.join(column_headers) + '\n'
    else:
        csv_data = ''

    column_names = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'QuoteAssetVolume', 'NumberTrades', 'TakerBuyBaseAssetVolume', 'TakerBuyQuoteAssetVolume', 'Ignore']
    df = pd.DataFrame(data, columns=column_names)

    csv_data += df.to_csv(index=False, header=False)

    blob.upload_from_string(csv_data, content_type='text/csv')

    logger.info(f'Data uploaded to GCS bucket: gs://{bucket_name}/{file_name}')
