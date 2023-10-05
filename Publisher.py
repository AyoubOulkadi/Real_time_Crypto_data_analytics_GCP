import os
import requests
import json
import datetime
import logging
from google.cloud import pubsub_v1
from google.cloud import storage
import pandas as pd
keyfile_path = "orbital-surge-359121-1dd5e9f4ccac.json"

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keyfile_path  

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_binance_historical_data(symbol, interval, start_date, end_date):
    endpoint_url = "https://api.binance.com/api/v3/klines"
    
    # Convert date and time to milliseconds
    start_timestamp = int(start_date.timestamp()) * 1000
    end_timestamp = int(end_date.timestamp()) * 1000
    
    # Initialize parameters for the API request
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_timestamp,
        "endTime": end_timestamp,
        "limit": 1000,  # Maximum limit per request
    }
    
    historical_data = []
    
    try:
        while True:
            # Make a GET request to the Binance API
            response = requests.get(endpoint_url, params=params)
    
            # Check if the request was successful (HTTP status code 200)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    break  # No more data available
    
                # Append the data to the historical_data list
                historical_data.extend(data)
    
                # Set the start time for the next request
                params["startTime"] = int(data[-1][0]) + 1  # Increment by 1 millisecond
            else:
                logger.error(f"API request failed with status code: {response.status_code}")
                break
    
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP request error: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    
    return historical_data

def publish_to_pubsub(project_id, topic_name, data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    
    # Publish the messages individually
    for data_point in data:
        message_data = json.dumps(data_point)
        message_bytes = message_data.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes)

    logger.info(f"Historical data collected and published to Pub/Sub topic: {topic_name}")


def upload_to_gcs(bucket_name, file_name, data, column_headers=None):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    # If column_headers are provided, join them as the first row
    if column_headers:
        csv_data = ','.join(column_headers) + '\n'
    else:
        csv_data = ''

    # Convert the data to a DataFrame
    column_names = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'QuoteAssetVolume', 'NumberTrades', 'TakerBuyBaseAssetVolume', 'TakerBuyQuoteAssetVolume', 'Ignore']
    df = pd.DataFrame(data, columns=column_names)

    # Print the DataFrame
    print("DataFrame:")
    print(df.head())

    # Save the DataFrame as a CSV string without the headers
    csv_data += df.to_csv(index=False, header=False)

    # Upload the CSV data to GCS
    blob.upload_from_string(csv_data, content_type='text/csv')

    print(f'Data uploaded to GCS bucket: gs://{bucket_name}/{file_name}')

def main():
    api_key = "113a2a365af9cae5f1edc415919da1410f2356b0dd84efaaca816f1b866eea65"
    project_id = "orbital-surge-359121"
    topic_name = "real_time_data"
    symbol = "BTCUSDT"
    interval = "1d"
    start_date = datetime.datetime(2020, 1, 1)
    end_date = datetime.datetime.now()
    bucket_name = "ayoubcryptodata"  # Specify your GCS bucket name here
    column_headers = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'QuoteAssetVolume', 'NumberTrades', 'TakerBuyBaseAssetVolume', 'TakerBuyQuoteAssetVolume', 'Ignore']

    historical_data = get_binance_historical_data(symbol, interval, start_date, end_date)

    if historical_data:
        # Publish to Pub/Sub
        publish_to_pubsub(project_id, topic_name, historical_data)
        logger.info(f"Historical data collected and published to Pub/Sub topic: {topic_name}")

        # Upload to GCS in CSV format
        upload_to_gcs(bucket_name, "cryptodata.csv", historical_data, column_headers)

if __name__ == "__main__":
    main()

