import datetime
import logging
from binance_api import get_binance_historical_data
from pubsub_publisher import publish_to_pubsub
from gcs_uploader import upload_to_gcs
from utils.config import Config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    api_key = Config.BINANCE_API_KEY
    project_id = Config.GCP_PROJECT_ID
    topic_name = Config.PUBSUB_TOPIC_NAME
    symbol = "BTCUSDT"
    interval = "1d"
    start_date = datetime.datetime(2020, 1, 1)
    end_date = datetime.datetime.now()
    bucket_name = Config.GCS_BUCKET_NAME
    column_headers = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'CloseTime', 'QuoteAssetVolume', 'NumberTrades', 'TakerBuyBaseAssetVolume', 'TakerBuyQuoteAssetVolume', 'Ignore']

    historical_data = get_binance_historical_data(symbol, interval, start_date, end_date)

    if historical_data:
        publish_to_pubsub(project_id, topic_name, historical_data)
        upload_to_gcs(bucket_name, "cryptodata.csv", historical_data, column_headers)

if __name__ == "__main__":
    main()
