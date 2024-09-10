import requests
import logging

logger = logging.getLogger(__name__)

def get_binance_historical_data(symbol, interval, start_date, end_date):
    endpoint_url = "https://api.binance.com/api/v3/klines"
    
    # Convert date and time to milliseconds
    start_timestamp = int(start_date.timestamp()) * 1000
    end_timestamp = int(end_date.timestamp()) * 1000
    
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_timestamp,
        "endTime": end_timestamp,
        "limit": 1000,  
    }
    
    historical_data = []
    
    try:
        while True:
            response = requests.get(endpoint_url, params=params)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    break
                
                historical_data.extend(data)
                params["startTime"] = int(data[-1][0]) + 1
            else:
                logger.error(f"API request failed with status code: {response.status_code}")
                break
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP request error: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    
    return historical_data
