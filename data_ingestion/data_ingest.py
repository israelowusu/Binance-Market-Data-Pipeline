import os
import json
import requests
from google.cloud import storage
from datetime import datetime
from kafka import KafkaProducer
import zipfile
from io import BytesIO


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/phendy/Downloads/intrepid-period-422622-n5-4b46f2a8737b.json"


# Binance API settings
BINANCE_API_URL = 'https://api-gcp.binance.com'

# Kafka settings
KAFKA_SERVER = 'pkc-n3603.us-central1.gcp.confluent.cloud:9092'
KAFKA_TOPIC_GCS = 'binance_data_ingest'

# Google Cloud Storage settings
GCS_BUCKET_NAME = 'coinbase_api_bucket'

# Load Binance API credentials from environment variables
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_API_SECRET = os.environ.get('BINANCE_API_SECRET')

# Define the cryptocurrencies to retrieve data for
cryptocurrencies = ['BTC', 'ETH', 'SOL', 'USDT', 'ADA']

# Function to retrieve cryptocurrency prices from Binance API
def get_crypto_prices():
    prices = {}
    for crypto in cryptocurrencies:
        response = requests.get(f'{BINANCE_API_URL}/api/v3/ticker/price', params={'symbol': f'{crypto}USDT'})
        if response.status_code == 200:
            prices[crypto] = response.json()['price']
        else:
            prices[crypto] = None
    return prices

# Kafka producer initialization
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

# Function to publish data to Kafka topic
def publish_to_kafka(data):
    producer.send(KAFKA_TOPIC_GCS, json.dumps(data).encode('utf-8'))

# Function to compress data into ZIP file
def compress_data(data):
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        zip_file.writestr('data.json', json.dumps(data))
    return zip_buffer.getvalue()

# Function to upload data to Google Cloud Storage
def upload_to_gcs(zip_data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f'binance-data-project-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.zip')
    blob.upload_from_string(zip_data, content_type='application/zip')

# Main function
def main():
    # Step 1: Fetch data from Binance API
    data = get_crypto_prices()
    
    # Step 2: Publish data to Kafka topic
    publish_to_kafka(data)
    
    # Step 3: Compress data into ZIP file
    zip_data = compress_data(data)
    
    # Step 4: Upload ZIP data to Google Cloud Storage
    upload_to_gcs(zip_data)

if __name__ == '__main__':
    main()
