import os
import json
import requests
from google.cloud import storage
from datetime import datetime
import zipfile
from io import BytesIO
from confluent_kafka import Producer, Consumer

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/phendy/Downloads/intrepid-period-422622-n5-4b46f2a8737b.json"

# Binance API settings
BINANCE_API_URL = 'https://api-gcp.binance.com'

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

def read_config():
    # reads the client configuration from client.properties
    # and returns it as a key-value map
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

def produce(topic, config):
    # creates a new producer instance
    producer = Producer(config)

    # produces a sample message
    key = "key"
    value = "value"
    producer.produce(topic, key=key, value=value)
    print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()

def consume(topic, config):
    # sets the consumer group ID and offset  
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"

    # creates a new consumer instance
    consumer = Consumer(config)

    # subscribes to the specified topic
    consumer.subscribe([topic])

    try:
        while True:
            # consumer polls the topic and prints any incoming messages
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                key = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                print(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")
    except KeyboardInterrupt:
        pass
    finally:
        # closes the consumer connection
        consumer.close()

# Google Cloud Storage settings
GCS_BUCKET_NAME = 'coinbase_api_bucket'

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
    blob = bucket.blob(f'binance-data-project.zip')
    blob.upload_from_string(zip_data, content_type='application/zip')

# Main function for data ingestion
def data_ingest():
    # Step 1: Fetch data from Binance API
    data = get_crypto_prices()
    
    config = read_config()
    topic = "binance.data.pipeline.topic"

    produce(topic, config)
    consume(topic, config)
    
    # Step 3: Compress data into ZIP file
    zip_data = compress_data(data)
    
    # Step 4: Upload ZIP data to Google Cloud Storage
    upload_to_gcs(zip_data)

if __name__ == '__main__':
    data_ingest()
