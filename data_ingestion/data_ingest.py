import os
import json
import requests
from google.cloud import storage
from datetime import datetime
import zipfile
from io import BytesIO
from confluent_kafka import Producer, Consumer
from threading import Thread, Event
import time

# Environment settings
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/phendy/Downloads/intrepid-period-422622-n5-4b46f2a8737b.json"

# Binance API settings
BINANCE_API_URL = 'https://api-gcp.binance.com'

# Load Binance API credentials from environment variables
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY')
BINANCE_API_SECRET = os.environ.get('BINANCE_API_SECRET')

# Define the cryptocurrencies to retrieve data for
cryptocurrencies = ['BTC', 'ETH', 'SOL', 'USDT', 'ADA']

# Kafka settings
topic = "binance.data.pipeline.topic"

# Google Cloud Storage settings
GCS_BUCKET_NAME = 'coinbase_api_bucket'

# Control event for threads
stop_event = Event()

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
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

def produce(config):
    producer = Producer(config)
    while not stop_event.is_set():
        data = get_crypto_prices()
        key = "crypto_prices"
        value = json.dumps(data)
        producer.produce(topic, key=key, value=value)
        print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")
        producer.flush()
        time.sleep(60)  # Fetch and produce data every minute

def consume(config):
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"
    consumer = Consumer(config)
    consumer.subscribe([topic])

    collected_data = []

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                value = msg.value().decode("utf-8")
                print(f"Consumed message from topic {topic}: value = {value:12}")
                collected_data.append(json.loads(value))
                if len(collected_data) >= 10:  # Process data in batches of 10 messages
                    process_and_upload_data(collected_data)
                    collected_data.clear()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def process_and_upload_data(data):
    zip_data = compress_data(data)
    upload_to_gcs(zip_data)

def compress_data(data):
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        zip_file.writestr('data.json', json.dumps(data))
    return zip_buffer.getvalue()

def upload_to_gcs(zip_data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob('binance-data-project.zip')
    blob.upload_from_string(zip_data, content_type='application/zip')
    print(f"Uploaded data to GCS bucket {GCS_BUCKET_NAME}")

def data_ingest():
    config = read_config()

    producer_thread = Thread(target=produce, args=(config,))
    consumer_thread = Thread(target=consume, args=(config,))

    producer_thread.start()
    consumer_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_event.set()

    producer_thread.join()
    consumer_thread.join()

if __name__ == '__main__':
    data_ingest()
