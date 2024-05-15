import os
import configparser
from prefect import task, flow
from data_ingestion.data_ingest import data_ingest
from data_transform.data_transform import data_transform
from data_load.data_load import data_load

# Define the cryptocurrencies to retrieve data for
cryptocurrencies = ['BTC', 'ETH', 'SOL', 'USDT', 'ADA']

# Binance API settings
BINANCE_API_URL = 'https://api-gcp.binance.com'

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/phendy/Downloads/intrepid-period-422622-n5-4b46f2a8737b.json"

# Kafka settings
KAFKA_SERVER = 'pkc-n3603.us-central1.gcp.confluent.cloud:9092'
topic = 'binance.data.pipeline.topic'

# Google Cloud Storage settings
GCS_BUCKET_NAME = 'coinbase_api_bucket'

# Read database credentials from config.ini file
config = configparser.ConfigParser()
config.read('config.ini')

# Cloud SQL PostgreSQL settings
POSTGRES_HOST = config['development']['DB_HOST']
POSTGRES_PORT = config['development']['DB_PORT']
POSTGRES_DB = config['development']['DB_NAME']
POSTGRES_USER = config['development']['DB_USER']
POSTGRES_PASSWORD = config['development']['DB_PASSWORD']

@task
def execute_data_ingestion():
    data_ingest()

@task
def execute_data_transform():
    data_transform()

@task
def execute_data_load():
    data_load('config.ini', '/home/phendy/Downloads/intrepid-period-422622-n5-4b46f2a8737b.json')

# Define Prefect flow
@flow
def BinanceDataPipeline():
    execute_data_ingestion()
    execute_data_transform()
    execute_data_load()

# Run the Prefect flow
if __name__ == '__main__':
    BinanceDataPipeline()
