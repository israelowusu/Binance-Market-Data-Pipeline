import os
import configparser
from datetime import timedelta
from prefect import task, flow, get_run_logger
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule
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
    logger = get_run_logger()
    logger.info("Starting data ingestion...")
    data_ingest()
    logger.info("Data ingestion completed.")

@task
def execute_data_transform():
    logger = get_run_logger()
    logger.info("Starting data transformation...")
    data_transform()
    logger.info("Data transformation completed.")

@task
def execute_data_load():
    logger = get_run_logger()
    logger.info("Starting data load...")
    data_load('config.ini', '/home/phendy/Downloads/intrepid-period-422622-n5-4b46f2a8737b.json')
    logger.info("Data load completed.")

# Define Prefect flow
@flow(name="BinanceDataPipeline")
def BinanceDataPipeline():
    ingestion_task = execute_data_ingestion()
    transform_task = execute_data_transform(wait_for=[ingestion_task])
    load_task = execute_data_load(wait_for=[transform_task])
    return load_task

if __name__ == '__main__':
    # Register the flow
    deployment = Deployment.build_from_flow(
        flow=BinanceDataPipeline,
        name="BinanceDataPipelineDeployment",
        schedule=IntervalSchedule(interval=timedelta(hours=1)),  # Adjust the schedule as needed
        work_queue_name="default"
    )
    deployment.apply()

    # Optionally run the flow
    BinanceDataPipeline()
