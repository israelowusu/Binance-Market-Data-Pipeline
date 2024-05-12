import os
import json
import requests
from datetime import datetime
import zipfile
import io
from google.cloud import storage, bigquery
import psycopg2
import httpx   # an HTTP client library and dependency of Prefect
from prefect import task, flow

# Binance API settings
BINANCE_API_URL = 'https://api-gcp.binance.com'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/phendy/Downloads/intrepid-period-422622-n5-4b46f2a8737b.json"

# Kafka settings
KAFKA_SERVER = 'pkc-n3603.us-central1.gcp.confluent.cloud:9092'
KAFKA_TOPIC_GCS = 'binance_data_ingest'
KAFKA_TOPIC_BIGQUERY = 'binance_data_transform'

# Google Cloud Storage settings
GCS_BUCKET_NAME = 'coinbase_api_bucket'

# Cloud SQL PostgreSQL settings
POSTGRES_HOST = ''
POSTGRES_PORT = '5432'
POSTGRES_DB = ''
POSTGRES_USER = ''
POSTGRES_PASSWORD = ''

# Define the cryptocurrencies to retrieve data for
cryptocurrencies = ['BTC', 'ETH', 'SOL', 'USDT', 'ADA']

# Task to fetch cryptocurrency prices from Binance API
@task
def fetch_crypto_prices():
    prices = {}
    for crypto in cryptocurrencies:
        response = requests.get(f'{BINANCE_API_URL}/api/v3/ticker/price', params={'symbol': f'{crypto}USDT'})
        if response.status_code == 200:
            prices[crypto] = response.json()['price']
        else:
            prices[crypto] = None
    return prices

# Task to compress data into ZIP file
@task
def compress_data(data):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        zip_file.writestr('prices.json', json.dumps(data))
    return zip_buffer.getvalue()

# Task to upload ZIP file to Google Cloud Storage
@task
def upload_to_gcs(zip_data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f'binance-data-project-{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.zip')
    blob.upload_from_string(zip_data, content_type='application/zip')

# Task to read data from Google Cloud Storage, transform it, and load it into BigQuery
@task
def transform_and_load_to_bigquery():
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob('binance-data-project.zip')
    data = json.loads(blob.download_as_string())

    transformed_data = []
    for price in data:
        transformed_data.append({
            'timestamp': datetime.fromisoformat(price['timestamp']),
            'price': price['price'],
            'currency': price['currency']
        })

    bigquery_client = bigquery.Client()
    dataset_id = 'coinbase_data_warehouse'
    table_id = 'binance_prices'
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    rows_to_insert = []
    for row in transformed_data:
        rows_to_insert.append((row['timestamp'],row['price'], row['currency']))

    errors = bigquery_client.insert_rows(table_ref, rows_to_insert)
    if errors:
        raise ValueError(f'Errors occurred while inserting rows: {errors}')

# Task to load data from BigQuery into Cloud SQL PostgreSQL
@task
def load_to_postgresql():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()
    query = """
        SELECT timestamp, price, currency
        FROM coinbase_data_warehouse.binance_prices
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    insert_query = """
        INSERT INTO your_table_name (timestamp, price, currency)
        VALUES (%s, %s, %s)
    """
    cursor.executemany(insert_query, rows)
    conn.commit()
    cursor.close()
    conn.close()

# Define Prefect flow
@flow
def CryptoDataOrchestration():
    crypto_prices = fetch_crypto_prices()
    zip_data = compress_data(crypto_prices)
    upload_to_gcs(zip_data)
    transform_and_load_to_bigquery()
    load_to_postgresql()

# Run the Prefect flow
if __name__ == '__main__':
    import requests

    url = 'http://localhost:4200/api/flows/<flow_id>/run'
    headers = {
        'Authorization': 'Bearer <your_api_key>',
        'Content-Type': 'application/json'
    }
    data = {
        'name': 'CryptoDataOrchestration',
        'parameters': {}
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 201:
        raise ValueError(f'Failed to submit flow: {response.text}')
    print(f'Flow submitted successfully with ID {response.json()["id"]}')