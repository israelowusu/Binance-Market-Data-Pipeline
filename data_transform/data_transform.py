import os
import json
from datetime import datetime
import zipfile
import io
from google.cloud import storage, bigquery
from kafka import KafkaConsumer

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/phendy/Downloads/intrepid-period-422622-n5-4b46f2a8737b.json"


# Set up Google Cloud Storage (GCS) and BigQuery connections
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Set up GCS bucket and blob name
bucket_name = 'coinbase_api_bucket'
blob_name = 'binance-data-project.zip'

# Kafka settings
KAFKA_SERVER = 'pkc-n3603.us-central1.gcp.confluent.cloud:9092'
KAFKA_TOPIC_BIGQUERY = 'binance_data_transform'

# Download the ZIP file from GCS
def download_data_from_gcs():
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_string()
    return data

# Extract the ZIP file
def extract_zip(data):
    with zipfile.ZipFile(io.BytesIO(data), 'r') as zip_ref:
        zip_ref.extractall()

# Read data from Kafka topic
def read_data_from_kafka():
    consumer = KafkaConsumer(KAFKA_TOPIC_BIGQUERY, bootstrap_servers=KAFKA_SERVER)
    data = []
    for message in consumer:
        data.append(json.loads(message.value.decode('utf-8')))
    return data

# Transform the data
def transform_data(prices_data):
    transformed_data = []
    for price in prices_data:
        transformed_data.append({
            'timestamp': datetime.fromisoformat(price['timestamp']),
            'price': price['price'],
            'currency': price['currency']
        })
    return transformed_data

# Load the transformed data into BigQuery
def load_data_to_bigquery(transformed_data):
    dataset_id = 'coinbase_data_warehouse'
    table_id = 'binance_prices'
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)
    
    rows_to_insert = []
    for row in transformed_data:
        rows_to_insert.append((row['timestamp'], row['price'], row['currency']))
    
    errors = bigquery_client.insert_rows(table, rows_to_insert)  # Make an API request.
    if errors:
        print(f'Errors occurred while inserting rows: {errors}')

# Main function
def main():
    # Step 1: Download the ZIP file from GCS
    zip_data = download_data_from_gcs()

    # Step 2: Extract the ZIP file
    extract_zip(zip_data)

    # Step 3: Read data from Kafka topic
    data = read_data_from_kafka()

    # Step 4: Transform the data
    transformed_data = transform_data(data)

    # Step 5: Load the transformed data into BigQuery
    load_data_to_bigquery(transformed_data)

if __name__ == '__main__':
    main()
