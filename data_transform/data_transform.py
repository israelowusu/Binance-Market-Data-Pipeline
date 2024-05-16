import os
import json
from datetime import datetime
import zipfile
import io
from google.cloud import storage, bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/phendy/Downloads/intrepid-period-422622-n5-4b46f2a8737b.json"

# Set up Google Cloud Storage (GCS) and BigQuery connections
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Set up GCS bucket and blob name
bucket_name = 'coinbase_api_bucket'

def download_data_from_gcs(blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_string()
    return data

def extract_zip(data):
    with zipfile.ZipFile(io.BytesIO(data), 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            with zip_ref.open(file_name) as file:
                return json.load(file)

def transform_data(prices_data):
    transformed_data = []
    for currency, price in prices_data.items():
        transformed_data.append({
            'timestamp': datetime.utcnow().isoformat(),  # Use current UTC time
            'price': float(price),  # Ensure price is a float
            'currency': currency
        })
    return transformed_data

def load_data_to_bigquery(transformed_data):
    dataset_id = 'coinbase_data_warehouse'
    table_id = 'prices'
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    
    errors = bigquery_client.insert_rows_json(table_ref, transformed_data)  # Use insert_rows_json for simplicity
    if errors:
        print(f'Errors occurred while inserting rows: {errors}')
    else:
        print('Data successfully inserted into BigQuery')

def data_transform(blob_name):
    # Step 1: Download the ZIP file from GCS
    zip_data = download_data_from_gcs(blob_name)

    # Step 2: Extract the ZIP file and read data from the extracted JSON file
    data = extract_zip(zip_data)

    # Step 3: Transform the data
    transformed_data = transform_data(data)

    # Step 4: Load the transformed data into BigQuery
    load_data_to_bigquery(transformed_data)

def trigger_data_transform(event, context):
    """Triggered by a change to a Cloud Storage bucket."""
    blob_name = event['name']
    print(f'Processing file: {blob_name}')
    data_transform(blob_name)
