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
blob_name = 'binance-data-project.zip'

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
    table_id = 'prices'
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
def data_transform():
    # Step 1: Download the ZIP file from GCS
    zip_data = download_data_from_gcs()

    # Step 2: Extract the ZIP file
    extract_zip(zip_data)

    # Step 3: Read data from the extracted JSON file
    with open('prices.json', 'r') as jsonfile:
        data = json.load(jsonfile)

    # Step 4: Transform the data
    transformed_data = transform_data(data)

    # Step 5: Load the transformed data into BigQuery
    load_data_to_bigquery(transformed_data)

if __name__ == '__main__':
    data_transform()