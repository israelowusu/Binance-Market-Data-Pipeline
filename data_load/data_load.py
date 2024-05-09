import json
from datetime import datetime
import zipfile
import io
from google.cloud import storage, bigquery

# Set up Google Cloud Storage (GCS) and BigQuery connections
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Set up GCS bucket and blob name
bucket_name = 'coinbase_api_bucket'
blob_name = 'binance-data-project.zip'

# Download the ZIP file from GCS
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_name)
data = blob.download_as_string()

# Extract the ZIP file
with zipfile.ZipFile(io.BytesIO(data), 'r') as zip_ref:
    zip_ref.extractall()

# Read the JSON files
with open('prices.json', 'r') as prices_file:
    prices_data = json.load(prices_file)

with open('news.json', 'r') as news_file:
    news_data = json.load(news_file)

# Transform the data
transformed_data = []
for price in prices_data:
    transformed_data.append({
        'timestamp': datetime.fromisoformat(price['timestamp']),
        'price': price['price'],
        'currency': price['currency']
    })

# Load the transformed data into BigQuery
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
