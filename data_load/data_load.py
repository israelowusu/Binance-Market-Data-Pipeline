import boto3
import psycopg2
import json
from datetime import datetime
import zipfile
import io

# Set up S3 and Redshift connections
s3 = boto3.client('s3')
redshift = psycopg2.connect(
    host='your-redshift-endpoint',
    port='5439',
    dbname='your-redshift-dbname',
    iam_role='arn:aws:iam::123456789012:role/your-redshift-iam-role'
)

# Set up S3 bucket and key
bucket_name = 'coinbase-api-bucket'
key = 'data-2023-03-21.zip'

# Download the ZIP file from S3
obj = s3.get_object(Bucket=bucket_name, Key=key)
data = obj['Body'].read()

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

# Load the transformed data into Redshift
with redshift.cursor() as cur:
    cur.execute('''
        CREATE TABLE IF NOT EXISTS coinbase_prices (
            timestamp TIMESTAMP,
            price NUMERIC(18, 8),
            currency VARCHAR(10)
        )
    ''')
    cur.executemany('''
        INSERT INTO coinbase_prices (timestamp, price, currency)
        VALUES (%s, %s, %s)
    ''', [(row['timestamp'], row['price'], row['currency']) for row in transformed_data])

# Commit the transaction
redshift.commit()
