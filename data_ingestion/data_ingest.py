import os
import zipfile
import requests
from datetime import datetime
from google.cloud import storage

# Binance API settings
BINANCE_API_URL = 'https://api-gcp.binance.com'

# Google Cloud Storage settings
GCS_BUCKET_NAME = 'coinbase_api_bucket'

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

# Function to compress data into ZIP file
def compress_data(data):
    with zipfile.ZipFile('binance-data-project.zip', 'w') as zip_file:
        with zip_file.open('prices.json', 'w') as prices_file:
            prices_file.write(bytes(str(data['prices']), 'utf-8'))

# Function to upload data to Google Cloud Storage
def upload_to_gcs():
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f'binance-data-project.zip')
    blob.upload_from_filename('binance-data-project.zip')

# Main function
def main():
    data = {}
    data['prices'] = get_crypto_prices()

    compress_data(data)

    upload_to_gcs()

if __name__ == '__main__':
    main()
