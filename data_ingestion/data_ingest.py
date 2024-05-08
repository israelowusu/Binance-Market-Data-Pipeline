import os
import json
import zipfile
import requests
from datetime import datetime, timedelta
from google.cloud import storage

# Coinbase API settings
COINBASE_API_URL = 'https://api.coinbase.com/v2'

# Google Cloud Storage settings
GCS_BUCKET_NAME = 'YOUR_GCS_BUCKET_NAME'

# Set up Coinbase API credentials
headers = {
    'Content-Type': 'application/json'
}

# Define the cryptocurrencies to retrieve data for
cryptocurrencies = ['BTC', 'ETH', 'SOL', 'USDT', 'USDC']

# Define the news API endpoint
news_api_endpoint = 'https://newsapi.org/v2/everything'

# Define the news API parameters
news_api_params = {
    'q': 'bitcoin OR ethereum OR solana OR tether OR usd coin',
    'language': 'en',
    'sortBy': 'publishedAt',
    'apiKey': 'YOUR_NEWS_API_KEY'
}

# Function to retrieve cryptocurrency prices from Coinbase API
def get_crypto_prices():
    prices = {}
    for crypto in cryptocurrencies:
        response = requests.get(f'{COINBASE_API_URL}/prices/{crypto}-USD/spot', headers=headers)
        if response.status_code == 200:
            prices[crypto] = response.json()['data']['amount']
        else:
            prices[crypto] = None
    return prices

# Function to retrieve news from news API
def get_news():
    response = requests.get(news_api_endpoint, params=news_api_params)
    if response.status_code == 200:
        return response.json()['articles']
    else:
        return None

# Function to compress data into ZIP file
def compress_data(data):
    with zipfile.ZipFile('data.zip', 'w') as zip_file:
        with zip_file.open('prices.json', 'w') as prices_file:
            json.dump(data['prices'], prices_file)
        with zip_file.open('news.json', 'w') as news_file:
            json.dump(data['news'], news_file)

# Function to upload data to Google Cloud Storage
def upload_to_gcs():
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f'data-{datetime.now().strftime("%Y-%m-%d")}.zip')
    blob.upload_from_filename('data.zip')

# Main function
def main():
    data = {}
    data['prices'] = get_crypto_prices()
    data['news'] = get_news()

    compress_data(data)

    upload_to_gcs()

if __name__ == '__main__':
    main()
