import os
import psycopg2
from google.cloud import bigquery
import configparser

def data_load(config_path: str, google_credentials_path: str):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials_path

    # Read database credentials from config.ini file
    config = configparser.ConfigParser()
    config.read(config_path)

    # Set up BigQuery and Cloud SQL PostgreSQL connections
    bigquery_client = bigquery.Client()
    cloudsql_conn_params = {
        'dbname': config['development']['DB_NAME'],
        'user': config['development']['DB_USER'],
        'password': config['development']['DB_PASSWORD'],
        'host': config['development']['DB_HOST'],
        'port': config['development']['DB_PORT']
    }

    # Set up BigQuery query to fetch transformed data
    query = """
        SELECT timestamp, price, currency
        FROM `intrepid-period-422622-n5.coinbase_data_warehouse.prices`
    """

    # Execute query in BigQuery
    query_job = bigquery_client.query(query)
    rows = query_job.result()

    # Connect to Cloud SQL PostgreSQL database
    cloudsql_conn = psycopg2.connect(**cloudsql_conn_params)

    # Define PostgreSQL table schema
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS prices (
            timestamp TIMESTAMP,
            price NUMERIC(18, 8),
            currency VARCHAR(10)
        )
    """

    # Execute create table SQL
    with cloudsql_conn.cursor() as cursor:
        cursor.execute(create_table_sql)
        cloudsql_conn.commit()

    # Insert rows into PostgreSQL table
    insert_sql = """
        INSERT INTO prices (timestamp, price, currency)
        VALUES (%s, %s, %s)
    """

    with cloudsql_conn.cursor() as cursor:
        for row in rows:
            cursor.execute(insert_sql, (row.timestamp, row.price, row.currency))
        cloudsql_conn.commit()

    # Close connections
    cloudsql_conn.close()

# Example usage in another script, e.g., binance_etl.py
if __name__ == "__main__":
    data_load('config.ini', '/home/phendy/Downloads/intrepid-period-422622-n5-4b46f2a8737b.json')
