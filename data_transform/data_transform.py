import os
import psycopg2
from google.cloud import bigquery

# Set up BigQuery and Cloud SQL PostgreSQL connections
bigquery_client = bigquery.Client()
cloudsql_conn_params = {
    'dbname': os.environ.get('CLOUDSQL_DBNAME'),
    'user': os.environ.get('CLOUDSQL_USERNAME'),
    'password': os.environ.get('CLOUDSQL_PASSWORD'),
    'host': os.environ.get('CLOUDSQL_HOST'),
    'port': '5432'  # Change port if necessary
}

# Ensure all required environment variables are set
required_env_vars = ['CLOUDSQL_DBNAME', 'CLOUDSQL_USERNAME', 'CLOUDSQL_PASSWORD', 'CLOUDSQL_HOST']
for var in required_env_vars:
    if var not in os.environ:
        raise ValueError(f"Environment variable {var} is not set.")

# Set up BigQuery query to fetch transformed data
query = """
    SELECT timestamp, price, currency
    FROM `intrepid-period-422622-n5.coinbase_data_warehouse.binance_prices`
"""

# Execute query in BigQuery
query_job = bigquery_client.query(query)
rows = query_job.result()

# Connect to Cloud SQL PostgreSQL database
cloudsql_conn = psycopg2.connect(**cloudsql_conn_params)

# Define PostgreSQL table schema
create_table_sql = """
    CREATE TABLE IF NOT EXISTS coinbase_prices (
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
    INSERT INTO binance_prices (timestamp, price, currency)
    VALUES (%s, %s, %s)
"""

with cloudsql_conn.cursor() as cursor:
    for row in rows:
        cursor.execute(insert_sql, (row.timestamp, row.price, row.currency))
    cloudsql_conn.commit()

# Close connections
cloudsql_conn.close()
