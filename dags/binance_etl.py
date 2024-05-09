from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSBucketToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_sql import BigQueryToSQLTransfer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 15,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'coinbase_data_pipeline',
    default_args=default_args,
    description='DAG to orchestrate the Coinbase data pipeline',
    schedule_interval=timedelta(days=1),
)

# Define tasks
load_to_bigquery_task = GCSBucketToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket='coinbase_api_bucket',
    source_objects=['binance-data-project.zip'],  # Adjust the path to your data file
    destination_project_dataset_table='coinbase_data_warehouse.binance_prices',
    write_disposition='WRITE_TRUNCATE',  # Overwrite the table each time
    dag=dag,
)

load_to_cloudsql_task = BigQueryToSQLTransfer(
    task_id='load_to_cloudsql',
    sql='SELECT * FROM `coinbase_data_warehouse.binance_prices`',  # SQL query to extract data from BigQuery
    destination_conn_id='cloudsql_postgres_conn',  # Connection ID to Cloud SQL PostgreSQL
    table='binance_prices',  # Name of the table in Cloud SQL PostgreSQL
    dag=dag,
)

# Define task dependencies
load_to_bigquery_task >> load_to_cloudsql_task
