#!/bin/bash
set -e

# Initialize the database
airflow db init

# Start the webserver
airflow webserver --port 8080
