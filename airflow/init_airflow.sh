#!/bin/bash

# Wait for PostgreSQL to be ready using the postgres
echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD=airflow psql -h "cluster_util_db" -U "airflow" -d "airflow" -c '\q' 2>/dev/null; do
  echo "Waiting for airflow user/database to be ready..."
  sleep 3
done


# Initialize Airflow DB
airflow db init

# Create admin user
airflow users create \
    --username amira \
    --password 12345 \
    --firstname amira \
    --lastname mostafa \
    --role Admin \
    --email amirazalazel@gmail.com

# Set connections
airflow connections add "sftp_tidaline" \
    --conn-type "sftp" \
    --conn-host "${SFTP_HOST}" \
    --conn-port "${SFTP_PORT}" \
    --conn-login "${SFTP_USER}" \
    --conn-password "${SFTP_PASSWORD}"

airflow connections add "hdfs_default" \
    --conn-type "hdfs" \
    --conn-host "itvdelab" \
    --conn-port 9870

airflow connections add "postgres_metadata" \
    --conn-type "postgres" \
    --conn-host "cluster_util_db" \
    --conn-schema "airflow" \
    --conn-login "airflow" \
    --conn-password "airflow"

airflow connections add "ssh_itvdelab" \
    --conn-type "ssh" \
    --conn-host "itvdelab" \
    --conn-login "itversity" \
    --conn-port 22 \
    --conn-extra '{"key_file": "/home/airflow/.ssh/itvdelab.key", "no_host_key_check": "true"}'

# Set variables
airflow variables set "SFTP_USER" "${SFTP_USER}"
airflow variables set "SFTP_PASSWORD" "${SFTP_PASSWORD}"
airflow variables set "HDFS_BASE_PATH" "/raw_layer"
airflow variables set "RAW_PORTS_PATH" "/raw_layer/ports"
airflow variables set "RAW_VESSELS_PATH" "/raw_layer/vessels"


# Start services
airflow scheduler &
airflow webserver