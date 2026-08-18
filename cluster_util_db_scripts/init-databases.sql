

-- Create metastore database for Hive
CREATE DATABASE metastore;
CREATE USER hive WITH ENCRYPTED PASSWORD 'itversity';
GRANT ALL ON DATABASE metastore TO hive;

-- Create airflow database
CREATE DATABASE airflow;
CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
GRANT ALL ON DATABASE airflow TO airflow;