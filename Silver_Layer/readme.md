# Silver Layer

## Overview

The **Silver Layer** transforms raw data from the Bronze Layer into a clean, standardized, validated, and analytics-ready dataset.

It reads the Ports dataset from HDFS, applies schema validation, data cleaning, standardization, type casting, null handling, and duplicate handling, then writes the trusted result back to HDFS in **Parquet** format.

The Silver Layer serves as the trusted data source for the downstream **Gold Layer**.

---

## Architecture

```text
Bronze Layer
     |
     v
HDFS: /bronze_layer/ports
     |
     v
Spark ETL
ports_to_silver.py
     |
     |-- Schema Validation
     |-- Column Standardization
     |-- Data Type Casting
     |-- String Cleaning
     |-- Null Handling
     |-- Data Validation
     |-- Deduplication
     |
     v
HDFS: /silver_layer/ports
     |
     v
Parquet
     |
     v
Gold Layer
```

---

## Input

The Silver Layer reads the raw Ports dataset from:

```text
/bronze_layer/ports
```

The input data is stored in **HDFS** and represents the raw data ingested by the Bronze Layer.

---

## Output

The processed dataset is written to:

```text
/silver_layer/ports
```

### Output Format

```text
Parquet
```

Parquet is used because it provides:

* Columnar storage
* Efficient compression
* Efficient Spark processing
* Better performance for analytical workloads
* Compatibility with Hive and downstream data-processing tools

---

## Main Responsibilities

### 1. Schema Validation

The incoming dataset is validated against the Silver schema defined in:

```text
config/silver_schema.yaml
```

The schema defines the expected structure and data types of the Silver dataset.

Validation ensures that the incoming data conforms to the expected Silver-layer structure before being written to HDFS.

---

### 2. Column Standardization

Source column names are converted into consistent and Spark-friendly naming conventions.

Examples:

```text
OID_
    ↓
oid

Main Port Name
    ↓
main_port_name
```

This provides consistent column naming across Spark, Hive, SQL, and downstream analytical systems.

---

### 3. Data Type Casting

Columns are converted to their expected data types based on the Silver schema.

Examples include:

```text
OID                     → integer
Latitude                → double
Longitude               → double
Tidal Range (m)         → double
Maximum Vessel Draft    → double
```

This ensures consistent data types and prevents downstream processing issues.

---

### 4. String Cleaning

String columns are cleaned by removing unnecessary leading and trailing whitespace.

Example:

```text
"  Alexandria Port  "
        ↓
"Alexandria Port"
```

This improves data consistency and prevents formatting issues during analysis and joins.

---

### 5. Null Handling

Null values are handled according to the rules defined by the Silver schema.

Required fields are validated to ensure that critical attributes are available before the data is promoted to the Silver Layer.

---

### 6. Data Validation

The Silver Layer performs data-quality checks to ensure that the resulting dataset is reliable and consistent.

Validation includes checks related to:

* Required columns
* Expected data types
* Required fields
* Nullability
* Schema consistency
* Invalid or inconsistent records

The objective is to ensure that only trusted data is passed to the Gold Layer.

---

### 7. Duplicate Handling

Duplicate records are handled as part of the Silver-layer data-quality process.

The deduplication step ensures that the Silver dataset contains clean and consistent records before further business transformations are applied in the Gold Layer.

---

## Configuration

The Silver schema is maintained in:

```text
config/silver_schema.yaml
```

This configuration defines the expected structure and data types used during the Silver transformation process.

Keeping the schema in a separate configuration file makes the pipeline easier to maintain and update.

---

## Spark Job

The main Spark ETL job is:

```text
Silver_Layer/Spark_job/ports_to_silver.py
```

The job is responsible for:

1. Creating the Spark session
2. Reading Bronze data from HDFS
3. Applying the Silver schema
4. Cleaning and standardizing the data
5. Casting columns to the required data types
6. Applying validation and data-quality rules
7. Handling duplicate records
8. Writing the final dataset to HDFS as Parquet

---

## How to Run

Make sure that the Hadoop/HDFS and Spark environments are running before executing the job.

From the project environment:

```bash
spark-submit /silver_scripts/Spark_job/ports_to_silver.py
```

---

## Verify the Output

After the Spark job completes successfully, verify that the Silver output exists in HDFS:

```bash
hdfs dfs -ls /silver_layer/ports
```

The output directory should contain Parquet files and the Spark completion marker:

```text
part-00000-....snappy.parquet
_SUCCESS
```

---

## Logs

Silver Layer execution logs are stored in:

```text
Silver_Layer/logs/
```

Main log file:

```text
silver_ports_etl.log
```

The logs provide information about:

* Job start and completion
* Input processing
* Validation results
* Transformation steps
* Output processing
* Errors and failures

---

## Hive Integration

The Silver dataset is stored in HDFS as Parquet files.

A Hive external table is created on top of these Parquet files using the DDL file:

```text
Silver_Layer/silver_ddl.sql
```

## 1. Open Hive

Enter the Hadoop container:

```bash
docker exec -it itvdelab bash
```

Start the Hive CLI:

```bash
hive
```

## 2. Create the Silver Database and Table

Inside the Hive CLI, execute:

```sql
SOURCE /silver_scripts/Hive_scripts/silver_ddl.sql;
```

The DDL creates:

* **Database:** `silver`
* **Table:** `silver.ports`

The table points directly to the Silver Parquet files stored in:

```text
/silver_layer/ports
```

## 3. Verify the Table

Check that the database exists:

```sql
SHOW DATABASES;
```

Select the Silver database:

```sql
USE silver;
```

Check the available tables:

```sql
SHOW TABLES;
```

Expected result:

```text
ports
```

## 4. Verify the Schema

Inspect the Silver table schema:

```sql
DESCRIBE silver.ports;
```

## 5. Query the Silver Data

Test the table by retrieving a sample of records:

```sql
SELECT *
FROM silver.ports
LIMIT 10;
```

Since `silver.ports` is an **EXTERNAL TABLE**, Hive reads the Parquet files directly from HDFS:

```text
/silver_layer/ports
```

The underlying Parquet files remain stored in HDFS and are not managed by Hive. Dropping the Hive table does not delete the underlying Silver data from HDFS.

---

## Silver → Gold

The Silver Hive table provides a SQL-accessible interface to the trusted Silver data.

The overall flow is:

```text
Bronze Layer
       |
       v
HDFS
/bronze_layer/ports
       |
       v
Silver Spark ETL
       |
       v
HDFS
/silver_layer/ports
       |
       v
Hive External Table
silver.ports
       |
       v
Gold Layer
```

The Silver Layer provides the **cleaned, standardized, validated, and trusted data** required by the Gold Layer for downstream business transformations and dimensional modeling.


## Layer Responsibility

| Layer      | Responsibility                                     |
| ---------- | -------------------------------------------------- |
| **Bronze** | Raw ingested data                                  |
| **Silver** | Cleaned, standardized, validated, and trusted data |
| **Gold**   | Business-ready dimensional and analytical data     |

The **Silver Layer focuses on data quality and standardization**, while business transformations, dimensional modeling, metrics, and analytical logic are handled in the **Gold Layer**.
