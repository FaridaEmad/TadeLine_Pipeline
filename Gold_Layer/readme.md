# Gold Layer

## Overview

The **Gold Layer** transforms trusted Silver data into business-ready dimensional data for analytics and reporting.

For the Ports domain, the Gold Layer produces the **Port Dimension**, which represents the current state of the Ports dataset and serves as a core dimension in the Data Warehouse.

The Gold Layer reads validated data from the Silver Layer, selects the latest available snapshot, applies business transformations, calculates analytical metrics, assigns stable surrogate keys, and writes the final dimensional dataset to HDFS in **Parquet** format.

The Gold Layer is designed to provide a trusted, business-ready dataset for **Hive, SQL analytics, and Power BI**.

---

## Architecture

```text
Silver Layer
     |
     v
HDFS: /silver_layer/ports
     |
     v
Spark ETL
Ports_to_gold.py
     |
     |-- Latest Snapshot Selection
     |-- Business Key Validation
     |-- Deduplication
     |-- Business Transformations
     |-- Analytical Metrics
     |-- Surrogate Key Assignment
     |
     v
HDFS: /gold_layer/ports
     |
     v
Hive
     |
     v
Power BI
```

---

# Gold Layer Responsibilities

## 1. Latest Snapshot Selection

The Silver Layer may contain multiple snapshots of the Ports dataset.

The Gold Layer selects the **latest available snapshot** using:

```text
snapshot_year
snapshot_month
```

For example:

```text
2026-06
2026-07
2026-08
```

The Gold Layer processes:

```text
2026-08
```

because it represents the latest available snapshot.

This ensures that the Gold Port Dimension represents the most current state of the Ports dataset.

---

## 2. Business Key Validation

The natural/business key of the Port Dimension is:

```text
oid
```

The Gold Layer validates that `oid` is not null.

Records without a valid `oid` cannot be uniquely identified and therefore cannot be correctly represented in the Port Dimension.

The source identifier `oid` remains available in the Gold dataset as the business key.

---

## 3. Deduplication

The selected latest snapshot is deduplicated using:

```text
oid
```

This ensures that each Port is represented by a single record in the current Gold Port Dimension.

---

# 4. Supplies Metrics

The Gold Layer derives analytical metrics from the following attributes:

```text
supplies_provisions
supplies_fuel_oil
supplies_diesel_oil
supplies_potable_water
repairs
```

Each availability value is normalized into a binary representation.

### Available Values

```text
Available → 1
Yes       → 1
Y         → 1
True      → 1
```

### Unavailable Values

```text
Unavailable → 0
No          → 0
N           → 0
False       → 0
NULL        → 0
```

The normalized values are aggregated into:

```text
supplies_count
```

The maximum possible value is:

```text
5
```

---

## Supplies Rate

The Gold Layer derives:

```text
supplies_rate
```

based on the total number of available services.

| Supplies Count | Supplies Rate |
| -------------: | ------------- |
|              5 | Excellent     |
|            3–4 | Good          |
|            1–2 | Limited       |
|              0 | Unavailable   |

This provides a business-friendly classification of the level of supplies available at each port.

---

# 5. Communication Metrics

Communication availability is calculated from:

```text
communications_radio
communications_telephone
communications_airport
communications_telefax
```

Each source value is converted into a binary availability indicator.

The results are aggregated into:

```text
communications_count
```

The maximum possible value is:

```text
4
```

---

## Communication Rate

The Gold Layer derives:

```text
comm_rate
```

based on the number of available communication services.

| Communication Count | Communication Rate |
| ------------------: | ------------------ |
|                   4 | Excellent          |
|                   3 | Good               |
|                 1–2 | Limited            |
|                   0 | Unavailable        |

This provides a simplified business classification of communication availability at each port.

---

# 6. Surrogate Key

The Gold Port Dimension uses:

```text
port_key
```

as its warehouse surrogate key.

The source/business identifier remains:

```text
oid
```

### Key Design

```text
oid
 |
 v
Business / Natural Key

port_key
 |
 v
Warehouse Surrogate Key
```

The surrogate key is independent of the source-system identifier and provides a stable identifier for the Data Warehouse dimension.

---

## Initial Load

If the Gold Port Dimension does not already exist, surrogate keys are generated using:

```text
row_number()
```

ordered by:

```text
oid
```

Example:

| oid | port_key |
| --: | -------: |
| 101 |        1 |
| 205 |        2 |
| 310 |        3 |

---

## Subsequent Loads

When the Gold Port Dimension already exists:

1. Existing `oid` values keep their existing `port_key`.
2. New Ports receive new surrogate keys.
3. New keys start after the current maximum `port_key`.
4. Existing surrogate keys are never regenerated.

### Example

#### Existing Gold Dimension

| oid | port_key |
| --: | -------: |
| 101 |        1 |
| 205 |        2 |
| 310 |        3 |

#### New Snapshot

```text
101
205
310
450
500
```

#### Result

| oid | port_key |
| --: | -------: |
| 101 |        1 |
| 205 |        2 |
| 310 |        3 |
| 450 |        4 |
| 500 |        5 |

This approach preserves **surrogate-key stability across subsequent loads**.

---

# 7. Gold Schema

The Gold schema is defined in:

```text
config/gold_schema.yaml
```

The schema defines:

* Gold columns
* Expected data types
* Nullable fields
* Analytical metrics
* Snapshot information

Keeping the schema in a separate configuration file makes the Gold pipeline easier to maintain and validate.

---

# 8. Gold Port Dimension

The final Gold dataset contains the following groups of attributes.

### Port Identification

```text
port_key
oid
world_port_index_number
main_port_name
alternate_port_name
un_locode
country_code
region_name
world_water_body
iho_s130_sea_area
```

### Harbor Information

```text
harbor_size
harbor_type
harbor_use
shelter_afforded
```

### Geographic Information

```text
latitude
longitude
```

### Port Capacity

```text
tidal_range_m
entrance_width_m
channel_depth_m
anchorage_depth_m
cargo_pier_depth_m
maximum_vessel_length_m
maximum_vessel_beam_m
maximum_vessel_draft_m
```

### Supplies

```text
supplies_provisions
supplies_fuel_oil
supplies_diesel_oil
supplies_potable_water
repairs
supplies_count
supplies_rate
```

### Communications

```text
communications_radio
communications_telephone
communications_airport
communications_telefax
communications_count
comm_rate
```

### Snapshot Information

```text
snapshot_year
snapshot_month
```

---

# Spark Job

The main Gold Spark ETL job is:

```text
Gold_Layer/Spark_job/Ports_to_gold.py
```

The job is responsible for:

1. Reading the Silver dataset from HDFS.
2. Selecting the latest available snapshot.
3. Validating the business key.
4. Deduplicating the current snapshot.
5. Applying business transformations.
6. Calculating supplies metrics.
7. Calculating communication metrics.
8. Assigning stable surrogate keys.
9. Writing the final Port Dimension to HDFS.

---

# How to Run

Make sure Hadoop/HDFS, Spark, and the Silver Layer output are available before running the Gold job.

If the script is deployed in the Hadoop environment:

```bash
spark-submit /gold_scripts/Spark_job/Ports_to_gold.py
```

---

# Verify the Output

After the Spark job completes successfully, verify the Gold HDFS directory:

```bash
hdfs dfs -ls /gold_layer/ports
```

The output directory should contain Parquet files and the Spark completion marker:

```text
part-00000-....snappy.parquet
_SUCCESS
```

---

# Inspect the Gold Data

The Gold dataset can be read using Spark:

```python
df = spark.read.parquet("/gold_layer/ports")

df.show()
df.printSchema()
```

This can be used to inspect the generated Port Dimension and verify its schema and contents.

---

# Hive Integration

The Gold Port Dimension is stored in HDFS as Parquet files.

A Hive external table is created on top of these Parquet files using the DDL file:

`Gold_Layer/Hive_scripts/gold_ddl.sql`

## 1. Open Hive

Enter the Hadoop container:

```bash
docker exec -it itvdelab bash
```

Start the Hive CLI:

```bash
hive
```

## 2. Create the Gold Database and Table

Inside the Hive CLI, execute:

```sql
SOURCE /gold_scripts/Hive_scripts/gold_ddl.sql;
```

The DDL creates:

* **Database:** `gold`
* **Table:** `gold.ports`

The table points directly to the Gold Parquet files stored in:

```text
/gold_layer/ports
```

## 3. Verify the Table

Check that the database exists:

```sql
SHOW DATABASES;
```

Select the Gold database:

```sql
USE gold;
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

Inspect the table schema:

```sql
DESCRIBE gold.ports;
```

## 5. Query the Gold Data

Test the table by retrieving a sample of records:

```sql
SELECT *
FROM gold.ports
LIMIT 10;
```

Since `gold.ports` is an **EXTERNAL TABLE**, Hive reads the Parquet files directly from HDFS:

```text
/gold_layer/ports
```

The underlying Parquet files remain stored in HDFS and are not managed by Hive. Therefore, dropping the Hive table does not delete the underlying data from HDFS.

---

## Hive → Power BI

The Gold data can be exposed to downstream analytical and BI tools through the Hive/SQL connectivity layer.

The logical flow is:

```text
HDFS
/gold_layer/ports
       |
       v
Hive External Table
gold.ports
       |
       v
SQL Queries
       |
       v
Power BI
```

This allows the Gold Port Dimension to be queried using SQL and consumed by BI tools for analytics and reporting.


# Logs

Gold Layer execution logs are stored in:

```text
Gold_Layer/logs/
```

Main log file:

```text
gold_ports_etl.log
```

The logs provide information about:

* ETL start and completion
* Silver record count
* Latest snapshot selection
* Business-key validation
* Deduplication results
* Supplies metrics
* Communication metrics
* Surrogate-key assignment
* Final record count
* Output status
* Errors and failures

---

# Project Files

| File                         | Description               |
| ---------------------------- | ------------------------- |
| `Spark_job/Ports_to_gold.py` | Main Gold Spark ETL job   |
| `gold_ddl.sql`               | Gold database/table DDL   |
| `../config/gold_schema.yaml` | Gold schema configuration |
| `Business Requirements.docx` | Business requirements     |
| `DataWarehouseDesign.drawio` | Data Warehouse design     |
| `logs/gold_ports_etl.log`    | Gold ETL execution log    |

---

# End-to-End Data Flow

```text
Raw Ports Data
      |
      v
Bronze Layer
      |
      | Raw Storage
      v
HDFS
/bronze_layer/ports
      |
      v
Silver Layer
      |
      | Cleaning
      | Standardization
      | Validation
      | Type Casting
      | Deduplication
      v
HDFS
/silver_layer/ports
      |
      v
Gold Layer
      |
      | Latest Snapshot
      | Business Key Validation
      | Deduplication
      | Business Transformations
      | Analytical Metrics
      | Surrogate Keys
      v
Port Dimension
      |
      v
HDFS
/gold_layer/ports
      |
      v
Hive / SQL
      |
      v
Power BI
```

---

# Layer Responsibilities

| Layer        | Responsibility                                                         |
| ------------ | ---------------------------------------------------------------------- |
| **Bronze**   | Raw data ingestion and storage                                         |
| **Silver**   | Cleaning, standardization, validation, and trusted data preparation    |
| **Gold**     | Business transformations, analytical metrics, and dimensional modeling |
| **Hive**     | SQL access layer                                                       |
| **Power BI** | Visualization and business reporting                                   |

The **Gold Layer consumes trusted Silver data and focuses on business-level transformations, analytical metrics, and dimensional modeling** required by downstream consumers.

This separation of responsibilities keeps the pipeline organized and ensures that each layer has a clear purpose.
