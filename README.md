# TadeLine Pipeline

**ITI Data Management Track — Graduation Project (46th Intake)**

TadeLine is an end-to-end data platform that combines **maritime vessel/port logistics** with **real-time seismic (earthquake) events** to answer one central operational question:

> *Is any vessel heading toward a port that was just hit by an earthquake — and how much danger is it in?*

It stitches together batch ingestion, a Bronze/Silver/Gold lakehouse, Kafka-based Change Data Capture streaming, geospatial risk analysis, cold-storage archiving, and a Power BI dashboard into a single orchestrated system.

**Team:** Amira Mostafa · Farida Emad Eldin · Shahd Hamdi

---

## Table of Contents

- [Business Questions](#business-questions)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Data Sources](#data-sources)
- [Data Warehouse Design](#data-warehouse-design)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
- [Pipeline Walkthrough](#pipeline-walkthrough)
- [Dashboard](#dashboard)
- [Known Limitations](#known-limitations)

---

## Business Questions

| Domain | Questions Answered |
|---|---|
| **Risk Analysis** | What earthquakes happened recently? Where are they? Which ports are nearby, and how dangerous is the situation? Which vessels are heading toward an affected port and may need to reroute? |
| **Vessel Analysis** | Vessel type, weight, size, status, destination, current/last port |
| **Trip Analysis** | Departure/arrival times, arrival delay, destination country, trip duration |
| **Port Analysis** | Port depth, harbor size/type, shelter, supplies and communications availability |

---

## Architecture

TadeLine runs two pipelines side by side — a scheduled **batch** pipeline for port/vessel reference data, and an always-on **streaming** pipeline for earthquake events — both landing in a shared analytical layer that feeds Power BI.

<img width="1104" height="459" alt="Image" src="https://github.com/user-attachments/assets/4ab3ff3e-9d04-44ee-a62f-2d861fbe8cb3" />

Airflow DAGs (`airflow/dags/`) orchestrate the batch chain (`Batch_ETL_DAG.py`) and the streaming jobs (`Stream_ETL_DAG.py`), enforcing that Silver only runs after Bronze succeeds, and Gold only after Silver succeeds.

---

## Repository Structure

```text
TadeLine_Pipeline/
├── Bronze_Layer/            # SFTP → HDFS raw ingestion scripts (ports, vessels)
│   ├── load_ports_to_hdfs.sh
│   ├── load_vessels_to_hdfs.sh
│   ├── config.sh                  # shared SFTP/HDFS config
│   ├── snowflake_table_archive.py # earthquake watermark archiving job
│   ├── water_mark_table.sql
│   └── Retention_Policy.sql
├── Silver_Layer/            # Bronze → Silver cleaning/validation (PySpark + Hive)
│   ├── Spark_job/ports_to_silver.py
│   └── Hive_scripts/silver_ddl.sql
├── Gold_Layer/              # Silver → Gold dimensional modeling (PySpark + Hive)
│   ├── Spark_job/Ports_to_gold.py
│   └── Hive_scripts/gold_ddl.sql
├── config/                  # Schema-as-config for Silver/Gold (YAML)
│   ├── silver_schema.yaml
│   └── gold_schema.yaml
├── stream/                  # Postgres seismic source + CDC producer
│   ├── seismic/streamer.py
│   ├── seismic/writer.py
│   └── create_earthquakes.sql
├── Spark_job/               # Streaming jobs: CDC replication + risk analysis
│   ├── ingest_snowflake.py       # Kafka CDC → Snowflake EARTHQUAKES (MERGE)
│   └── risk_analysis.py          # Earthquake–port geospatial risk scoring
├── batch/                   # Vessel/port batch extraction utilities
│   ├── ports.py
│   └── vessels.py
├── data_extraction/         # Kafka Connect (Debezium) connector registration
│   ├── register-connector.sh
│   └── consumer.sh
├── airflow/                 # Orchestration
│   └── dags/
│       ├── Batch_ETL_DAG.py
│       └── Stream_ETL_DAG.py
├── Power_BI_Dashboard/      # Power BI project (.pbip) — semantic model + report
├── utilities/                 # Shared logging helpers
├── case-study/Gold_Layer/     # Data warehouse model diagram (.drawio)
├── hive/                      # Hive site configuration
├── cluster_util_db_scripts/   # Utility Postgres init scripts
├── docker-compose.yaml        # Full local stack (Kafka, Postgres, SFTP, Airflow, itvdelab)
├── dockerfile                 # itvdelab (Spark/Hive/HDFS) image extension
└── DM46.pdf                   # Project presentation / write-up
```

Each layer also has its own detailed `readme.md` (`Bronze_Layer/readme.md`, `Silver_Layer/readme.md`, `Gold_Layer/readme.md`) with exact run commands, verification steps, and schema details.

---

## Data Sources

| Source | Cadence | Description |
|---|---|---|
| **Ports** (NGA World Port Index) | Monthly, via SFTP CSV | Port identification, navigational charts, depth/physical restrictions, harbor characteristics, entrance restrictions, pilotage, tugs, communications, facilities, cranes, supplies, repairs, lat/lon |
| **Vessels** | Batch CSV | Vessel identity and construction, departure/destination info, reported status |
| **Earthquakes** | Streaming (CDC) | Seismic events (magnitude, depth, location, region) captured via Debezium CDC off a Postgres source table and streamed through Kafka |

---

## Data Warehouse Design

TadeLine's warehouse (`case-study/Gold_Layer/TredLine_Model.drawio`) is a **fact constellation (galaxy schema)**: five fact tables share a set of conformed dimensions, linking the maritime and seismic subject areas.

**Conformed dimensions**
- `DIM_PORT` — master port data (physical characteristics, depth limits, supply/communication services, safety attributes)
- `DIM_DATE` — calendar dimension, role-played across every fact table (arrival, departure, snapshot, event dates)
- `DIM_TIME` — intraday time dimension, role-played by event/snapshot timestamps

**Fact tables** (grain highlights)
- `FACT_EARTHQUAKE` — one row per seismic event
- `FACT_EARTHQUAKE_PORT_RISK` — one row per earthquake, or per earthquake–port pair when a port falls within the estimated impact radius
- Vessel trip / vessel snapshot / vessel–port-risk facts round out the constellation, sharing `DIM_PORT` and `DIM_DATE` as role-playing dimensions

This design supports drill-across analysis between maritime and seismic domains on shared calendar/date keys, and denormalized year/month columns on fact tables support partition pruning in a columnar warehouse (Snowflake).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Storage (lakehouse) | HDFS, Parquet, Hive external tables |
| Batch processing | PySpark 3.3, Bash (SFTP ingestion) |
| Streaming | Apache Kafka (KRaft mode), Debezium CDC, Spark Structured Streaming |
| Cloud warehouse | Snowflake (`EARTHQUAKES`, `DIM_PORT`, `FACT_EARTHQUAKE_PORT_RISK`) |
| Operational source | PostgreSQL (seismic source table) |
| Orchestration | Apache Airflow 2.5.1 |
| BI / Reporting | Power BI (`.pbip` project) |
| Environment | Docker Compose (itversity `itvdelab` Hadoop/Spark/Hive sandbox image) |

---

## Getting Started

> Requires Docker and Docker Compose.

1. **Bring up the stack**
   ```bash
   docker compose up -d
   ```
   This starts Kafka, Postgres (seismic source + Debezium init), Kafka Connect, the SFTP server, the `itvdelab` Hadoop/Spark/Hive sandbox, Airflow, and the seismic stream extractor.

2. **Register the CDC connector** (Postgres → Kafka)
   ```bash
   bash data_extraction/register-connector.sh
   ```

3. **Run the batch pipeline** (inside the `itvdelab` container)
   ```bash
   docker exec -it itvdelab bash
   bash /bronze_scripts/load_ports_to_hdfs.sh
   bash /bronze_scripts/load_vessels_to_hdfs.sh
   spark-submit /silver_scripts/Spark_job/ports_to_silver.py
   spark-submit /gold_scripts/Spark_job/Ports_to_gold.py
   ```

4. **Run the streaming jobs**
   ```bash
   spark-submit Spark_job/ingest_snowflake.py     # Kafka CDC → Snowflake
   spark-submit Spark_job/risk_analysis.py        # Earthquake–port risk analysis
   ```

5. **Orchestrate via Airflow** — DAGs in `airflow/dags/` (`Batch_ETL_DAG.py`, `Stream_ETL_DAG.py`) encode the same ordering with dependency checks, and can be triggered from the Airflow UI once `airflow/init_airflow.sh` has been run.

6. **Open the dashboard** — `Power_BI_Dashboard/Dashboard.pbip` in Power BI Desktop, pointed at the Snowflake tables populated above.

See `Bronze_Layer/readme.md`, `Silver_Layer/readme.md`, and `Gold_Layer/readme.md` for exact verification commands (`hdfs dfs -ls`, Hive `DESCRIBE`/`SELECT`, etc.).

---

## Pipeline Walkthrough

**Batch (Ports)**
1. **Bronze** — `load_ports_to_hdfs.sh` pulls `ports_YYYY-MM-DD.csv` files from SFTP and lands them in `HDFS /raw_layer/ports/year=YYYY/month=MM/`, deleting the source only after a confirmed HDFS write.
2. **Silver** — `ports_to_silver.py` standardizes column names, casts types per `config/silver_schema.yaml`, cleans strings, enforces NOT NULL rules, deduplicates on `oid`, and writes partitioned Parquet to `/silver_layer/ports`.
3. **Gold** — `Ports_to_gold.py` selects the latest snapshot, validates the `oid` business key, computes `supplies_count`/`supplies_rate` and `communications_count`/`comm_rate`, assigns stable surrogate keys (`port_key`) that persist across runs, and writes the current-state Port Dimension to `/gold_layer/ports`.

**Streaming (Earthquakes)**
1. **CDC replication** (`ingest_snowflake.py`) — consumes Debezium CDC events from Kafka every 30 seconds and applies them to Snowflake's `EARTHQUAKES` table via a staged `MERGE` (insert/update/delete based on the CDC `__op` code).
2. **Risk analysis** (`risk_analysis.py`) — for each earthquake batch, computes a magnitude-based impact radius, pre-filters nearby ports with a lat/lon bounding box, computes precise Haversine distances, classifies risk (`HIGH`/`MEDIUM`/`LOW`), and writes results to `FACT_EARTHQUAKE_PORT_RISK`.
3. **Archiving** (`snowflake_table_archive.py`) — a scheduled, watermark-based job that periodically moves older `EARTHQUAKES` rows from Snowflake into partitioned Parquet on HDFS, keeping the hot Snowflake table lean without losing history.

---

## Dashboard

The Power BI project (`Power_BI_Dashboard/Dashboard.pbip`) is a two-page operational dashboard sitting directly on the Snowflake Gold/ODS tables:

- **Page 1 — Earthquake & Port Risk Monitor:** risk-level and date-range filters, KPI cards, a detail table of recent earthquakes, a risk-level comparison chart, and a world map plotting earthquakes and ports together.
- **Page 2 — Port Analysis:** a global port map, harbor-type breakdown, and supplies/communications availability pie charts sourced from the Gold Port Dimension.

---


Full technical detail on each of these is available in `Tadline_Pipeline_Documentation.docx`.
