from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year,
    month,
    dayofmonth,
    count
)

import snowflake.connector

import os
import sys

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

sys.path.insert(0, BASE_DIR)

from utilities.logger import configure_logger

import re

def load_config(config_file):
    config = {}

    with open(config_file, "r") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            match = re.match(r'export\s+(\w+)=(["\']?)(.*?)\2$', line)

            if match:
                key = match.group(1)
                value = match.group(3)
                config[key] = value

    return config


config = load_config("/bronze_scripts/config.sh")

hdfs_base_path = config["HDFS_BASE_PATH"]

print(hdfs_base_path)

# Configuration

SOURCE_TABLE = "streaming_db.kafka.earthquakes"

WATERMARK_TABLE = (
    "streaming_db.kafka.earthquake_archive_watermark"
)

HDFS_BASE_PATH = f"{hdfs_base_path}/earthquakes"

PIPELINE_NAME = "earthquake_daily_archive"

logger = configure_logger(
    log_name="earthquake_archive",
    log_file="earthquake_archive",
    log_dir="/bronze_scripts/logs"
)

logger.info("[START] Earthquake daily archive job started")


# Snowflake configuration

logger.info("[CONFIG] Setting Snowflake configuration")

SF_ACCOUNT = "qn44692.eu-central-2.aws"
SF_USER = "SPARK"
SF_PASSWORD = "12345678"

SF_DATABASE = "STREAMING_DB"
SF_SCHEMA = "KAFKA"
SF_WAREHOUSE = "STREAMING_WH"
SF_ROLE = "SPARK_ROLE"

logger.info(f"[CONFIG] Snowflake account: {SF_ACCOUNT}")
logger.info(f"[CONFIG] Database: {SF_DATABASE}")
logger.info(f"[CONFIG] Schema: {SF_SCHEMA}")
logger.info(f"[CONFIG] Warehouse: {SF_WAREHOUSE}")
logger.info(f"[CONFIG] Role: {SF_ROLE}")

# Spark Snowflake Connector
sf_options = {
    "sfURL": f"{SF_ACCOUNT}.snowflakecomputing.com",
    "sfUser": SF_USER,
    "sfPassword": SF_PASSWORD,
    "sfDatabase": SF_DATABASE,
    "sfSchema": SF_SCHEMA,
    "sfWarehouse": SF_WAREHOUSE,
    "sfRole": SF_ROLE
}

SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"

logger.info("[CONFIG] Snowflake configuration created")


# Spark

logger.info("[SPARK] Creating SparkSession")
logger.info("Starting earthquake daily archive job")

spark = (
    SparkSession.builder
    .appName("EarthquakeDailyArchive")
    .getOrCreate()
)

logger.info("[SPARK] SparkSession created successfully")


# Read watermark

logger.info("[WATERMARK] Reading previous watermark")

watermark_query = f"""
SELECT last_ingestion_date
FROM {WATERMARK_TABLE}
WHERE pipeline_name = '{PIPELINE_NAME}'
"""

logger.info("[WATERMARK] Query:")
logger.info(watermark_query)

watermark_df = (
    spark.read
    .format(SNOWFLAKE_FORMAT)
    .options(**sf_options)
    .option(
        "query",
        watermark_query
    )
    .load()
)

logger.info("[WATERMARK] Watermark query executed successfully")

watermark_row = watermark_df.first()

if watermark_row is None:
    logger.info("[ERROR] No watermark found!")
    raise RuntimeError(
        "No watermark found for earthquake archive pipeline"
    )

last_watermark = watermark_row["LAST_INGESTION_DATE"]

logger.info(
    f"[WATERMARK] Previous watermark: {last_watermark}"
)

# Establish the upper boundary

logger.info("[BOUNDARY] Getting current Snowflake timestamp")

end_df = (
    spark.read
    .format(SNOWFLAKE_FORMAT)
    .options(**sf_options)
    .option(
        "query",
        "SELECT CURRENT_TIMESTAMP() AS run_end"
    )
    .load()
)

logger.info("[BOUNDARY] CURRENT_TIMESTAMP query executed")

run_end = end_df.first()["RUN_END"]

logger.info(
    f"[BOUNDARY] Current archive upper boundary: {run_end}"
)


# Read only new earthquakes

logger.info("[SOURCE] Reading new earthquake records from Snowflake")

query = f"""
SELECT
    UNID AS unid,
    SOURCE_ID,
    TIME AS event_timestamp,
    FLYNN_REGION,
    LAT AS latitude,
    LON AS longitude,
    DEPTH AS depth,
    MAG AS magnitude,
    MAGTYPE AS magnitude_type,
    EVTYPE AS event_type,
    SOURCE_CATALOG,
    AUTH AS authority,
    Action,
    RECEIVED_AT,
    EVENT_DATE AS ingestion_date
FROM {SOURCE_TABLE}
WHERE ingestion_date > '{last_watermark}'
  AND ingestion_date <= '{run_end}'
"""

logger.info("[SOURCE] Earthquake query:")
logger.info(query)

earthquakes_df = (
    spark.read
    .format(SNOWFLAKE_FORMAT)
    .options(**sf_options)
    .option("query", query)
    .load()
)

logger.info("[SOURCE] Earthquake data loaded successfully")

# Count records

logger.info("[COUNT] Counting new earthquake records")

record_count = earthquakes_df.count()

logger.info(
    f"[COUNT] New earthquake records found: {record_count}"
)

# Nothing to archive

if record_count == 0:

    logger.info(
        "[ARCHIVE] No new earthquake records found."
    )

    logger.info(
        "[ARCHIVE] Watermark will NOT be changed."
    )

else:

    logger.info(
        f"[ARCHIVE] {record_count} records need to be archived"
    )

    # Add partition columns

    logger.info(
        "[TRANSFORM] Creating HDFS partition columns"
    )


    archive_df = (
        earthquakes_df
        .withColumn(
            "year",
            year("event_timestamp")
        )
        .withColumn(
            "month",
            month("event_timestamp")
        )
        .withColumn(
            "day",
            dayofmonth("event_timestamp")
        )
    )

    logger.info(
        "[TRANSFORM] Partition columns created: "
        "year, month, day"
    )

    # Write to HDFS

    logger.info(
        f"[HDFS] Writing {record_count} records "
        f"to {HDFS_BASE_PATH}"
    )

    (
        archive_df
        .write
        .mode("append")
        .partitionBy(
            "year",
            "month",
            "day"
        )
        .parquet(
            HDFS_BASE_PATH
        )
    )

    logger.info(
        "[HDFS] HDFS write completed successfully"
    )

    # Verify records

    logger.info(
        "[VERIFY] Verifying archived records in HDFS"
    )

    archived_count = (
        spark.read
        .parquet(HDFS_BASE_PATH)
        .filter(
            f"""
            ingestion_date > '{last_watermark}'
            AND ingestion_date <= '{run_end}'
            """
        )
        .count()
    )

    logger.info(
        f"[VERIFY] Expected records: {record_count}"
    )

    logger.info(
        f"[VERIFY] Records found in HDFS: {archived_count}"
    )

    if archived_count < record_count:

        logger.info(
            "[VERIFY] ERROR: HDFS validation failed!"
        )

        raise RuntimeError(
            f"HDFS validation failed. "
            f"Expected at least {record_count}, "
            f"found {archived_count}"
        )

    logger.info(
        "[VERIFY] HDFS validation successful"
    )

    # Update watermark

    logger.info(
        f"[WATERMARK] Preparing to update watermark "
        f"to {run_end}"
    )

    update_query = f"""
    MERGE INTO {WATERMARK_TABLE} target
    USING (
        SELECT
            '{PIPELINE_NAME}' AS pipeline_name,
            '{run_end}'::TIMESTAMP_LTZ AS last_ingestion_date
    ) source
    ON target.pipeline_name = source.pipeline_name

    WHEN MATCHED THEN UPDATE SET
        target.last_ingestion_date =
            source.last_ingestion_date,
        target.updated_at =
            CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        pipeline_name,
        last_ingestion_date
    )
    VALUES (
        source.pipeline_name,
        source.last_ingestion_date
    )
    """

    logger.info("[WATERMARK] MERGE query:")
    logger.info(update_query)


    # Connect to Snowflake

    logger.info(
        "[SNOWFLAKE] Connecting to Snowflake..."
    )

    conn = None

    try:

        conn = snowflake.connector.connect(
            account=SF_ACCOUNT,
            user=SF_USER,
            password=SF_PASSWORD,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            schema=SF_SCHEMA,
            role=SF_ROLE
        )

        logger.info(
            "[SNOWFLAKE] Connection established successfully"
        )

        # Execute MERGE

        logger.info(
            "[WATERMARK] Executing MERGE..."
        )

        with conn.cursor() as cursor:

            logger.info(
                "Executing watermark MERGE"
            )

            cursor.execute(update_query)

            logger.info(
                "[WATERMARK] MERGE executed successfully"
            )

        # Commit

        logger.info(
            "[WATERMARK] Committing transaction..."
        )

        conn.commit()

        logger.info(
            f"[WATERMARK] Watermark successfully updated "
            f"to {run_end}"
        )

    except Exception as e:

        logger.info(
            f"[ERROR] Failed to update watermark: {e}"
        )

        if conn is not None:

            logger.info(
                "[SNOWFLAKE] Rolling back transaction"
            )

            conn.rollback()

        raise

    finally:

        if conn is not None:

            logger.info(
                "[SNOWFLAKE] Closing Snowflake connection"
            )

            conn.close()

            logger.info(
                "[SNOWFLAKE] Snowflake connection closed"
            )

# Finish

logger.info(
    "[FINISH] Earthquake daily archive job completed successfully"
)


spark.stop()

logger.info("[SPARK] Spark session stopped")