#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Project Base Directory
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
sys.path.append(BASE_DIR)

# Logger
from utilities.logger import configure_logger

logger = configure_logger(
    log_name="EarthquakeCDC",
    log_file="cdc_earthquake_streaming",
    log_dir="/spark_jobs/logs"
)

# Load Snowflake credentials from environment variables
load_dotenv()

sfOptions = {
    "sfURL": os.getenv("SNOWFLAKE_ACCOUNT"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    "sfAsyncResultSetEnabled": "false"
}

# ============================================================================
# Schema Definition
# ============================================================================

earthquake_schema = StructType([
    StructField("unid", StringType()),
    StructField("source_id", StringType()),
    StructField("source_catalog", StringType()),
    StructField("lastupdate", LongType()),
    StructField("time", LongType()),
    StructField("flynn_region", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("depth", DoubleType()),
    StructField("evtype", StringType()),
    StructField("auth", StringType()),
    StructField("mag", DoubleType()),
    StructField("magtype", StringType()),
    StructField("action", StringType()),
    StructField("received_at", LongType()),
    StructField("__op", StringType()),
    StructField("__ts_ms", LongType())
])

# ============================================================================
# Spark Session
# ============================================================================

spark = SparkSession.builder \
    .appName("KafkaEarthquakeToSnowflake") \
    .config("spark.snowflake.use_copy_unload", "false") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

logger.info("=" * 60)
logger.info("TidalLine - Kafka to Snowflake Streaming")
logger.info("=" * 60)

# ============================================================================
# Kafka Configuration
# ============================================================================

kafka_broker = "kafka:29092"
kafka_topic = "earthquakes_json.public.earthquakes"

logger.info(f"Reading from Kafka: {kafka_topic}")
logger.info(f"Kafka Broker: {kafka_broker}")

# ============================================================================
# Batch Processing Function
# ============================================================================

def process_cdc_batch(batch_df, batch_id):
    logger.info(f"[BATCH {batch_id}] started")
    
    if batch_df.limit(1).count() == 0:
        logger.info(f"[BATCH {batch_id}] empty batch, skipping.")
        return
    
    try:
        # Count operations
        inserts_count = batch_df.filter(F.col("__op").isin("c", "r")).count()
        updates_count = batch_df.filter(F.col("__op") == "u").count()
        deletes_count = batch_df.filter(F.col("__op") == "d").count()
        
        logger.info(
            f"[BATCH {batch_id}] Inserts: {inserts_count}, "
            f"Updates: {updates_count}, Deletes: {deletes_count}"
        )
        
        if inserts_count == 0 and updates_count == 0 and deletes_count == 0:
            logger.info(f"[BATCH {batch_id}] No CDC operations to process.")
            return
        
        # Transform data
        transformed_df = batch_df \
            .withColumn("lastupdate", (F.col("lastupdate") / 1000).cast("timestamp")) \
            .withColumn("time", (F.col("time") / 1000).cast("timestamp")) \
            .withColumn("received_at", (F.col("received_at") / 1000).cast("timestamp")) \
            .withColumn("event_date", F.to_date(F.col("time"))) \
            .drop("__ts_ms")
        
        # Prepare data for staging
        prepared_df = transformed_df.select(
            F.col("unid"),
            F.col("source_id"),
            F.col("source_catalog"),
            F.col("lastupdate").cast("timestamp").alias("lastupdate"),
            F.col("time").cast("timestamp").alias("time"),
            F.col("received_at").cast("timestamp").alias("received_at"),
            F.col("flynn_region"),
            F.col("lat").cast("double").alias("lat"),
            F.col("lon").cast("double").alias("lon"),
            F.col("depth").cast("double").alias("depth"),
            F.col("evtype"),
            F.col("auth"),
            F.col("mag").cast("double").alias("mag"),
            F.col("magtype"),
            F.col("action"),
            F.col("event_date").cast("date").alias("event_date"),
            F.col("__op")
        )
        
        # Get Snowflake utilities
        spark = SparkSession.getActiveSession()
        Utils = spark._jvm.net.snowflake.spark.snowflake.Utils
        java_map = spark._jvm.java.util.HashMap()
        for k, v in sfOptions.items():
            java_map.put(k, v)
        
        # Write to staging table
        logger.info(f"[BATCH {batch_id}] Writing to staging table...")
        
        prepared_df.write \
            .format("snowflake") \
            .options(**sfOptions) \
            .option("dbtable", "CDC_STAGING") \
            .mode("overwrite") \
            .save()
        
        logger.info(f"[BATCH {batch_id}] Staging table populated with {prepared_df.count()} records")
        
        # Execute MERGE
        logger.info(f"[BATCH {batch_id}] Executing MERGE...")
        
        merge_query = """
            MERGE INTO EARTHQUAKES t
            USING CDC_STAGING s
            ON t.UNID = s.unid

            WHEN MATCHED AND s.__op = 'd' THEN DELETE

            WHEN MATCHED AND s.__op = 'u' THEN UPDATE SET
                t.SOURCE_ID = s.source_id,
                t.SOURCE_CATALOG = s.source_catalog,
                t.LASTUPDATE = s.LASTUPDATE,
                t.TIME = s.TIME,
                t.RECEIVED_AT = s.RECEIVED_AT,
                t.FLYNN_REGION = s.flynn_region,
                t.LAT = s.lat,
                t.LON = s.lon,
                t.DEPTH = s.depth,
                t.EVTYPE = s.evtype,
                t.AUTH = s.auth,
                t.MAG = s.mag,
                t.MAGTYPE = s.magtype,
                t.ACTION = s.action,
                t.EVENT_DATE = s.event_date

            WHEN NOT MATCHED AND s.__op IN ('c', 'r') THEN INSERT (
                UNID, SOURCE_ID, SOURCE_CATALOG, LASTUPDATE, TIME, RECEIVED_AT,
                FLYNN_REGION, LAT, LON, DEPTH, EVTYPE, AUTH, MAG, MAGTYPE, ACTION, EVENT_DATE
            ) VALUES (
                s.unid, s.source_id, s.source_catalog, s.LASTUPDATE, s.TIME, s.RECEIVED_AT,
                s.flynn_region, s.lat, s.lon, s.depth, s.evtype, s.auth, s.mag, s.magtype, s.action, s.event_date
            )
        """
        
        Utils.runQuery(java_map, merge_query)
        
        # Clean up staging table
        Utils.runQuery(java_map, "DROP TABLE IF EXISTS CDC_STAGING")
        
        logger.info(f"[BATCH {batch_id}] Successfully processed CDC operations via MERGE")
        logger.info(f"[BATCH {batch_id}] completed")
        
    except Exception as e:
        logger.exception(f"[BATCH {batch_id}] ERROR: {e}")

# ============================================================================
# Read Stream and Start Processing
# ============================================================================

raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = raw_kafka_df.select(
    F.from_json(F.col("value").cast("string"), earthquake_schema).alias("data")
).select("data.*")

query = parsed_df.writeStream \
    .foreachBatch(process_cdc_batch) \
    .outputMode("append") \
    .trigger(processingTime='30 seconds') \
    .start()

logger.info("Streaming started! Writing to Snowflake with CDC support...")
logger.info("-" * 60)

print("\n" + "=" * 60)
print("Streaming started! Writing to Snowflake with CDC support...")
print("=" * 60 + "\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Stream stopped by user.")
    print("\n[INFO] Stopping stream...")
    query.stop()
    spark.stop()
    logger.info("Spark session stopped.")
    print("[INFO] Stream stopped.")