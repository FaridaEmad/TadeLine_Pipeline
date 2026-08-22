#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import math

# Project Base Directory
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
sys.path.append(BASE_DIR)

# Logger
from utilities.logger import configure_logger

logger = configure_logger(
    log_name="EarthquakeRiskAnalysis",
    log_file="earthquake_risk_analysis_streaming",
    log_dir="/spark_jobs/logs"
)

# Load Snowflake credentials
load_dotenv()

sfOptions = {
    "sfURL": os.getenv("SNOWFLAKE_ACCOUNT"),
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    "sfCompress": "true",
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
# Haversine UDF
# ============================================================================

def haversine(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

haversine_udf = F.udf(haversine, DoubleType())

# ============================================================================
# Load Ports from Snowflake
# ============================================================================

def load_ports_from_snowflake(spark):
    """Load ports data from Snowflake DIM_PORT table."""
    logger.info("Loading ports from Snowflake...")
    
    ports_df = (
        spark.read
        .format("snowflake")
        .options(**sfOptions)
        .option("query", """
            SELECT 
                PORT_KEY,
                LATITUDE AS port_latitude,
                LONGITUDE AS port_longitude
            FROM DIM_PORT
        """)
        .load()
    )
    
    port_count = ports_df.count()
    logger.info(f"Loaded {port_count} ports from Snowflake")
    return ports_df

# ============================================================================
# Spark Session
# ============================================================================

spark = SparkSession.builder \
    .appName("EarthquakePortRiskAnalysis") \
    .config("spark.snowflake.use_copy_unload", "false") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

logger.info("=" * 60)
logger.info("TidalLine - Earthquake-Port Risk Analysis Streaming")
logger.info("=" * 60)

# ============================================================================
# Load Ports Data from Snowflake
# ============================================================================

ports_df = load_ports_from_snowflake(spark)
ports_df.cache()
port_count = ports_df.count()
logger.info(f"Cached {port_count} ports")

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

accumulated_batches = []
batch_counter = 0
last_write_time = 0

def process_batch(batch_df, batch_id):
    global accumulated_batches, batch_counter, last_write_time
    
    logger.info(f"[BATCH {batch_id}] started")
    
    if batch_df.limit(1).count() == 0:
        logger.info(f"[BATCH {batch_id}] empty batch, skipping.")
        return
    
    # Transform earthquakes
    transformed_df = batch_df \
        .withColumn("lastupdate", (F.col("lastupdate") / 1000).cast("timestamp")) \
        .withColumn("time", (F.col("time") / 1000).cast("timestamp")) \
        .withColumn("received_at", (F.col("received_at") / 1000).cast("timestamp")) \
        .withColumn("event_year", F.year(F.col("time"))) \
        .withColumn("event_month", F.month(F.col("time"))) \
        .withColumn("event_date", F.to_date(F.col("time"))) \
        .drop("__ts_ms")
    
    earthquake_count = transformed_df.count()
    logger.info(f"[BATCH {batch_id}] Processing {earthquake_count} earthquakes")
    
    # Add impact radius
    earthquakes_with_radius = transformed_df \
        .withColumn(
            "impact_radius_km",
            F.when(F.col("mag") < 4.0, 50)
             .when(F.col("mag") < 5.0, 100)
             .when(F.col("mag") < 6.0, 200)
             .when(F.col("mag") < 7.0, 500)
             .when(F.col("mag") < 8.0, 1000)
             .otherwise(2000)
        ) \
        .withColumn(
            "lat_min",
            F.col("lat") - (F.col("impact_radius_km") / 111.0)
        ) \
        .withColumn(
            "lat_max",
            F.col("lat") + (F.col("impact_radius_km") / 111.0)
        ) \
        .withColumn(
            "lon_min",
            F.col("lon") - (F.col("impact_radius_km") / (111.0 * F.cos(F.radians(F.col("lat")))))
        ) \
        .withColumn(
            "lon_max",
            F.col("lon") + (F.col("impact_radius_km") / (111.0 * F.cos(F.radians(F.col("lat")))))
        )
    
    # Find affected ports
    affected_pairs = earthquakes_with_radius \
        .join(
            ports_df,
            (F.col("port_latitude").between(F.col("lat_min"), F.col("lat_max"))) &
            (F.col("port_longitude").between(F.col("lon_min"), F.col("lon_max"))),
            "inner"
        ) \
        .withColumn(
            "distance_km",
            haversine_udf(
                F.col("lat"), F.col("lon"),
                F.col("port_latitude"), F.col("port_longitude")
            )
        ) \
        .filter(F.col("distance_km") <= F.col("impact_radius_km")) \
        .withColumn(
            "impact_level",
            F.when(F.col("distance_km") <= F.col("impact_radius_km") * 0.3, "HIGH")
             .when(F.col("distance_km") <= F.col("impact_radius_km") * 0.6, "MEDIUM")
             .otherwise("LOW")
        ) \
        .select(
            F.col("unid").alias("earthquake_id"),
            F.col("port_key"),
            F.col("port_latitude"),
            F.col("port_longitude"),
            F.col("distance_km"),
            F.col("impact_radius_km"),
            F.col("impact_level"),
            F.col("event_year"),
            F.col("event_month"),
            F.col("event_date")
        )
    
    # Build final result
    earthquakes_with_keys = transformed_df.select(
        F.col("unid").alias("earthquake_id"),
        F.col("time"),
        F.col("lat"),
        F.col("lon"),
        F.col("mag"),
        F.col("depth"),
        F.col("flynn_region"),
        F.col("event_year"),
        F.col("event_month"),
        F.col("event_date")
    )

    result_df = earthquakes_with_keys.alias("eq") \
        .join(
            affected_pairs.alias("ports"),
            F.col("eq.earthquake_id") == F.col("ports.earthquake_id"),
            "left"
        ) \
        .select(
            F.col("eq.earthquake_id").cast("string").alias("EARTHQUAKE_KEY"),
            F.coalesce(F.col("ports.port_key"), F.lit(-1)).cast("long").alias("PORT_KEY"),
            F.date_format(F.col("eq.event_date"), "yyyyMMdd").cast("long").alias("EARTHQUAKE_DATE_KEY"),
            F.col("ports.distance_km").cast("double").alias("DISTANCE_KM"),
            F.col("ports.impact_radius_km").cast("double").alias("RISK_RADIUS_KM"),
            F.when(F.col("ports.distance_km").isNotNull(), 
                   F.col("ports.distance_km") <= F.col("ports.impact_radius_km"))
             .otherwise(F.lit(False)).alias("IS_NEARBY"),
            F.coalesce(F.col("ports.impact_level"), F.lit("NONE")).alias("RISK_LEVEL"),
            F.when(
                F.col("ports.distance_km").isNotNull(),
                F.concat(
                    F.lit("Earthquake of magnitude "),
                    F.col("eq.mag"),
                    F.lit(" at "),
                    F.col("ports.distance_km"),
                    F.lit(" km from port")
                )
            ).otherwise(F.lit("No nearby ports")).alias("RISK_REASON"),
            F.col("eq.event_year").cast("int").alias("EVENT_YEAR"),
            F.col("eq.event_month").cast("int").alias("EVENT_MONTH")
        ) \
        .fillna({
            "DISTANCE_KM": -1.0,
            "RISK_RADIUS_KM": -1.0,
            "IS_NEARBY": False,
            "RISK_LEVEL": "NONE",
            "RISK_REASON": "No nearby ports"
        }) \
        .coalesce(1)
    
    # Log summary
    nearby_count = result_df.filter(F.col("IS_NEARBY") == True).count()
    high_risk_count = result_df.filter(F.col("RISK_LEVEL") == "HIGH").count()
    
    logger.info(f"[BATCH {batch_id}] Summary:")
    logger.info(f"  Total earthquake-port pairs: {result_df.count()}")
    logger.info(f"  Nearby ports: {nearby_count}")
    logger.info(f"  HIGH risk ports: {high_risk_count}")
    
    # Accumulate
    accumulated_batches.append(result_df)
    current_time = time.time()
    batch_counter += 1
    
    # FIXED: Write every batch (or every 2 batches for testing)
    should_write = (
        len(accumulated_batches) >= 1 or  # Write every batch
        (current_time - last_write_time) > 60 or  # Or every 1 minute
        batch_counter == 1
    )
    
    if should_write and accumulated_batches:
        logger.info(f"[BATCH {batch_id}] Writing accumulated data to Snowflake...")
        
        combined_df = accumulated_batches[0]
        for df in accumulated_batches[1:]:
            combined_df = combined_df.union(df)
        
        combined_df = combined_df.coalesce(max(1, combined_df.rdd.getNumPartitions() // 4))
        total_records = combined_df.count()
        
        if total_records > 0:
            # Use JDBC directly (skip Snowflake connector)
            logger.info(f"[BATCH {batch_id}] Using JDBC INSERT...")
            
            spark = SparkSession.getActiveSession()
            Utils = spark._jvm.net.snowflake.spark.snowflake.Utils
            java_map = spark._jvm.java.util.HashMap()
            for k, v in sfOptions.items():
                java_map.put(k, v)
            
            rows = combined_df.collect()
            inserted_count = 0
            table_name = "FACT_EARTHQUAKE_PORT_RISK"
            
            for row in rows:
                try:
                    earthquake_key = f"'{row.EARTHQUAKE_KEY}'" if row.EARTHQUAKE_KEY else "NULL"
                    port_key = str(row.PORT_KEY) if row.PORT_KEY is not None else "NULL"
                    earthquake_date_key = str(row.EARTHQUAKE_DATE_KEY) if row.EARTHQUAKE_DATE_KEY is not None else "NULL"
                    distance_km = str(row.DISTANCE_KM) if row.DISTANCE_KM is not None else "NULL"
                    risk_radius_km = str(row.RISK_RADIUS_KM) if row.RISK_RADIUS_KM is not None else "NULL"
                    is_nearby = "TRUE" if row.IS_NEARBY else "FALSE"
                    risk_level = f"'{row.RISK_LEVEL}'" if row.RISK_LEVEL else "NULL"
                    risk_reason = f"'{row.RISK_REASON}'" if row.RISK_REASON else "NULL"
                    event_year = str(row.EVENT_YEAR) if row.EVENT_YEAR is not None else "NULL"
                    event_month = str(row.EVENT_MONTH) if row.EVENT_MONTH is not None else "NULL"
                    
                    insert_query = f"""
                        INSERT INTO {table_name} (
                            EARTHQUAKE_KEY, PORT_KEY, EARTHQUAKE_DATE_KEY,
                            DISTANCE_KM, RISK_RADIUS_KM, IS_NEARBY,
                            RISK_LEVEL, RISK_REASON, EVENT_YEAR, EVENT_MONTH
                        ) VALUES (
                            {earthquake_key},
                            {port_key},
                            {earthquake_date_key},
                            {distance_km},
                            {risk_radius_km},
                            {is_nearby},
                            {risk_level},
                            {risk_reason},
                            {event_year},
                            {event_month}
                        )
                    """
                    Utils.runQuery(java_map, insert_query)
                    inserted_count += 1
                    
                except Exception as e:
                    logger.exception(f"Error inserting row: {e}")
                    continue
            
            logger.info(f"[BATCH {batch_id}] Inserted {inserted_count}/{len(rows)} records via JDBC")
            
            accumulated_batches = []
            last_write_time = current_time
        else:
            logger.info(f"[BATCH {batch_id}] No records to write, clearing accumulated data")
            accumulated_batches = []
            last_write_time = current_time
    
    logger.info(f"[BATCH {batch_id}] completed")


# def process_batch(batch_df, batch_id):
#     global accumulated_batches, batch_counter, last_write_time
    
#     logger.info(f"[BATCH {batch_id}] started")
    
#     if batch_df.limit(1).count() == 0:
#         logger.info(f"[BATCH {batch_id}] empty batch, skipping.")
#         return
    
#     # Transform earthquakes
#     transformed_df = batch_df \
#         .withColumn("lastupdate", (F.col("lastupdate") / 1000).cast("timestamp")) \
#         .withColumn("time", (F.col("time") / 1000).cast("timestamp")) \
#         .withColumn("received_at", (F.col("received_at") / 1000).cast("timestamp")) \
#         .withColumn("event_year", F.year(F.col("time"))) \
#         .withColumn("event_month", F.month(F.col("time"))) \
#         .withColumn("event_date", F.to_date(F.col("time"))) \
#         .drop("__ts_ms")
    
#     earthquake_count = transformed_df.count()
#     logger.info(f"[BATCH {batch_id}] Processing {earthquake_count} earthquakes")
    
#     # Add impact radius
#     earthquakes_with_radius = transformed_df \
#         .withColumn(
#             "impact_radius_km",
#             F.when(F.col("mag") < 4.0, 50)
#              .when(F.col("mag") < 5.0, 100)
#              .when(F.col("mag") < 6.0, 200)
#              .when(F.col("mag") < 7.0, 500)
#              .when(F.col("mag") < 8.0, 1000)
#              .otherwise(2000)
#         ) \
#         .withColumn(
#             "lat_min",
#             F.col("lat") - (F.col("impact_radius_km") / 111.0)
#         ) \
#         .withColumn(
#             "lat_max",
#             F.col("lat") + (F.col("impact_radius_km") / 111.0)
#         ) \
#         .withColumn(
#             "lon_min",
#             F.col("lon") - (F.col("impact_radius_km") / (111.0 * F.cos(F.radians(F.col("lat")))))
#         ) \
#         .withColumn(
#             "lon_max",
#             F.col("lon") + (F.col("impact_radius_km") / (111.0 * F.cos(F.radians(F.col("lat")))))
#         )
    
#     # Find affected ports
#     affected_pairs = earthquakes_with_radius \
#         .join(
#             ports_df,
#             (F.col("port_latitude").between(F.col("lat_min"), F.col("lat_max"))) &
#             (F.col("port_longitude").between(F.col("lon_min"), F.col("lon_max"))),
#             "inner"
#         ) \
#         .withColumn(
#             "distance_km",
#             haversine_udf(
#                 F.col("lat"), F.col("lon"),
#                 F.col("port_latitude"), F.col("port_longitude")
#             )
#         ) \
#         .filter(F.col("distance_km") <= F.col("impact_radius_km")) \
#         .withColumn(
#             "impact_level",
#             F.when(F.col("distance_km") <= F.col("impact_radius_km") * 0.3, "HIGH")
#              .when(F.col("distance_km") <= F.col("impact_radius_km") * 0.6, "MEDIUM")
#              .otherwise("LOW")
#         ) \
#         .select(
#             F.col("unid").alias("earthquake_id"),
#             F.col("port_key"),
#             F.col("port_latitude"),
#             F.col("port_longitude"),
#             F.col("distance_km"),
#             F.col("impact_radius_km"),
#             F.col("impact_level"),
#             F.col("event_year"),
#             F.col("event_month"),
#             F.col("event_date")
#         )
    
#     # Build final result - UPPERCASE columns for Snowflake
#     earthquakes_with_keys = transformed_df.select(
#         F.col("unid").alias("earthquake_id"),
#         F.col("time"),
#         F.col("lat"),
#         F.col("lon"),
#         F.col("mag"),
#         F.col("depth"),
#         F.col("flynn_region"),
#         F.col("event_year"),
#         F.col("event_month"),
#         F.col("event_date")
#     )

#     result_df = earthquakes_with_keys.alias("eq") \
#         .join(
#             affected_pairs.alias("ports"),
#             F.col("eq.earthquake_id") == F.col("ports.earthquake_id"),
#             "left"
#         ) \
#         .select(
#             # UPPERCASE column names matching Snowflake
#             F.col("eq.earthquake_id").cast("string").alias("EARTHQUAKE_KEY"),
#             F.coalesce(F.col("ports.port_key"), F.lit(-1)).cast("long").alias("PORT_KEY"),
#             F.col("eq.event_date").cast("long").alias("EARTHQUAKE_DATE_KEY"),
#             F.col("ports.distance_km").cast("double").alias("DISTANCE_KM"),
#             F.col("ports.impact_radius_km").cast("double").alias("RISK_RADIUS_KM"),
#             F.when(F.col("ports.distance_km").isNotNull(), 
#                    F.col("ports.distance_km") <= F.col("ports.impact_radius_km"))
#              .otherwise(F.lit(False)).alias("IS_NEARBY"),
#             F.coalesce(F.col("ports.impact_level"), F.lit("NONE")).alias("RISK_LEVEL"),
#             F.when(
#                 F.col("ports.distance_km").isNotNull(),
#                 F.concat(
#                     F.lit("Earthquake of magnitude "),
#                     F.col("eq.mag"),
#                     F.lit(" at "),
#                     F.col("ports.distance_km"),
#                     F.lit(" km from port")
#                 )
#             ).otherwise(F.lit("No nearby ports")).alias("RISK_REASON"),
#             F.col("eq.event_year").cast("int").alias("EVENT_YEAR"),
#             F.col("eq.event_month").cast("int").alias("EVENT_MONTH")
#         ) \
#         .fillna({
#             "DISTANCE_KM": -1.0,
#             "RISK_RADIUS_KM": -1.0,
#             "IS_NEARBY": False,
#             "RISK_LEVEL": "NONE",
#             "RISK_REASON": "No nearby ports"
#         }) \
#         .coalesce(1)
    
#     # Log summary
#     nearby_count = result_df.filter(F.col("IS_NEARBY") == True).count()
#     high_risk_count = result_df.filter(F.col("RISK_LEVEL") == "HIGH").count()
    
#     logger.info(f"[BATCH {batch_id}] Summary:")
#     logger.info(f"  Total earthquake-port pairs: {result_df.count()}")
#     logger.info(f"  Nearby ports: {nearby_count}")
#     logger.info(f"  HIGH risk ports: {high_risk_count}")
    
#     # Accumulate
#     accumulated_batches.append(result_df)
#     current_time = time.time()
#     batch_counter += 1
    
#     # Write every 10 batches OR every 10 minutes
#     should_write = (
#         len(accumulated_batches) >= 10 or
#         (current_time - last_write_time) > 600 or
#         batch_counter == 1
#     )
    
#     if should_write and accumulated_batches:
#         logger.info(f"[BATCH {batch_id}] Writing accumulated data to Snowflake...")
        
#         combined_df = accumulated_batches[0]
#         for df in accumulated_batches[1:]:
#             combined_df = combined_df.union(df)
        
#         combined_df = combined_df.coalesce(max(1, combined_df.rdd.getNumPartitions() // 4))
#         total_records = combined_df.count()
        
#         if total_records > 0:
#             combined_df.write \
#                 .format("snowflake") \
#                 .options(**sfOptions) \
#                 .option("dbtable", "FACT_EARTHQUAKE_PORT_RISK") \
#                 .option("columnList", "EARTHQUAKE_KEY,PORT_KEY,EARTHQUAKE_DATE_KEY,DISTANCE_KM,RISK_RADIUS_KM,IS_NEARBY,RISK_LEVEL,RISK_REASON,EVENT_YEAR,EVENT_MONTH") \
#                 .mode("append") \
#                 .save()
            
#             logger.info(f"[BATCH {batch_id}] Successfully written {total_records} records!")
#             accumulated_batches = []
#             last_write_time = current_time
#         else:
#             logger.info(f"[BATCH {batch_id}] No records to write, clearing accumulated data")
#             accumulated_batches = []
#             last_write_time = current_time
    
#     logger.info(f"[BATCH {batch_id}] completed")



# ============================================================================
# Read Stream and Start Processing
# ============================================================================

raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "1000") \
    .load()

parsed_df = raw_kafka_df.select(
    F.from_json(F.col("value").cast("string"), earthquake_schema).alias("data")
).select("data.*")

query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime='30 seconds') \
    .start()

logger.info("\n" + "=" * 60)
logger.info("Streaming started! Earthquake-Port Risk Analysis is running...")
logger.info("=" * 60 + "\n")

print("\n" + "=" * 60)
print("Streaming started! Earthquake-Port Risk Analysis is running...")
print("=" * 60 + "\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Stream stopped by user.")
    print("\n[INFO] Stopping stream...")
    
    # Write remaining accumulated data
    if accumulated_batches:
        logger.info("Writing remaining accumulated data...")
        try:
            combined_df = accumulated_batches[0]
            for df in accumulated_batches[1:]:
                combined_df = combined_df.union(df)
            
            combined_df.write \
                .format("snowflake") \
                .options(**sfOptions) \
                .option("dbtable", "FACT_EARTHQUAKE_PORT_RISK") \
                .mode("append") \
                .save()
            
            logger.info("Final data written successfully")
        except Exception as e:
            logger.exception(f"Error writing final data: {e}")
    
    query.stop()
    spark.stop()
    logger.info("Spark session stopped")
    print("[INFO] Stream stopped.")