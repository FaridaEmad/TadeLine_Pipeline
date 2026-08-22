import sys 
import os
import yaml

# Project Base Directory
BASE_DIR= os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(BASE_DIR)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, lower, row_number, max as spark_max
from pyspark.sql.window import Window
from utilities.logger import configure_logger



# Logger
logger = configure_logger(
    log_name="PortsGold",
    log_file="gold_ports_etl",
    log_dir="/gold_scripts/logs"
)

# paths 
SILVER_PATH = "/silver_layer/ports"
GOLD_PATH = "/gold_layer/ports"

CONFIG_PATH = os.path.join(
    BASE_DIR,
    "config",
    "gold_schema.yaml"
)

# Spark Session
spark = (
    SparkSession.builder
    .appName("Ports_Gold_ETL")
    .enableHiveSupport()
    .getOrCreate()
)

# Helper Functions

def load_config(config_path):
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)

    with open(config_path, "r") as config_file:
        config = yaml.safe_load(config_file)

    ports_config = config["sources"]["ports"]
    logger.info(
        "Ports Gold configuration loaded successfully."
    )
    return ports_config

def select_latest_snapshot(df):
    latest_snapshot = (
    df.select("snapshot_year", "snapshot_month").distinct().orderBy(col("snapshot_year").desc(), col("snapshot_month").desc()).first())
    if latest_snapshot is None:
        logger.error("No snapshots found in the DataFrame.")
        sys.exit(1)
    latest_year = latest_snapshot["snapshot_year"]
    latest_month = latest_snapshot["snapshot_month"]
    logger.info(
        f"Latest snapshot: "
        f"{latest_year}-{latest_month:02d}"
    )
    df = df.filter(
        (col("snapshot_year") == latest_year)
        &
        (col("snapshot_month") == latest_month)
    )
    return df

def availability_to_binary(column_name):
    normalized_value = lower(
        trim(
            col(column_name).cast("string")
        )
    )
    return (
        when(
            normalized_value.isin(
                "1",
                "y",
                "yes",
                "true",
                "available"
            ),
            lit(1)
        )
        .when(
            normalized_value.isin(
                "0",
                "n",
                "no",
                "false",
                "unavailable"
            ),
            lit(0)
        )
        .otherwise(
            lit(0)
        )
    )
def add_supplies_metrics(df):
       supplies_columns = [
        "supplies_provisions",
        "supplies_fuel_oil",
        "supplies_diesel_oil",
        "supplies_potable_water",
        "repairs"
    ]
       logger.info(
        "Calculating supplies metrics."
    )
       availability_expressions = [
        availability_to_binary(column_name)
        for column_name in supplies_columns
    ]
       supplies_count_expression = (
        availability_expressions[0]
        + availability_expressions[1]
        + availability_expressions[2]
        + availability_expressions[3]
        + availability_expressions[4]
    )
       df = df.withColumn(
        "supplies_count",
        supplies_count_expression
    )
       df = df.withColumn(
        "supplies_rate",
        when(
            col("supplies_count") == 5,
            "Excellent"
        )
        .when(
            col("supplies_count").between(3, 4),
            "Good"
        )
        .when(
            col("supplies_count").between(1, 2),
            "Limited"
        )
        .otherwise(
            "Unavailable"
        )
    )

       logger.info(
        "Supplies metrics calculated successfully."
    )

       return df
def add_communication_metrics(df):
    communication_columns = [
        "communications_radio",
        "communications_telephone",
        "communications_airport",
        "communications_telefax"
    ]
    logger.info(
        "Calculating communication metrics."
    )
    availability_expressions = [
        availability_to_binary(column_name)
        for column_name in communication_columns
    ]
    communications_count_expression = (
        availability_expressions[0]
        + availability_expressions[1]
        + availability_expressions[2]
        + availability_expressions[3]
    )

    df = df.withColumn(
        "communications_count",
        communications_count_expression
    )

    df = df.withColumn(
        "comm_rate",
        when(
            col("communications_count") == 4,
            "Excellent"
        )
        .when(
            col("communications_count") == 3,
            "Good"
        )
        .when(
            col("communications_count").between(1, 2),
            "Limited"
        )
        .otherwise(
            "Unavailable"
        )
    )

    logger.info(
        "Communication metrics calculated successfully."
    )

    return df

def validate_oid(df):
    null_count = (
        df
        .filter(col("oid").isNull())
        .count()
    )

    if null_count > 0:
        logger.error(
            f"Validation failed: {null_count} rows have null 'oid' values."
        )
        sys.exit(1)
    return df

def deduplicate_current_ports(df):
    before_count = df.count()

    df = df.dropDuplicates(
        ["oid"]
    )

    after_count = df.count()

    logger.info(
        f"Current Ports deduplication completed. "
        f"Removed {before_count - after_count} duplicates."
    )

    return df


# Surrogate Key

def assign_surrogate_keys(df):

    logger.info(
        "Starting surrogate key assignment."
    )

    gold_exists = False
    existing_keys = None

    # ========================================================
    # Read Existing Gold Keys
    # ========================================================

    try:

        existing_keys_raw = (
            spark.read
            .parquet(GOLD_PATH)
            .select(
                "oid",
                "port_key"
            )
            .dropDuplicates(["oid"])
        )

        existing_keys_count = existing_keys_raw.count()

        if existing_keys_count > 0:

            gold_exists = True

            logger.info(
                f"Existing Gold dimension found. "
                f"Existing keys: {existing_keys_count}"
            )

            # Materialize the mapping independently
            existing_keys_data = existing_keys_raw.collect()

            existing_keys = spark.createDataFrame(
                existing_keys_data,
                schema=existing_keys_raw.schema
            )

        else:

            logger.info(
                "Gold dimension exists but contains no records. "
                "Treating as initial load."
            )

    except Exception as e:

        logger.info(
            f"No existing Gold dimension found. "
            f"Treating as initial load. Reason: {str(e)}"
        )

    # ========================================================
    # Initial Load
    # ========================================================

    if not gold_exists:

        logger.info(
            "Assigning initial surrogate keys."
        )

        window_spec = Window.orderBy(
            col("oid")
        )

        df = df.withColumn(
            "port_key",
            row_number()
            .over(window_spec)
            .cast("long")
        )

        logger.info(
            "Initial surrogate keys assigned successfully."
        )

        return df

    # ========================================================
    # Match Existing Ports With Existing Surrogate Keys
    # ========================================================

    df_with_keys = (
        df.alias("current")
        .join(
            existing_keys.alias("existing"),
            col("current.oid")
            == col("existing.oid"),
            "left"
        )
        .select(
            col("current.*"),
            col("existing.port_key")
            .alias("existing_port_key")
        )
    )

    # ========================================================
    # Find Maximum Existing Surrogate Key
    # ========================================================

    max_key_row = (
        existing_keys
        .agg(
            spark_max("port_key")
            .alias("max_port_key")
        )
        .first()
    )

    max_port_key = (
        max_key_row["max_port_key"]
        if max_key_row["max_port_key"] is not None
        else 0
    )

    logger.info(
        f"Current maximum port_key: {max_port_key}"
    )

    # ========================================================
    # Existing Ports
    # ========================================================

    existing_ports = (
        df_with_keys
        .filter(
            col("existing_port_key").isNotNull()
        )
        .withColumn(
            "port_key",
            col("existing_port_key").cast("long")
        )
        .drop(
            "existing_port_key"
        )
    )

    # ========================================================
    # New Ports
    # ========================================================

    new_ports = (
        df_with_keys
        .filter(
            col("existing_port_key").isNull()
        )
    )

    new_ports_count = new_ports.count()

    logger.info(
        f"New Ports requiring surrogate keys: "
        f"{new_ports_count}"
    )

    if new_ports_count > 0:

        window_spec = Window.orderBy(
            col("oid")
        )

        new_ports = (
            new_ports
            .withColumn(
                "port_key",
                (
                    row_number()
                    .over(window_spec)
                    + lit(max_port_key)
                )
                .cast("long")
            )
            .drop(
                "existing_port_key"
            )
        )

    else:

        new_ports = (
            new_ports
            .withColumn(
                "port_key",
                lit(None).cast("long")
            )
            .drop(
                "existing_port_key"
            )
        )

    # ========================================================
    # Combine Existing + New Ports
    # ========================================================

    final_df = existing_ports.unionByName(
        new_ports
    )

    logger.info(
        "Surrogate keys assigned successfully."
    )

    return final_df

# Select Gold Columns
def select_gold_columns(df, gold_columns):
    logger.info(
        "Selecting Gold columns."
    )
    missing_columns = [
        column_name
        for column_name in gold_columns
        if column_name not in df.columns
    ]
    if missing_columns:
        logger.error(
            f"Missing columns in DataFrame: "
            f"{', '.join(missing_columns)}"
        )
        sys.exit(1)
    df = df.select(
        *gold_columns
    )

    logger.info(
        "Gold columns selected successfully."
    )

    return df

# Main ETL

def main():

    logger.info(
        "========== Ports Gold ETL Started =========="
    )
    try:

        # 1. Load configuration
        ports_config = load_config(CONFIG_PATH)
        gold_columns = ports_config[
            "gold_columns"
        ]

        # 2. Read Silver
      
        logger.info(
            f"Reading Silver data from: "
            f"{SILVER_PATH}"
        )

        df = (
            spark.read
            .parquet(SILVER_PATH)
        )

        silver_count = df.count()

        logger.info(
            f"Silver data loaded successfully. "
            f"Records: {silver_count}"
        )
        # 3. Select latest snapshot
        df = select_latest_snapshot(
            df
        )

        current_snapshot_count = df.count()

        logger.info(
            f"Latest snapshot records: "
            f"{current_snapshot_count}"
        )

        # 4. Validate business key
        df = validate_oid(df)

        # 5. Deduplicate current Ports
        df = deduplicate_current_ports(df)


        # 6. Add Supplies metrics
        df = add_supplies_metrics(df)

        # 7. Add Communication metrics
        df = add_communication_metrics(df)

        # 8. Assign surrogate keys
        df = assign_surrogate_keys(df)

        # 9. Select final Gold columns
        df = select_gold_columns(
            df,
            gold_columns
        )

        # 10. Final record count
        final_count = df.count()
        logger.info(
            f"Final Gold records: {final_count}"
        )
        # 11. Write Current Port Dimension
        logger.info(
            f"Writing Gold data to: "
            f"{GOLD_PATH}"
        )
        df.write.mode("overwrite").parquet(
            GOLD_PATH)
        logger.info(
            "Gold data written successfully."
        )
        logger.info(
            "========== Ports Gold ETL Completed =========="
        )
    except Exception as e:
        logger.error(
            f"Ports Gold ETL failed: {str(e)}"
        )
        sys.exit(1)
# Entry point
if __name__ == "__main__":
    try:
        main()

    finally:
        spark.stop()

        logger.info(
            "Spark session stopped."
        )