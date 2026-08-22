import re
import sys
import os
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when


# Project Base Directory

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

sys.path.append(BASE_DIR)

# Logger


from utilities.logger import configure_logger

logger = configure_logger(
    log_name="PortsSilver",
    log_file="silver_ports_etl",
    log_dir="/silver_scripts/logs"
)

# Paths


RAW_PATH = "hdfs:///raw_layer/ports/"
SILVER_PATH = "hdfs:///silver_layer/ports"

CONFIG_PATH = os.path.join(
    BASE_DIR,
    "config",
    "silver_schema.yaml"
)


# Spark Session


spark = (
    SparkSession.builder
    .appName("Ports_Silver_ETL")
    .enableHiveSupport()
    .getOrCreate()
)

# Helper Functions

import re

import re

def standardize_column_name(column_name):
    column_name = column_name.strip().lower()

    column_name = re.sub(
        r"(?<=[a-z])-(?=\d)",
        "",
        column_name
    )

    column_name = re.sub(
        r"[^a-z0-9]+",
        "_",
        column_name
    )

    column_name = column_name.strip("_")

    # Specific NGA naming corrections
    column_name = column_name.replace(
        "lifts_100_tons",
        "lifts_100_plus_tons"
    )

    return column_name

def clean_empty_strings(df):
    """
    Convert empty or whitespace-only strings to NULL.
    """

    for column_name in df.columns:

        df = df.withColumn(
            column_name,
            when(
                trim(
                    col(column_name).cast("string")
                ) == "",
                None
            ).otherwise(
                col(column_name)
            )
        )

    return df


def validate_columns(df, expected_columns):
    """
    Validate that all required columns exist.
    """

    actual_columns = set(df.columns)
    expected_columns = set(expected_columns)

    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns

    if missing_columns:
        raise ValueError(
            f"Missing required columns: "
            f"{sorted(missing_columns)}"
        )

    if extra_columns:
        logger.warning(
            f"Extra columns found: "
            f"{sorted(extra_columns)}"
        )

    logger.info(
        "Column validation completed successfully."
    )


def cast_columns(df, schema_columns):
    """
    Cast columns according to silver_schema.yaml.
    """

    type_mapping = {
        "integer": "int",
        "long": "bigint",
        "double": "double",
        "string": "string",
        "float": "float",
        "boolean": "boolean"
    }

    for column_name, column_config in schema_columns.items():

        if column_name not in df.columns:
            continue

        column_type = column_config.get("type")

        if column_type in type_mapping:

            df = df.withColumn(
                column_name,
                col(column_name).cast(
                    type_mapping[column_type]
                )
            )

    return df


def validate_not_null(df, schema_columns):
    """
    Validate columns marked as nullable: false.
    """

    non_nullable_columns = [
        column_name
        for column_name, config in schema_columns.items()
        if config.get("nullable") is False
    ]

    for column_name in non_nullable_columns:

        if column_name not in df.columns:
            continue

        null_count = (
            df.filter(
                col(column_name).isNull()
            )
            .count()
        )

        if null_count > 0:

            raise ValueError(
                f"Column '{column_name}' "
                f"contains {null_count} NULL records "
                f"but is defined as nullable: false."
            )

    logger.info(
        "NOT NULL validation completed successfully."
    )

    return df


def clean_string_columns(df, schema_columns):
    """
    Trim whitespace from all string columns.
    """

    string_columns = [
        column_name
        for column_name, config in schema_columns.items()
        if config.get("type") == "string"
        and column_name in df.columns
    ]

    for column_name in string_columns:

        df = df.withColumn(
            column_name,
            trim(col(column_name))
        )

    logger.info(
        f"Cleaned {len(string_columns)} string columns."
    )

    return df


def deduplicate_ports(df):
    """
    Deduplicate ports using OID as the business identifier.
    """

    before_count = df.count()

    df = df.dropDuplicates(["oid"])

    after_count = df.count()

    duplicates_removed = before_count - after_count

    logger.info(
        f"Deduplication completed. "
        f"Removed {duplicates_removed} duplicate records."
    )

    return df


# Main ETL

def main():

    logger.info(
        "========== Ports Silver ETL Started =========="
    )

    try:

        # 1. Validate configuration file

        if not os.path.exists(CONFIG_PATH):

            raise FileNotFoundError(
                f"Configuration file not found: "
                f"{CONFIG_PATH}"
            )

        logger.info(
            f"Loading configuration from: "
            f"{CONFIG_PATH}"
        )

        with open(
            CONFIG_PATH,
            "r"
        ) as config_file:

            config = yaml.safe_load(config_file)

        ports_config = config["sources"]["ports"]

        schema_columns = ports_config["columns"]

        required_columns = ports_config[
            "validation"
        ]["required_columns"]

        logger.info(
            "Ports schema loaded successfully."
        )

        # 2. Read Bronze

        logger.info(
            f"Reading Bronze data from: "
            f"{RAW_PATH}"
        )

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(RAW_PATH)
        )

        bronze_count = df.count()

        logger.info(
            f"Bronze data loaded successfully. "
            f"Records: {bronze_count}"
        )

        # 3. Standardize column names
        df = df.toDF(
            *[
                standardize_column_name(c)
                for c in df.columns
            ]
        )

        logger.info(
            "Column names standardized."
        )



        # 4. Rename Bronze columns
    

        # OID_ -> oid

        if "oid_" in df.columns:

            df = df.withColumnRenamed(
                "oid_",
                "oid"
            )

            logger.info(
                "Renamed oid_ to oid."
            )

    
        # 5. Validate required columns

        validate_columns(
            df,
            required_columns
        )

        # 6. Empty strings -> NULL
        

        df = clean_empty_strings(df)

        logger.info(
            "Empty strings converted to NULL."
        )

        # 7. Cast data types according to YAML

        df = cast_columns(
            df,
            schema_columns
        )

        logger.info(
            "Data types cast according to Silver schema."
        )

        # 8. Clean string columns

        df = clean_string_columns(
            df,
            schema_columns
        )

        # 9. Validate NOT NULL columns

        df = validate_not_null(
            df,
            schema_columns
        )

        # 10. Deduplicate

        df = deduplicate_ports(df)

        # 11. Create Silver partition columns

        if "year" not in df.columns:
            raise ValueError(
                "Bronze partition column 'year' "
                "was not found."
            )

        if "month" not in df.columns:
            raise ValueError(
                "Bronze partition column 'month' "
                "was not found."
            )

        df = (
            df
            .withColumn(
                "snapshot_year",
                col("year").cast("int")
            )
            .withColumn(
                "snapshot_month",
                col("month").cast("int")
            )
        )

        logger.info(
            "Snapshot partition columns added successfully."
        )


        # 12. Remove Bronze partition columns
    

        df = df.drop(
            "year",
            "month"
        )

        logger.info(
            "Bronze partition columns removed."
        )


        # 13. Reorder columns

        final_columns = (
            list(schema_columns.keys())
            + [
                "snapshot_year",
                "snapshot_month"
            ]
        )

        df = df.select(
            *final_columns
        )


        # 14. Final record count

        final_count = df.count()

        logger.info(
            f"Final Silver record count: "
            f"{final_count}"
        )


        # 15. Write Silver Parquet

        logger.info(
            f"Writing Silver data to: "
            f"{SILVER_PATH}"
        )

        (
            df.write
            .mode("overwrite")
            .partitionBy(
                "snapshot_year",
                "snapshot_month"
            )
            .format("parquet")
            .save(SILVER_PATH)
        )

        logger.info(
            "Silver Parquet written successfully."
        )

        # 16. ETL Completed

        logger.info(
            "========== Ports Silver ETL "
            "Completed Successfully =========="
        )

        print(
            "Ports Silver transformation "
            "completed successfully."
        )


    except Exception as e:

        logger.exception(
            f"Ports Silver ETL failed: {e}"
        )

        raise


# Entry Point


if __name__ == "__main__":

    try:
        main()

    finally:

        spark.stop()

        logger.info(
            "Spark session stopped."
        )