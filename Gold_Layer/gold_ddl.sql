CREATE DATABASE IF NOT EXISTS gold;

USE gold;


-- =========================================================
-- 1. DIMENSION: DATE
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INT,
    full_date DATE,

    day INT,
    month INT,
    month_name STRING,
    quarter INT,
    year INT,

    week_of_year INT,
    day_of_week INT,
    day_name STRING,

    is_weekend BOOLEAN
)
STORED AS PARQUET;


-- =========================================================
-- 2. DIMENSION: TIME
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.dim_time (
    time_key INT,

    hour INT,
    minute INT,
    second INT,

    hour_minute STRING
)
STORED AS PARQUET;


-- =========================================================
-- 3. DIMENSION: COUNTRY
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.dim_country (
    country_key BIGINT,
    country_code STRING,
    country_name STRING
)
STORED AS PARQUET;


-- =========================================================
-- 4. DIMENSION: PORT
-- SCD TYPE 2
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.dim_port (
    port_key BIGINT,

    oid INT,
    world_port_index_number INT,

    main_port_name STRING,
    alternate_port_name STRING,
    un_locode STRING,

    country_code STRING,
    region_name STRING,
    world_water_body STRING,
    iho_s130_sea_area STRING,

    latitude DOUBLE,
    longitude DOUBLE,

    harbor_size STRING,
    harbor_type STRING,
    harbor_use STRING,
    shelter_afforded STRING,

    channel_depth_m DOUBLE,
    anchorage_depth_m DOUBLE,
    cargo_pier_depth_m DOUBLE,
    oil_terminal_depth_m DOUBLE,
    lng_terminal_depth_m DOUBLE,

    maximum_vessel_length_m DOUBLE,
    maximum_vessel_beam_m DOUBLE,
    maximum_vessel_draft_m DOUBLE,

    port_security STRING,

    -- Supplies
    supplies_provisions STRING,
    supplies_potable_water STRING,
    supplies_fuel_oil STRING,
    supplies_diesel_oil STRING,
    supplies_aviation_fuel STRING,
    supplies_deck STRING,
    supplies_engine STRING,

    -- Communications
    communications_telephone STRING,
    communications_telefax STRING,
    communications_radio STRING,
    communications_radiotelephone STRING,
    communications_airport STRING,
    communications_rail STRING,

    -- Derived business attributes
    supplies_available_count INT,
    supplies_rate_classification STRING,

    communications_available_count INT,
    communications_rate_classification STRING,

    -- SCD Type 2
    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN
)
STORED AS PARQUET;


-- =========================================================
-- 5. DIMENSION: VESSEL
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.dim_vessel (
    vessel_key BIGINT,

    vessel_name STRING,
    vessel_type STRING,

    year_built INT,

    gross_tonnage BIGINT,
    deadweight BIGINT,

    length_m DOUBLE,
    beam_m DOUBLE,

    detail_link STRING
)
STORED AS PARQUET;


-- =========================================================
-- 6. FACT: EARTHQUAKE
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.fact_earthquake (
    earthquake_key BIGINT,

    unid STRING,
    source_id STRING,

    event_date_key INT,
    event_time_key INT,

    event_timestamp TIMESTAMP,

    flynn_region STRING,

    latitude DOUBLE,
    longitude DOUBLE,

    depth DOUBLE,
    magnitude DOUBLE,
    magnitude_type STRING,

    event_type STRING,
    source_catalog STRING,
    authority STRING,

    received_at TIMESTAMP
)
PARTITIONED BY (
    event_year INT,
    event_month INT
)
STORED AS PARQUET;


-- =========================================================
-- 7. FACT: EARTHQUAKE - PORT RISK
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.fact_earthquake_port_risk (
    earthquake_port_risk_key BIGINT,

    earthquake_key BIGINT,
    port_key BIGINT,

    earthquake_date_key INT,

    distance_km DOUBLE,
    risk_radius_km DOUBLE,

    is_nearby BOOLEAN,

    risk_level STRING,
    risk_reason STRING
)
PARTITIONED BY (
    event_year INT,
    event_month INT
)
STORED AS PARQUET;


-- =========================================================
-- 8. FACT: VESSEL SNAPSHOT
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.fact_vessel_snapshot (
    vessel_snapshot_key BIGINT,

    vessel_key BIGINT,

    snapshot_date_key INT,
    snapshot_time_key INT,

    last_port_key BIGINT,
    destination_port_key BIGINT,

    departure_date_key INT,
    arrival_date_key INT,

    reported_status STRING,

    days_to_arrival INT,

    snapshot_timestamp TIMESTAMP
)
PARTITIONED BY (
    snapshot_year INT,
    snapshot_month INT
)
STORED AS PARQUET;


-- =========================================================
-- 9. FACT: VESSEL TRIP
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.fact_vessel_trip (
    trip_key BIGINT,

    vessel_key BIGINT,

    departure_port_key BIGINT,
    destination_port_key BIGINT,

    departure_date_key INT,
    arrival_date_key INT,

    scheduled_arrival_timestamp TIMESTAMP,
    actual_arrival_timestamp TIMESTAMP,

    trip_duration_hours DOUBLE,
    arrival_delay_hours DOUBLE,

    destination_country_key BIGINT,

    trip_status STRING
)
PARTITIONED BY (
    departure_year INT,
    departure_month INT
)
STORED AS PARQUET;


-- =========================================================
-- 10. FACT: VESSEL - PORT RISK
-- =========================================================

CREATE TABLE IF NOT EXISTS gold.fact_vessel_port_risk (
    vessel_risk_key BIGINT,

    vessel_key BIGINT,
    port_key BIGINT,
    earthquake_key BIGINT,

    snapshot_date_key INT,

    distance_km DOUBLE,

    risk_level STRING,

    is_heading_to_dangerous_port BOOLEAN,

    notification_required BOOLEAN
)
PARTITIONED BY (
    snapshot_year INT,
    snapshot_month INT
)
STORED AS PARQUET;