CREATE DATABASE IF NOT EXISTS gold;

USE gold;

CREATE EXTERNAL TABLE IF NOT EXISTS gold.ports (
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

    harbor_size STRING,
    harbor_type STRING,
    harbor_use STRING,
    shelter_afforded STRING,

    latitude DOUBLE,
    longitude DOUBLE,

    tidal_range_m DOUBLE,
    entrance_width_m DOUBLE,
    channel_depth_m DOUBLE,
    anchorage_depth_m DOUBLE,
    cargo_pier_depth_m DOUBLE,
    maximum_vessel_length_m DOUBLE,
    maximum_vessel_beam_m DOUBLE,
    maximum_vessel_draft_m DOUBLE,

    supplies_provisions STRING,
    supplies_fuel_oil STRING,
    supplies_diesel_oil STRING,
    supplies_potable_water STRING,
    repairs STRING,
    supplies_count INT,
    supplies_rate STRING,

    communications_radio STRING,
    communications_telephone STRING,
    communications_airport STRING,
    communications_telefax STRING,
    communications_count INT,
    comm_rate STRING,

    snapshot_year INT,
    snapshot_month INT
)
STORED AS PARQUET
LOCATION '/gold_layer/ports';