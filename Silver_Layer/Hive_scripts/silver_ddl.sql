CREATE DATABASE IF NOT EXISTS silver;

USE silver;

CREATE EXTERNAL TABLE IF NOT EXISTS silver.ports (
    oid INT,
    world_port_index_number INT,
    region_name STRING,
    main_port_name STRING,
    alternate_port_name STRING,
    un_locode STRING,
    country_code STRING,
    world_water_body STRING,
    iho_s130_sea_area STRING,

    sailing_direction_or_publication STRING,
    publication_link STRING,
    standard_nautical_chart STRING,
    iho_s57_electronic_navigational_chart STRING,
    iho_s101_electronic_navigational_chart STRING,
    digital_nautical_chart STRING,

    tidal_range_m DOUBLE,
    entrance_width_m DOUBLE,
    channel_depth_m DOUBLE,
    anchorage_depth_m DOUBLE,
    cargo_pier_depth_m DOUBLE,
    oil_terminal_depth_m DOUBLE,
    liquified_natural_gas_terminal_depth_m DOUBLE,

    maximum_vessel_length_m DOUBLE,
    maximum_vessel_beam_m DOUBLE,
    maximum_vessel_draft_m DOUBLE,

    offshore_maximum_vessel_length_m DOUBLE,
    offshore_maximum_vessel_beam_m DOUBLE,
    offshore_maximum_vessel_draft_m DOUBLE,

    harbor_size STRING,
    harbor_type STRING,
    harbor_use STRING,
    shelter_afforded STRING,

    entrance_restriction_tide STRING,
    entrance_restriction_heavy_swell STRING,
    entrance_restriction_ice STRING,
    entrance_restriction_other STRING,

    overhead_limits STRING,
    underkeel_clearance_management_system STRING,
    good_holding_ground STRING,
    turning_area STRING,
    port_security STRING,

    estimated_time_of_arrival_message STRING,

    quarantine_pratique STRING,
    quarantine_sanitation STRING,
    quarantine_other STRING,

    traffic_separation_scheme STRING,
    vessel_traffic_service STRING,
    first_port_of_entry STRING,
    us_representative STRING,

    pilotage_compulsory STRING,
    pilotage_available STRING,
    pilotage_local_assistance STRING,
    pilotage_advisable STRING,

    tugs_salvage STRING,
    tugs_assistance STRING,

    communications_telephone STRING,
    communications_telefax STRING,
    communications_radio STRING,
    communications_radiotelephone STRING,
    communications_airport STRING,
    communications_rail STRING,

    search_and_rescue STRING,
    navarea STRING,

    facilities_wharves STRING,
    facilities_anchorage STRING,
    facilities_dangerous_cargo_anchorage STRING,
    facilities_med_mooring STRING,
    facilities_beach_mooring STRING,
    facilities_ice_mooring STRING,
    facilities_ro_ro STRING,
    facilities_solid_bulk STRING,
    facilities_liquid_bulk STRING,
    facilities_container STRING,
    facilities_breakbulk STRING,
    facilities_oil_terminal STRING,
    facilities_lng_terminal STRING,
    facilities_other STRING,

    medical_facilities STRING,
    garbage_disposal STRING,
    chemical_holding_tank_disposal STRING,
    degaussing STRING,
    dirty_ballast_disposal STRING,

    cranes_fixed STRING,
    cranes_mobile STRING,
    cranes_floating STRING,
    cranes_container STRING,

    lifts_100_plus_tons STRING,
    lifts_50_100_tons STRING,
    lifts_25_49_tons STRING,
    lifts_0_24_tons STRING,

    services_longshoremen STRING,
    services_electricity STRING,
    services_steam STRING,
    services_navigation_equipment STRING,
    services_electrical_repair STRING,
    services_ice_breaking STRING,
    services_diving STRING,

    supplies_provisions STRING,
    supplies_potable_water STRING,
    supplies_fuel_oil STRING,
    supplies_diesel_oil STRING,
    supplies_aviation_fuel STRING,
    supplies_deck STRING,
    supplies_engine STRING,

    repairs STRING,
    dry_dock STRING,
    railway STRING,

    latitude DOUBLE,
    longitude DOUBLE
)
PARTITIONED BY (
    snapshot_year INT,
    snapshot_month INT
)
STORED AS PARQUET
LOCATION '/silver_layer/ports';

-- Register existing partitions in Hive Metastore
MSCK REPAIR TABLE silver.ports;