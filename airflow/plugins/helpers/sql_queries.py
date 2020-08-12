class SqlQueries:
    create_staging_tables = ("""
    DROP TABLE IF EXISTS staging.country_continent_map;
    DROP TABLE IF EXISTS staging.corona_data_api;
    
    -- Create stage table for base mapping table
    CREATE TABLE IF NOT EXISTS staging.country_continent_map(
    "name" varchar, 
    "alpha_2" varchar, 
    "alpha_3" varchar, 
    "country_code" varchar, 
    "iso_3166_2" varchar, 
    "region" varchar,
    "sub_region" varchar, 
    "intermediate_region" varchar, 
    "region_code" varchar, 
    "sub_region_code" varchar,
    "intermediate_region_code" varchar);
    
    -- Create stage table for source data        
    CREATE TABLE IF NOT EXISTS staging.corona_data_api(
    COUNTRY varchar(40),
    RECORD_DATE DATE,
    CASES INT,
    DEATHS INT,
    RECOVERED INT);
    """)
