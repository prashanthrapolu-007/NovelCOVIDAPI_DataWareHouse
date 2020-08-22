class SqlQueries:
    create_staging_tables = ("""    
    -- Drop tables if they exist already
    -- DROP TABLE IF EXISTS public.corona_data_api;
    
    -- creating country_continent_map table
    CREATE TABLE IF NOT EXISTS public.country_continent_map(
    NAME VARCHAR(50),
    ALPHA_2 VARCHAR(10),
    ALPHA_3 VARCHAR(10),
    COUNTRY_CODE VARCHAR(10),
    ISO_3166_2 VARCHAR(10),
    REGION VARCHAR(50),
    SUB_REGION VARCHAR(50),
    INTERMEDIATE_REGION VARCHAR(50),
    REGION_CODE VARCHAR(10),
    SUB_REGION_CODE VARCHAR(10),
    INTERMEDIATE_REGION_CODE VARCHAR(10) 
    );    
    
    -- creating table to store corona data from api
    CREATE TABLE IF NOT EXISTS public.corona_data_api(
    COUNTRY VARCHAR(40),
    RECORD_DATE DATE,
    CASES INT,
    DEATHS INT,
    RECOVERED INT);
            
    """)
