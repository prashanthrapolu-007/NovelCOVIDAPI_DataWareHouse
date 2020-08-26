class SqlQueries:
    create_dim_and_fact_tables = ("""    
    -- Drop tables if required by uncommenting the below two lines
    -- DROP TABLE IF EXISTS public.corona_data_api;
    -- DROP TABLE IF EXISTS public.country_continent_map;
    
    
    -- creating dimension table dim_country
    CREATE TABLE IF NOT EXISTS public.dim_country(
    COUNTRY_CODE VARCHAR(10),
    REGION_CODE VARCHAR(10),
    SUB_REGION_CODE VARCHAR(10),
    NAME VARCHAR(50),
    ALPHA_2 VARCHAR(10),
    ALPHA_3 VARCHAR(10),
    ISO_3166_2 VARCHAR(20)    
    );
    
    --creating dimension table dim_region
    CREATE TABLE IF NOT EXISTS public.dim_region(
    REGION_CODE VARCHAR(10),
    REGION VARCHAR(50)
    );
    
    --creating dimension table dim_sub_region
    CREATE TABLE IF NOT EXISTS public.dim_sub_region(
    SUB_REGION_CODE VARCHAR(10),
    REGION_CODE VARCHAR(10),
    SUB_REGION VARCHAR(50)
    );    
     
    -- creating country_continent_map table
    CREATE TABLE IF NOT EXISTS public.country_continent_map(
    NAME VARCHAR(50),
    ALPHA_2 VARCHAR(10),
    ALPHA_3 VARCHAR(10),
    COUNTRY_CODE VARCHAR(10),
    ISO_3166_2 VARCHAR(20),
    REGION VARCHAR(50),
    SUB_REGION VARCHAR(50),
    INTERMEDIATE_REGION VARCHAR(50),
    REGION_CODE VARCHAR(10),
    SUB_REGION_CODE VARCHAR(10),
    INTERMEDIATE_REGION_CODE VARCHAR(10) 
    );    
    
    
    -- creating fact table store corona data from api
    CREATE TABLE IF NOT EXISTS public.fact_corona_data_api(
    COUNTRY_CODE VARCHAR(10),
    REGION_CODE VARCHAR(10),
    SUB_REGION_CODE VARCHAR(10),
    RECORD_DATE DATE,
    CASES INT,
    DEATHS INT,
    RECOVERED INT);
            
    """)

    fetch_country_names = ("""
    SELECT name from public.country_continent_map;
    """)
