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

    fetch_country_region_subregion_codes = ("""
    SELECT country_code, region_code, sub_region_code, name from public.dim_country;
    """)

    visualize_last_7_days_country_data = \
        ("""
            select
            b.name, a.new_cases_in_last_7_days, a.new_recoveries_in_last_7_days, a.new_deaths_in_last_7_days
            from
        (select
            country_code,
            record_date,
            cases - lag(cases,7) over (partition by country_code order by record_date) as new_cases_in_last_7_days,
            recovered - lag(recovered,7) over (partition by country_code order by record_date) as new_recoveries_in_last_7_days,
            deaths - lag(deaths,7) over (partition by country_code order by record_date) as new_deaths_in_last_7_days, 
            cases,
            deaths,
            recovered
        from 
            public.fact_corona_data_api
        where country_code in (select
                                    country_code
                                from
                                    public.fact_corona_data_api
                                where
                                    record_date = (select max(record_date) from public.fact_corona_data_api)
                                order by 
                                    cases desc limit 10)) a
        inner join
            public.dim_country b
        on
            a.country_code = b.country_code
        where 
            a.record_date = (select max(record_date) from public.fact_corona_data_api)
        order by 
            new_cases_in_last_7_days;""")

    visualize_last_15_days_country_data = ("""
    select
            b.name, a.new_cases_in_last_15_days, a.new_recoveries_in_last_15_days, a.new_deaths_in_last_15_days
            from
        (	
        select
            country_code,
            record_date,
            cases - lag(cases,14) over (partition by country_code order by record_date) as new_cases_in_last_15_days,
            recovered - lag(recovered,14) over (partition by country_code order by record_date) as new_recoveries_in_last_15_days,
            deaths - lag(deaths,14) over (partition by country_code order by record_date) as new_deaths_in_last_15_days
        from 
            public.fact_corona_data_api
        where country_code in (select
                                    country_code
                                from
                                    public.fact_corona_data_api
                                where
                                    record_date = (select max(record_date) from public.fact_corona_data_api)
                                order by 
                                    cases desc limit 10)) a
        inner join
            public.dim_country b
        on
            a.country_code = b.country_code
        where 
            a.record_date = (select max(record_date) from public.fact_corona_data_api)
        order by 
            new_cases_in_last_15_days;
            """)

    visualize_last_30_days_country_data = ("""
    select
        b.name, a.new_cases_in_last_30_days, a.new_recoveries_in_last_30_days, a.new_deaths_in_last_30_days
        from
        (	
        select
            country_code,
            record_date,
            cases - lag(cases,30) over (partition by country_code order by record_date) as new_cases_in_last_30_days,
            recovered - lag(recovered,30) over (partition by country_code order by record_date) as new_recoveries_in_last_30_days,
            deaths - lag(deaths,30) over (partition by country_code order by record_date) as new_deaths_in_last_30_days
        from 
            public.fact_corona_data_api
        where country_code in (select
                                    country_code
                                from
                                    public.fact_corona_data_api
                                where
                                    record_date = (select max(record_date) from public.fact_corona_data_api)
                                order by 
                                    cases desc limit 10)) a
        inner join
            public.dim_country b
        on
            a.country_code = b.country_code
        where 
            a.record_date = (select max(record_date) from public.fact_corona_data_api)
        order by 
            new_cases_in_last_30_days;	
        """)

    visualize_last_7_days_region_data = ("""
        with temp_table as 
        (select 
            a.region_code,
            a.record_date,
            a.cases - lag(a.cases,7) over (partition by a.region_code order by a.record_date) as last_7_day_cases,
            a.recovered - lag(a.recovered,7) over (partition by a.region_code order by a.record_date) as last_7_day_recovered,
            a.deaths - lag(a.deaths,7) over (partition by a.region_code order by a.record_date) as last_7_day_deaths
        from
         (select
            region_code,
            record_date,
            sum(cases) as cases,
            sum(deaths) as deaths,
            sum(recovered) as recovered
        from 
            public.fact_corona_data_api
        group by
            region_code, record_date
        order by
            region_code, record_date desc)a)
            
        select 
            dim_region.region,
            c.last_7_day_cases,
            c.last_7_day_recovered,
            c.last_7_day_deaths
        from 
            temp_table c
        inner join
            dim_region
        on c.region_code = dim_region.region_code	
        where
            c.record_date = (select max(record_date) from temp_table);
         """)

    visualize_last_15_days_region_data = ("""
    with temp_table as 
    (select 
        a.region_code,
        a.record_date,
        a.cases - lag(a.cases,15) over (partition by a.region_code order by a.record_date) as last_15_day_cases,
        a.recovered - lag(a.recovered,15) over (partition by a.region_code order by a.record_date) as last_15_day_recovered,
        a.deaths - lag(a.deaths,15) over (partition by a.region_code order by a.record_date) as last_15_day_deaths
    from
     (select
        region_code,
        record_date,
        sum(cases) as cases,
        sum(deaths) as deaths,
        sum(recovered) as recovered
    from 
        public.fact_corona_data_api
    group by
        region_code, record_date
    order by
        region_code, record_date desc)a)
        
    select 
        dim_region.region,
        c.last_15_day_cases,
        c.last_15_day_recovered,
        c.last_15_day_deaths
    from 
        temp_table c
    inner join
        dim_region
    on c.region_code = dim_region.region_code	
    where
        c.record_date = (select max(record_date) from temp_table);
        """)

    visualize_last_30_days_region_data = ("""
    with temp_table as 
    (select 
        a.region_code,
        a.record_date,
        a.cases - lag(a.cases,30) over (partition by a.region_code order by a.record_date) as last_30_day_cases,
        a.recovered - lag(a.recovered,30) over (partition by a.region_code order by a.record_date) as last_30_day_recovered,
        a.deaths - lag(a.deaths,30) over (partition by a.region_code order by a.record_date) as last_30_day_deaths
    from
     (select
        region_code,
        record_date,
        sum(cases) as cases,
        sum(deaths) as deaths,
        sum(recovered) as recovered
    from 
        public.fact_corona_data_api
    group by
        region_code, record_date
    order by
        region_code, record_date desc)a)
        
    select 
        dim_region.region,
        c.last_30_day_cases,
        c.last_30_day_recovered,
        c.last_30_day_deaths
    from 
        temp_table c
    inner join
        dim_region
    on c.region_code = dim_region.region_code	
    where
        c.record_date = (select max(record_date) from temp_table);
        """)

    visualize_last_7_days_sub_region_data = ("""
    with temp_table as 
    (select 
        a.sub_region_code,
        a.record_date,
        a.cases - lag(a.cases,7) over (partition by a.sub_region_code order by a.record_date) as last_7_day_cases,
        a.recovered - lag(a.recovered,7) over (partition by a.sub_region_code order by a.record_date) as last_7_day_recovered,
        a.deaths - lag(a.deaths,7) over (partition by a.sub_region_code order by a.record_date) as last_7_day_deaths
    from
     (select
        sub_region_code,
        record_date,
        sum(cases) as cases,
        sum(deaths) as deaths,
        sum(recovered) as recovered
    from 
        public.fact_corona_data_api
    group by
        sub_region_code, record_date
    order by
        sub_region_code, record_date desc)a)
        
    select 
        dim_sub_region.sub_region,
        c.last_7_day_cases,
        c.last_7_day_recovered,
        c.last_7_day_deaths
    from 
        temp_table c
    inner join
        dim_sub_region
    on c.sub_region_code = dim_sub_region.sub_region_code	
    where
        c.record_date = (select max(record_date) from temp_table)
    order by last_7_day_cases
    limit 10;
        """)

    visualize_last_15_days_sub_region_data = ("""
    with temp_table as 
    (select 
        a.sub_region_code,
        a.record_date,
        a.cases - lag(a.cases,15) over (partition by a.sub_region_code order by a.record_date) as last_15_day_cases,
        a.recovered - lag(a.recovered,15) over (partition by a.sub_region_code order by a.record_date) as last_15_day_recovered,
        a.deaths - lag(a.deaths,15) over (partition by a.sub_region_code order by a.record_date) as last_15_day_deaths
    from
     (select
        sub_region_code,
        record_date,
        sum(cases) as cases,
        sum(deaths) as deaths,
        sum(recovered) as recovered
    from 
        public.fact_corona_data_api
    group by
        sub_region_code, record_date
    order by
        sub_region_code, record_date desc)a)
        
    select 
        dim_sub_region.sub_region,
        c.last_15_day_cases,
        c.last_15_day_recovered,
        c.last_15_day_deaths
    from 
        temp_table c
    inner join
        dim_sub_region
    on c.sub_region_code = dim_sub_region.sub_region_code	
    where
        c.record_date = (select max(record_date) from temp_table)
    order by last_15_day_cases
    limit 10;
        """)

    visualize_last_30_days_sub_region_data = ("""
    with temp_table as 
    (select 
        a.sub_region_code,
        a.record_date,
        a.cases - lag(a.cases,30) over (partition by a.sub_region_code order by a.record_date) as last_30_day_cases,
        a.recovered - lag(a.recovered,30) over (partition by a.sub_region_code order by a.record_date) as last_30_day_recovered,
        a.deaths - lag(a.deaths,30) over (partition by a.sub_region_code order by a.record_date) as last_30_day_deaths
    from
     (select
        sub_region_code,
        record_date,
        sum(cases) as cases,
        sum(deaths) as deaths,
        sum(recovered) as recovered
    from 
        public.fact_corona_data_api
    group by
        sub_region_code, record_date
    order by
        sub_region_code, record_date desc)a)
        
    select 
        dim_sub_region.sub_region,
        c.last_30_day_cases,
        c.last_30_day_recovered,
        c.last_30_day_deaths
    from 
        temp_table c
    inner join
        dim_sub_region
    on c.sub_region_code = dim_sub_region.sub_region_code	
    where
        c.record_date = (select max(record_date) from temp_table)
    order by last_30_day_cases
    limit 10;
        """)
