from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from operators import CreateTablesOperator, LoadFromCSVOperator, FetchDataFromDBOperator
from helpers import SqlQueries, pyhelpers

import os
from datetime import datetime, timedelta, date

default_args = {
    'owner': 'nani',
    'start_date': datetime(2019, 1, 1),
    'retry_delay': timedelta(minutes=5)
}
path_to_data_folder = '/home/nani/airflow_projects/Corona_DataWareHouse_Analytics/airflow/data/'

with DAG('setup_base_data', default_args=default_args, schedule_interval='@once') as dag:
    start_task = DummyOperator(
        task_id='dummy_start'
    )

    create_base_tables = CreateTablesOperator(
        task_id='create_dimension_and_fact_tables',
        sql_queries=SqlQueries.create_dim_and_fact_tables
    )

    get_data_for_dim_country = PythonOperator(
        task_id='get_dim_country_data',
        python_callable=pyhelpers.get_specific_columns_and_store_csv,
        op_kwargs={
            'source_file': path_to_data_folder + "country_continent_isocodes.csv",
            'destination_file': path_to_data_folder + "dim_country.csv",
            'columns': ['country-code', 'region-code', 'sub-region-code', 'name', 'alpha-2', 'alpha-3', 'iso_3166-2']
        }
    )

    get_data_for_dim_region = PythonOperator(
        task_id='get_dim_region_data',
        python_callable=pyhelpers.get_specific_columns_and_store_csv,
        op_kwargs={
            'source_file': path_to_data_folder + "country_continent_isocodes.csv",
            'destination_file': path_to_data_folder + "dim_region.csv",
            'columns': ['region-code', 'region']
        }
    )

    get_data_for_dim_sub_region = PythonOperator(
        task_id='get_dim_sub_region_data',
        python_callable=pyhelpers.get_specific_columns_and_store_csv,
        op_kwargs={
            'source_file': path_to_data_folder + "country_continent_isocodes.csv",
            'destination_file': path_to_data_folder + "dim_sub_region.csv",
            'columns': ['sub-region-code', 'region-code', 'sub-region']
        }
    )

    stage_country_dim_table = LoadFromCSVOperator(
        task_id='stage_country_dim_table',
        skip_header_row=True,
        file_path=path_to_data_folder + 'dim_country.csv',
        table_name='public.dim_country'
    )

    stage_region_dim_table = LoadFromCSVOperator(
        task_id='stage_region_dim_table',
        skip_header_row=True,
        file_path=path_to_data_folder + 'dim_region.csv',
        table_name='public.dim_region'
    )

    stage_sub_region_dim_table = LoadFromCSVOperator(
        task_id='stage_sub_region_dim_table',
        skip_header_row=True,
        file_path=path_to_data_folder + 'dim_sub_region.csv',
        table_name='public.dim_sub_region'
    )

    fetch_codes_for_fact_table = FetchDataFromDBOperator(
        task_id='fetch_codes_for_fact_table',
        sql_queries=SqlQueries.fetch_country_region_subregion_codes,
        output_file_name=path_to_data_folder + 'base_for_fact_table.csv',
        headers=['country_code', 'region_code', 'sub_region_code', 'name']
    )

    get_historical_data_from_api = PythonOperator(
        task_id='get_historical_data_from_api',
        python_callable=pyhelpers.get_data_from_api,
        op_args=[path_to_data_folder + 'historical_data2.csv', path_to_data_folder + 'base_for_fact_table.csv', 'all']
    )

    load_fact_table_historical_data = LoadFromCSVOperator(
        task_id='load_fact_table_historical_data',
        skip_header_row=False,
        file_path=path_to_data_folder + 'historical_data2.csv',
        table_name='public.fact_corona_data_api'
    )

start_task >> create_base_tables
create_base_tables >> get_data_for_dim_country >> stage_country_dim_table >> fetch_codes_for_fact_table \
>> get_historical_data_from_api >> load_fact_table_historical_data
create_base_tables >> get_data_for_dim_region >> stage_region_dim_table
create_base_tables >> get_data_for_dim_sub_region >> stage_sub_region_dim_table
