from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from operators import CreateTablesOperator, LoadFromCSVOperator, FetchDataFromDBOperator
from helpers import SqlQueries, pyhelpers
from airflow.operators.sqlite_operator import SqliteOperator

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
        task_id='create_stage_base_tables',
        sql_queries=SqlQueries.create_staging_tables
    )

    stage_mapping_table = LoadFromCSVOperator(
        task_id='stage_mapping_table',
        skip_header_row=True,
        file_path=path_to_data_folder + 'country_continent_isocodes.csv',
        table_name='public.country_continent_map'
    )

    fetch_country_names = FetchDataFromDBOperator(
        task_id='fetch_country_names',
        sql_queries=SqlQueries.fetch_country_names,
        output_file_name='countries',
        headers=['Country']
    )

    get_historical_data_from_api = PythonOperator(
        task_id='get_historical_data_from_api',
        python_callable=pyhelpers.get_data_from_api,
        op_args=[path_to_data_folder+'historical_data.csv', path_to_data_folder+'countries.csv', 'all']
    )

    stage_historical_data = LoadFromCSVOperator(
        task_id='stage_historical_data',
        skip_header_row=False,
        file_path=path_to_data_folder + 'historical_data.csv',
        table_name='public.corona_data_api'
    )

start_task >> create_base_tables >> stage_mapping_table >> fetch_country_names >> get_historical_data_from_api >> stage_historical_data
