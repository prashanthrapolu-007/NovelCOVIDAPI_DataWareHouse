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


with DAG('setup_base_data', default_args=default_args, schedule_interval='@once') as dag:
    start_task = DummyOperator(
        task_id='dummy_start'
    )

    create_base_tables = CreateTablesOperator(
        task_id='create_stage_base_tables',
        # postgres_conn_id='postgres_conn',  # os.environ['postgres_connection_id'],
        # database='testdb',
        sql_queries=SqlQueries.create_staging_tables
    )

    stage_mapping_table = LoadFromCSVOperator(
        task_id='stage_mapping_table',
        # postgres_conn_id='postgres_conn',
        # database='testdb',
        skip_header_row=True,
        file_path='../../data/country_continent_isocodes.csv',
        table_name='public.country_continent_map'
    )

    fetch_country_names = FetchDataFromDBOperator(
        task_id='fetch_country_names',
        sql_queries=SqlQueries.fetch_country_names,
        output_file_name='countries',
        headers=['Country']
    )

start_task >> create_base_tables >> stage_mapping_table >> fetch_country_names

