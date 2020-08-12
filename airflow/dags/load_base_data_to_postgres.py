from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from operators import CreateTablesOperator, LoadFromCSVOperator
from helpers import SqlQueries, pyhelpers

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

    stage_base_tables = CreateTablesOperator(
        task_id='create_stage_base_tables',
        posgres_conn_id='postgres_conn',  # os.environ['postgres_connection_id'],
        sql_commands=SqlQueries.create_staging_tables
    )

    stage_mapping_table = LoadFromCSVOperator(
        task_id='stage_mapping_table',
        postgres_conn_id='postgres_conn',
        skip_header_row=True,
        file_path='../data/country_continent_isocodes.csv',
        table_name='staging.country_continent_map'
    )

    get_covid_data_from_api = PythonOperator(

    )

start_task >> stage_base_tables
