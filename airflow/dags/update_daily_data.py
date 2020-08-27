from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators import LoadFromCSVOperator, VisualizeDataOperator
from datetime import datetime, timedelta
from helpers import SqlQueries, pyhelpers, visualize

path_to_data_folder = '/home/nani/airflow_projects/Corona_DataWareHouse_Analytics/airflow/data/'

default_args = {
    'owner': 'nani',
    'start_date': datetime(2019, 1, 1),
    'retry_delay': timedelta(minutes=5)
}

with DAG('load_daily_data', default_args=default_args, schedule_interval='@once') as dag:
    start_task = DummyOperator(
        task_id='dummy_start'
    )

    get_yesterday_data_from_api = PythonOperator(
        task_id='get_yesterday_data',
        python_callable=pyhelpers.get_data_from_api,
        op_kwargs={
            'output_file_path': path_to_data_folder + 'yesterday_api_data.csv',
            'countries_csv_file': path_to_data_folder+'base_for_fact_table.csv',
            'history_param': 1
        }
    )

    merge_yesterday_data_with_historical_data = LoadFromCSVOperator(
        task_id='merge_data',
        file_path=path_to_data_folder + 'yesterday_api_data.csv',
        skip_header_row=False,
        delete_file_after_load=True,
        table_name='public.fact_corona_data_api'
    )

    visualize_analytics = PythonOperator(
        task_id='visualize_analytics',
        python_callable=visualize.visualize_graphs,
        op_kwargs={
            'sql_queries': [SqlQueries.visualize_last_7_days_country_data,
                            SqlQueries.visualize_last_15_days_country_data,
                            SqlQueries.visualize_last_30_days_country_data],
            'xlabel': 'countries'
        }
    )

start_task >> get_yesterday_data_from_api >> merge_yesterday_data_with_historical_data >> visualize_analytics
