from airflow.operators import BaseOperator
from airflow.utils import apply_defaults
import psycopg2

import os
import csv


class FetchDataFromDBOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 sql_queries="",
                 output_file_name="",
                 headers="",
                 *args, **kwargs):
        super(FetchDataFromDBOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_queries = sql_queries
        self.output_file_name = output_file_name
        self.headers = headers

    def execute(self, context):
        self.log.info('Fetching data from DB')
        try:
            connection = psycopg2.connect("host=localhost dbname=testdb user=postgres password=admin")
            cursor = connection.cursor()
            cursor.execute(self.sql_queries)
            data = cursor.fetchall()
            self.log.info('fetched data from postgres')

            file_path = '/home/nani/airflow_projects/Corona_DataWareHouse_Analytics/airflow/data/'
            with open(file_path + self.output_file_name + '.csv', 'w') as file:
                writer = csv.writer(file, delimiter=",")
                writer.writerow(self.headers)
                writer.writerows(data)
        except Exception as e:
            self.log.info('Error:{}'.format(e))
        finally:
            connection.close()
            self.log.info('Connection closed successfully!')
