from airflow.operators import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
import psycopg2
import os


class LoadFromCSVOperator(BaseOperator):
    def __init__(self,
                 postgres_conn_id="",
                 file_path="",
                 table_name="",
                 database="",
                 skip_header_row=False,
                 *args, **kwargs):
        super(LoadFromCSVOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.file_path = file_path
        self.table_name = table_name
        self.skip_header_row = skip_header_row
        self.database = database

    def execute(self, context):
        self.log.info('Loading table {} from CSV'.format(self.table_name))
        try:
            # pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
            # connection = pg_hook.get_conn()
            connection = psycopg2.connect("host=localhost dbname=testdb user=postgres password=admin")
            cursor = connection.cursor()
            if os.environ['path_to_data_folder']:
                file_path = os.environ.get('path_to_data_folder')
            else:
                raise ValueError('Environment variable file_path is not set up')
            with open(file_path+'country_continent_isocodes.csv', 'r') as f:
                if self.skip_header_row:
                    next(f)
                cursor.copy_from(f, self.table_name, sep=',')
            connection.commit()
            self.log.info('Successfully loaded table {} from CSV'.format(self.table_name))
        except Exception as e:
            self.log.info('Error:{}'.format(e))
