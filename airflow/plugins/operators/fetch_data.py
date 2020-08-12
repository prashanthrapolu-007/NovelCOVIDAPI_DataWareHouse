from airflow.operators import BaseOperator
from airflow.utils import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class FetchDataFromDB(BaseOperator):
    def __init__(self,
                 postgres_conn_id="",
                 sql_queries="",
                 output_file_name="",
                 *args, **kwargs):
        super(FetchDataFromDB, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_queries = sql_queries
        self.output_file_name = output_file_name

    def execute(self, context):
        self.log.info('Fetching data from DB')
        try:
            pg_hook = PostgresHook(postgre_conn_id=self.postgres_conn_id)
            connector = pg_hook.get_conn()
            cursor = connector.cursor()
            cursor.execute(self.sql_queries)
            all = cursor.fetchall()
            with open('../../data/'+self.output_file_name+'.csv', 'w'):
                #TODO
                pass
        except Exception as e:
            self.log.info('Error:{}'.format(e))