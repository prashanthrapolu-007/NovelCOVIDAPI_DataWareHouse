from airflow.operators import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class CreateTablesOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 sql_queries="",
                 *args, **kwargs):
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_queries = sql_queries

    def execute(self, context):
        self.log.info('Creating Stage Tables')
        pg_hook = PostgresHook(postgre_conn_id=self.postgres_conn_id, schema='testdb')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        try:
            cursor.execute(self.sql_queries)
            self.log.info('Created staging tables')
        except Exception as e:
            self.log.info('Error:{}'.format(e))
