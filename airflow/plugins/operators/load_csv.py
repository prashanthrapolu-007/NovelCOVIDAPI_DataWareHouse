from airflow.operators import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class LoadFromCSVOperator(BaseOperator):
    def __init__(self,
                 postgres_conn_id="",
                 file_path="",
                 table_name="",
                 skip_header_row=False,
                 *args, **kwargs):
        super(LoadFromCSVOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.file_path = file_path
        self.table_name = table_name
        self.skip_header_row = skip_header_row

    def execute(self, context):
        self.log.info('Loading table {} from CSV'.format(self.table_name))
        try:
            pg_hook = PostgresHook(postgre_conn_id=self.postgres_conn_id)
            connection = pg_hook.get_conn()
            cursor = connection.cursor()
            with open(self.file_path, 'r') as f:
                if self.skip_header_row:
                    next(f)
                cursor.copy_from(f, self.table_name, sep=',')
            connection.commit()
            self.log.info('Successfully loaded table {} from CSV'.format(self.table_name))
        except Exception as e:
            self.log.info('Error:{}'.format(e))
