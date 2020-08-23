from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2


class CreateTablesOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 sql_queries="",
                 *args, **kwargs):
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.sql_queries = sql_queries

    def execute(self, context):
        try:
            self.log.info('Creating Stage Tables')
            conn = psycopg2.connect("host=localhost dbname=testdb user=postgres password=admin")
            cursor = conn.cursor()
            cursor.execute(self.sql_queries)
            conn.commit()
            self.log.info('Created staging tables')

        except Exception as e:
            self.log.info('Error:{}'.format(e))
        finally:
            self.log.info('Closing connection')
            conn.close()
