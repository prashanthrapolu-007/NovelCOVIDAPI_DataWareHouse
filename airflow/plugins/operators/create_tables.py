from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2
import os


class CreateTablesOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 sql_queries="",
                 *args, **kwargs):
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.sql_queries = sql_queries

    def execute(self, context):
        try:
            self.log.info('Creating Dimension and Fact Tables')
            conn = psycopg2.connect(host=os.getenv('host'), dbname=os.getenv('dbname'),
                                    user=os.getenv('user'), password=os.getenv('password'))
            cursor = conn.cursor()
            cursor.execute(self.sql_queries)
            conn.commit()
            self.log.info('Created staging tables')

        except Exception as e:
            self.log.info('Error:{}'.format(e))
        finally:
            cursor.close()
            conn.close()
            self.log.info('Closing connection')

