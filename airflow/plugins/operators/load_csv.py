from airflow.operators import BaseOperator
import psycopg2
import os


class LoadFromCSVOperator(BaseOperator):
    def __init__(self,
                 file_path="",
                 table_name="",
                 skip_header_row=False,
                 delete_file_after_load=False,
                 *args, **kwargs):
        super(LoadFromCSVOperator, self).__init__(*args, **kwargs)
        self.file_path = file_path
        self.table_name = table_name
        self.skip_header_row = skip_header_row
        self.delete_file_after_load = delete_file_after_load

    def execute(self, context):
        self.log.info('Loading table {} from CSV'.format(self.table_name))
        try:
            connection = psycopg2.connect(host=os.getenv('host'), dbname=os.getenv('dbname'),
                                          user=os.getenv('user'), password=os.getenv('password'))
            cursor = connection.cursor()
            with open(self.file_path, 'r') as f:
                if self.skip_header_row:
                    next(f)
                cursor.copy_from(f, self.table_name, sep=',')
            connection.commit()
            self.log.info('Successfully loaded table {} from CSV'.format(self.table_name))
            if self.delete_file_after_load:
                os.remove(self.file_path)
        except Exception as e:
            self.log.info('Error:{}'.format(e))
        finally:
            cursor.close()
            connection.close()
            self.log.info('Connection closed successfully!')
