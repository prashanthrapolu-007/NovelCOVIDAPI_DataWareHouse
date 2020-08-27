from airflow.operators import BaseOperator
import psycopg2
import os

import matplotlib.pyplot as plt
path_to_viz = '/home/nani/airflow_projects/Corona_DataWareHouse_Analytics/airflow/data/analytics/'

class VisualizeDataOperator(BaseOperator):
    def __init__(self,
                 sql_queries="",
                 *args,
                 **kwargs):
        super(VisualizeDataOperator, self).__init__(*args, **kwargs)
        self.sql_queries = sql_queries

    def visualize_bar_chart(self, data):
        field='cases'
        plt.style.use('ggplot')

        country = [row[0] for row in data]
        cases = [row[1] for row in data]

        x_pos = [i for i, _ in enumerate(country)]

        plt.barh(x_pos, cases, color='orange')
        plt.ylabel("Top 10 countries with most cases")
        plt.xlabel("New cases")
        plt.title("New cases in the last seven days")

        plt.yticks(x_pos, country)
        plt.savefig(path_to_viz+'last_seven_days_new_'+field+'.png')
        plt.close()

    def execute(self, context):
        try:
            self.log.info('Retrieving data for visualization')
            conn = psycopg2.connect(host=os.getenv('host'), dbname=os.getenv('dbname'),
                                    user=os.getenv('user'), password=os.getenv('password'))
            cursor = conn.cursor()
            cursor.execute(self.sql_queries)
            data = cursor.fetchall()
            self.visualize_bar_chart(self, data)
        except Exception as e:
            self.log.info('Error:{}'.format(e))