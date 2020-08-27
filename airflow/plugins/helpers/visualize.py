import psycopg2
import os
import matplotlib.pyplot as plt


def connect_to_db():
    conn = psycopg2.connect(host=os.getenv('host'), dbname=os.getenv('dbname'),
                            user=os.getenv('user'), password=os.getenv('password'))
    return conn


def plot_bar_graph(country, records, file_name, xlabel):
    plt.style.use('ggplot')

    x_pos = [i for i, _ in enumerate(country)]

    plt.barh(x_pos, records, color='green')
    plt.ylabel("Top 10 " + xlabel + " with most cases")
    plt.xlabel(file_name)
    plt.title("New "+file_name+" in the last seven days")

    plt.yticks(x_pos, country)
    plt.tight_layout()
    plt.savefig(os.getenv('path_to_viz')+'/'+file_name+'.png', bbox_inches="tight")
    plt.close()


def get_data_from_db(conn, sql_queries):
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute(sql_queries)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


def visualize_graphs(sql_queries, xlabel):

    graph_dict = {'cases': 1, 'recovered': 2, 'deaths': 3}
    conn = connect_to_db()
    for query in sql_queries:
        data = get_data_from_db(conn, query)

        for key, value in graph_dict.items():
            country = [row[0] for row in data]
            records = [row[value] for row in data]
            plot_bar_graph(country, records, key, xlabel)
