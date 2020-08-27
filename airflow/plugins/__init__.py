from airflow.plugins_manager import AirflowPlugin
import operators
import helpers


class NovelCovidAnalyticsPlugin(AirflowPlugin):
    name = "covid_analytics_plugin"
    operators = [
        operators.CreateTablesOperator,
        operators.LoadFromCSVOperator,
        operators.FetchDataFromDBOperator,
        operators.VisualizeDataOperator
    ]

    helpers = [
        helpers.SqlQueries
    ]
