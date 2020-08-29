# NovelCOVIDAPI_DataWareHouse
This is a Data Warehouse project focused on analytics on spread of Corona across different countries and regions.
Data is collected from [NovelCOVID API](https://documenter.getpostman.com/view/11144369/Szf6Z9B3?version=latest). The collected data is then uploaded to Data Warehouse daily and analytics are generated on country, regoin and sub_region level. The number of new cases, new recoveries and new deaths in the last seven, fifteen and thirty days are visualized for every level.

### Tech Stack
This project used the following technologies
  1. Orchestration : Apache Airflow
  2. Data Warehouse : Postgres
  3. Visualizations : Matplotlib
  
## Dimensional Data Modeling
Following are the fact and dimension tables used
### Fact Tables
  >1. fact_corona_data_api
### Dimension Tables  
  >1. dim_country
  >2. dim_region
  >3. dim_sub_region
### Star Schema 
![Entity Relationship Diagram](/imgs/Entity_Relationship_Diagram.png)
## ETL Flow
The overall data flow and set up is divided across two dags. The first dag is run only once. The second dag is scheduled to run daily.

DAG 1:
* load_base_data_to_postgres: This is a one-time job that is run when initially setting up the system.
  1. Creates dimension and fact tables
  2. Collects data required from API and stores them in local data folder
  3. Once the data is collected, a task in airflow is triggered to load the data into respective tables.
  4. Dag execution completes once the data is uploaded into Data Warehouse
![DAG1](/imgs/dag1.png)

DAG 2:  
* udpate_daily_data: This job is scheduled to run every day
  1. Collects data from API for the date on which it is running
  2. UPSERT the data into fact table
  3. Once new data is availabe in Data Warehouse, an airflow task is triggered to create analytics.
![DAG2](/imgs/dag2.png)  
