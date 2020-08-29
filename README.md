# NovelCOVIDAPI_DataWareHouse
This is a Data Warehouse project focused on analytics on spread of Corona across different countries and regions.

### Tech Stack
This project used the following technologies
  1. Apache Airflow for Data Orchestration
  2. Postgres for Data Warehouse
  3. Python's Matplotlib for Visualizations
  
#### Overview
Data is collected from [Postman API](https://documenter.getpostman.com/view/11144369/Szf6Z9B3?version=latest). Data is set up initially by creating all the
required fact and dimension tables and all historical data is uplaoded into postgres. 
After initial set up, a job is scheduled that runs everyday to fetch the data for that day and udpates the Data Warehouse with new values.
As of now, analytics are generated using Python's Matlotlib.


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
There are two dags.
* load_base_data_to_postgres: This is a one-time job that is run when initially setting up the system.
  1. Creates dimension and fact tables
  2. Collects data required from API and stores them in local data folder
  3. Once the data is collected, a task in airflow is triggered to load the data into respective tables.
  4. Dag execution completes once the data is uploaded into Data Warehouse
![DAG1](/imgs/dag1.png)
  
* udpate_daily_data: This job is scheduled to run every day
  1. Collects data from API for the date on which it is running
  2. UPSERTS the data into fact table
  3. Once new data is availabe in Data Warehouse, an airflow task is triggered to create analytics
![DAG2](/imgs/dag2.png)  
