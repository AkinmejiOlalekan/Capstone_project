
# End-to-End Data Pipeline with ETL, Orchestration, and Data Transformation

## Project Overview
This is my Capstone project at AltSchool Africa for my Diploma Data Engineering course. This project involves developing an end-to-end ETL (Extract, Transform, Load) process using a dataset from Kaggle to help data end-users answer analytical questions. The project showcases the application of various data engineering tools and techniques, including:

- PostgreSQL for data storage
- Docker and Docker Compose for environment setup
- Apache Airflow for workflow orchestration
- dbt (Data Build Tool) for data transformation and modeling
- Google BigQuery for data warehousing

## Project Steps

### Step 1: Data Ingestion into PostgreSQL
1. **Downloaded the Dataset**: Download the Brazilian E-Commerce dataset from Kaggle from the link provided in the document.
2. **Set up PostgreSQL Database**: Set up a PostgreSQL database using Docker and Docker Compose.
3. **Created Tables**: Created tables in the PostgreSQL database corresponding to each CSV file in the dataset.
4. **Ingested Data**: Used custom Python ETL scripts or an `init.sql` script to ingest the data into the PostgreSQL tables.

### Step 2: Setting up Apache Airflow
1. **Installed Airflow**: Added Airflow to the Docker Compose setup.
2. **Created Airflow DAG**: Created a Directed Acyclic Graph (DAG) in Airflow to orchestrate the ETL process, including tasks to extract data from PostgreSQL and load it into Google Cloud storage for staging purpose and then load it into Google BigQuery.

### Step 3: Loading Data from PostgreSQL to BigQuery
1. **Set up Google BigQuery**: Create a new project in Google Cloud Platform (GCP), enable the BigQuery API, gave it the required privilege roles and create a dataset to hold the e-commerce data.
2. **Loaded Data Using Airflow**: Used Airflow operators to extract data from PostgreSQL, load it into Google Cloud Storage for staging purpose and then load it into Google BigQuery.

### Step 4: Transforming and Modeling Data with dbt
1. **Set up dbt**: Installed dbt and initialized a new dbt project.
2. **Configured dbt**: Configured dbt to connect to the BigQuery dataset.
3. **Created Models**: Created dbt models to transform the raw data into a clean and usable format.

### Step 5: Answering Analytical Questions
Based on my domain knowledge of the Brazilian E-Commerce dataset, I answered  the following three analytical questions:
1. Which product categories have the highest sales?
2. What is the average delivery time for orders?
3. Which states have the highest number of orders?

The dbt models for these analytical questions are:
- `int_sales_by_category.sql`: Aggregates sales data by product category.
- `int_avg_delivery_time.sql`: Calculates the average delivery time for each order.
- `int_orders_by_state.sql`: Counts the number of orders per state.

The final models for the analytical questions are:
- `fct_sales_by_category.sql`
- `fct_avg_delivery_time.sql`
- `fct_orders_by_state.sql`

## Project Deliverables
1. **PostgreSQL Scripts**: Scripts to create tables and ingest data.
2. **Airflow DAG**: DAG file to orchestrate the ETL process.
3. **dbt Project**: dbt models to transform and model the data.
4. **Analysis**: SQL queries or dashboards to answer the three analytical questions.
5. **Docker Compose File**: The YAML manifest showing how the project resources are set up.


## Conclusion
This data engineering capstone project demonstrates my ability to set up a complete ETL pipeline, orchestrate the workflow, transform and model the data, and answer analytical questions. The use of various data engineering tools and techniques showcased my skills and knowledge required for a successful data engineering project.
