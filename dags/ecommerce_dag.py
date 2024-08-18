from airflow import DAG
import logging

from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


logging.basicConfig(level=logging.INFO,format='%(asctime)s-%(levelname)s-%(message)s')


#BigQuery config variables
BQ_CONN_ID="gcp_conn"
BQ_PROJECT="spartan-tesla-410811"
BQ_DATASET="Ecommerce_data"
BQ_TABLE1="ecommerce_demo"
BQ_BUCKET="ecommerce_staging_bucket"



# Postgres config variables
PG_CONN_ID="source_db_conn"
PG_SCHEMA="capstone_project"
PG_TABLE= ["olist_customers_dataset", 
           "olist_geolocation_dataset", 
           "olist_order_items_dataset", 
           "olist_order_payments_dataset", 
           "olist_order_reviews_dataset", 
           "olist_orders_dataset", 
           "olist_product_dataset", 
           "olist_sellers_dataset", 
           "product_category_name_translation"]

CSV_FILENAME1='olist_customers_dataset' + datetime.now().strftime('%Y-%m-%d') + '.csv'
CSV_FILENAME2='olist_geolocation_dataset' + datetime.now().strftime('%Y-%m-%d') + '.csv'
CSV_FILENAME3='olist_order_items_dataset' + datetime.now().strftime('%Y-%m-%d') + '.csv'
CSV_FILENAME4='olist_order_payments_dataset' + datetime.now().strftime('%Y-%m-%d') + '.csv'
CSV_FILENAME5='olist_order_reviews_dataset' + datetime.now().strftime('%Y-%m-%d') + '.csv'
CSV_FILENAME6='olist_orders_dataset' + datetime.now().strftime('%Y-%m-%d') + '.csv'
CSV_FILENAME7='olist_products_dataset' + datetime.now().strftime('%Y-%m-%d') + '.csv'
CSV_FILENAME8='olist_sellers_dataset' + datetime.now().strftime('%Y-%m-%d') + '.csv'
CSV_FILENAME9='product_category_name_translation' + datetime.now().strftime('%Y-%m-%d') + '.csv'

# my_var = Variable.get("my_variable_key", default_var = "default_value")

default_arg = {
    'owner': 'altschool_interns',
    'start_date': datetime(2024, 8, 17),
    'depend_on_past': False,
    'retries': 1, # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5) # Time between retries
}

dag = DAG (
    'postgres_gbq_etl',
    default_args=default_arg,
    description = 'An Airflow DAG to load data from Postgres to BigQuery',
    schedule_interval=None,
    catchup= False
)

schemas = {
        "olist_customers_dataset": [
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_unique_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_zip_code_prefix", "type": "INT64", "mode": "REQUIRED"},
            {"name": "customer_city", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_state", "type": "STRING", "mode": "REQUIRED"}],

        "olist_geolocation_dataset": [
            {"name": "geolocation_zip_code_prefix", "type": "INT64", "mode": "REQUIRED"},
            {"name": "geolocation_lat", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "geolocation_lng", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "geolocation_city", "type": "STRING", "mode": "REQUIRED"},
            {"name": "geolocation_state", "type": "STRING", "mode": "REQUIRED"}],

        "olist_order_items_dataset": [
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "order_item_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "seller_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "shipping_limit_date", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "price", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "freight_value", "type": "NUMERIC", "mode": "REQUIRED"}],

        "olist_order_payments_dataset": [
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "payment_sequential", "type": "INT64", "mode": "REQUIRED"},
            {"name": "payment_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "payment_installments", "type": "INT64", "mode": "REQUIRED"},
            {"name": "payment_value", "type": "NUMERIC", "mode": "REQUIRED"}],

        "olist_order_reviews_dataset": [
            {"name": "review_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "review_score", "type": "INT64", "mode": "REQUIRED"},
            {"name": "review_comment_title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "review_comment_message", "type": "STRING", "mode": "NULLABLE"},
            {"name": "review_creation_date", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "review_answer_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}],

        "olist_orders_dataset": [
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "order_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "order_purchase_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "order_approved_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "order_delivered_carrier_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "order_delivered_customer_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "order_estimated_delivery_date", "type": "TIMESTAMP", "mode": "NULLABLE"}],

        "olist_product_dataset":[
            {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "product_category_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_name_lenght", "type": "INT64", "mode": "NULLABLE"},
            {"name": "product_description_lenght", "type": "INT64", "mode": "NULLABLE"},
            {"name": "product_photos_qty", "type": "INT64", "mode": "NULLABLE"},
            {"name": "product_weight_g", "type": "INT64", "mode": "NULLABLE"},
            {"name": "product_length_cm", "type": "INT64", "mode": "NULLABLE"},
            {"name": "product_height_cm", "type": "INT64", "mode": "NULLABLE"},
            {"name": "product_width_cm", "type": "INT64", "mode": "NULLABLE"}],

        "olist_sellers_dataset":[
            {"name": "seller_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "seller_zip_code_prefix", "type": "INT64", "mode": "NULLABLE"},
            {"name": "seller_city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "seller_state", "type": "STRING", "mode": "REQUIRED"}],

        "product_category_name_translation":[
            {"name": "product_category_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_category_name_english", "type": "STRING", "mode": "NULLABLE"}
        ]
}


for table in PG_TABLE:
    schema = schemas.get(table)
    logging.info(f"Table extraction begin from the sorce database: {table}")
    if schema is None:
        logging.error(f"No such schema: {table}")
        raise ValueError(f"No such schema: {table}")


start = EmptyOperator(
    task_id='start',
    dag=dag)

postgres_data_to_gcs = PostgresToGCSOperator(
    task_id = 'postgres_to_gcs',
    sql = f'SELECT * FROM "{PG_SCHEMA}"."{table}";',
    bucket = BQ_BUCKET,
    filename = [CSV_FILENAME1, 
                CSV_FILENAME2, 
                CSV_FILENAME3, 
                CSV_FILENAME4, 
                CSV_FILENAME5, 
                CSV_FILENAME6, 
                CSV_FILENAME7, 
                CSV_FILENAME8, 
                CSV_FILENAME9],
    export_format = 'CSV', # You can change the export format as needed
    postgres_conn_id = PG_CONN_ID, # Set your postgres connection ID
    field_delimiter = ',',
    gzip = False, # Set to True if you want to compress the output file
    task_concurrency = 1, #Optional, adjust concurrency as needed
    gcp_conn_id = BQ_CONN_ID,
    dag = dag
)

bq_load_csv = GCSToBigQueryOperator(
    task_id = 'bq_load_csv',
    bucket = BQ_BUCKET,
    source_objects = [CSV_FILENAME1, CSV_FILENAME2, CSV_FILENAME3, CSV_FILENAME4, CSV_FILENAME5, CSV_FILENAME6, CSV_FILENAME7, CSV_FILENAME8, CSV_FILENAME9],
    destination_project_dataset_table = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE1}",
    schema_fields = schema,
    create_disposition = 'CREATE_IF_NEEDED',
    write_disposition = "WRITE_TRUNCATE",
    gcp_conn_id = BQ_CONN_ID,
    allow_jagged_rows = True, # To allow data with missing rows copy GCS
    dag = dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag)

# Define task dependencies

start >> postgres_data_to_gcs >> bq_load_csv >> end