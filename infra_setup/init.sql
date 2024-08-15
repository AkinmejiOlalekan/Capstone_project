-- create a schema

CREATE SCHEMA IF NOT EXISTS CAPSTONE_PROJECT;


-- create a table
create table if not exists capstone_project.olist_customers_dataset
(
    customer_id uuid not null,
    customer_unique_id uuid not null,
    customer_zip_code_prefix bigint not null,
    customer_city varchar not null,
    customer_state varchar not null
);


COPY capstone_project.olist_customers_dataset (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
FROM '/data/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';


create table if not exists capstone_project.olist_geolocation_dataset
(
    geolocation_zip_code_prefix bigint not null,
    geolocation_lat numeric(20, 15),
    geolocation_lng numeric(20, 15),
    geolocation_city varchar not null,
    geolocation_state varchar not null
);


COPY capstone_project.olist_geolocation_dataset (geolocation_zip_code_prefix,geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
FROM '/data/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';


create table if not exists capstone_project.olist_order_items_dataset
(
    order_id uuid not null, 
    order_item_id int not null, 
    product_id uuid not null, 
    seller_id uuid not null, 
    shipping_limit_date timestamp not null, 
    price numeric(10, 3) not null, 
    freight_value numeric(10, 3) not null
);


COPY capstone_project.olist_order_items_dataset (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
FROM '/data/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';


create table if not exists capstone_project.olist_order_payments_dataset
(
    order_id uuid not null, 
    payment_sequential int not null, 
    payment_type varchar not null, 
    payment_installments int not null, 
    payment_value numeric(10, 3) not null
);


COPY capstone_project.olist_order_payments_dataset (order_id, payment_sequential, payment_type, payment_installments, payment_value)
FROM '/data/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';


create table if not exists capstone_project.olist_order_reviews_dataset
(
    review_id uuid not null, 
    order_id uuid not null, 
    review_score int not null, 
    review_comment_title varchar, 
    review_comment_message varchar, 
    review_creation_date timestamp not null, 
    review_answer_timestamp timestamp not null
);


COPY capstone_project.olist_order_reviews_dataset (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
FROM '/data/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';


create table if not exists capstone_project.olist_orders_dataset
(
    order_id uuid not null, 
    customer_id uuid not null, 
    order_status varchar not null, 
    order_purchase_timestamp timestamp not null, 
    order_approved_at timestamp, 
    order_delivered_carrier_date timestamp, 
    order_delivered_customer_date timestamp, 
    order_estimated_delivery_date timestamp
);


COPY capstone_project.olist_orders_dataset (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
FROM '/data/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';


create table if not exists capstone_project.olist_product_dataset
(
    product_id uuid not null, 
    product_category_name varchar, 
    product_name_lenght int, 
    product_description_lenght int, 
    product_photos_qty int, 
    product_weight_g int, 
    product_length_cm int, 
    product_height_cm int, 
    product_width_cm int
);


COPY capstone_project.olist_product_dataset (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
FROM '/data/olist_products_dataset.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';


create table if not exists capstone_project.olist_sellers_dataset
(
    seller_id uuid not null, 
    seller_zip_code_prefix bigint, 
    seller_city varchar, 
    seller_state varchar not null
);


COPY capstone_project.olist_sellers_dataset (seller_id, seller_zip_code_prefix, seller_city, seller_state)
FROM '/data/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';


create table if not exists capstone_project.product_category_name_translation
(
    product_category_name varchar,
    product_category_name_english varchar
);


COPY capstone_project.product_category_name_translation (product_category_name,product_category_name_english)
FROM '/data/product_category_name_translation.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';