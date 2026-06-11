DROP SCHEMA IF EXISTS raw;
CREATE SCHEMA IF NOT EXISTS raw;


DROP SCHEMA IF EXISTS bi;
CREATE SCHEMA IF NOT EXISTS bi;


DROP TABLE IF EXISTS raw.products;
CREATE TABLE IF NOT EXISTS raw.products (
    id INT PRIMARY KEY,
    gender TEXT,
    masterCategory TEXT,
    subCategory TEXT,
    articleType TEXT,
    baseColour TEXT,
    season TEXT,
    year INT,
    usage TEXT,
    productDisplayName TEXT
);


DROP TABLE IF EXISTS raw.customers;
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    username UUID,
    email TEXT,
    gender TEXT,
    birthdate DATE,
    device_type TEXT,
    device_id UUID,
    device_version TEXT,
    home_location_lat DECIMAL,
    home_location_long DECIMAL,
    home_location TEXT,
    home_country TEXT,
    first_join_date DATE
);


DROP TABLE IF EXISTS raw.clicks; 
CREATE TABLE IF NOT EXISTS raw.clicks (
    event_id UUID PRIMARY KEY,
    session_id UUID,
    event_name TEXT,
    event_time TIMESTAMP,
    traffic_source TEXT,
    event_metadata JSONB
);


DROP TABLE IF EXISTS raw.transactions;
CREATE TABLE IF NOT EXISTS raw.transactions (
    booking_id UUID PRIMARY KEY,
    session_id UUID,
    customer_id INT,
    created_at TIMESTAMP,
    product_metadata JSONB,
    payment_method TEXT,
    payment_status TEXT,
    promo_amount INT,
    promo_code TEXT,
    shipment_fee INT,
    shipment_date_limit TIMESTAMP,
    shipment_location_lat DECIMAL,
    shipment_location_long DECIMAL,
    total_amount INT,
    
    CONSTRAINT fk_transactions_customer
        FOREIGN KEY (customer_id)
        REFERENCES raw.customers(customer_id)
);


DROP VIEW bi.raw_transactions_v; 
CREATE OR REPLACE VIEW bi.raw_transactions_v as 
SELECT
    booking_id
    , session_id
    , customer_id
    , created_at
    ,(item ->> 'product_id')::INT AS product_id
    ,(item ->> 'quantity')::INT AS quantity
    ,(item ->> 'item_price')::NUMERIC AS item_price
    , payment_method
    , payment_status
    , promo_amount
    , promo_code
    , shipment_fee
    , shipment_date_limit
    , shipment_location_lat
    , shipment_location_long
    , total_amount 
FROM raw.transactions
CROSS JOIN LATERAL jsonb_array_elements(product_metadata) AS item;


CREATE TABLE IF NOT EXISTS bi.daily_sales (
    sales_date DATE PRIMARY KEY,
    total_orders INT,
    total_customers INT,
    total_revenue DECIMAL,
    avg_order_value DECIMAL,
    successful_payments INT,
    failed_payments INT
);


CREATE TABLE IF NOT EXISTS bi.daily_product_performance (
    sales_date DATE,
    product_id INT,
    product_name TEXT,
    category TEXT,
    sub_category TEXT,
    article_type TEXT,
    total_quantity INT,
    total_revenue DECIMAL,
    total_orders INT,
    PRIMARY KEY (sales_date, product_id)
);


CREATE TABLE IF NOT EXISTS bi.daily_customer_metrics (
    sales_date DATE,
    customer_id INT,
    gender TEXT,
    device_type TEXT,
    total_payments INT,
    successful_payments INT,
    failed_payments INT,
    total_spent DECIMAL,
    avg_payment_value DECIMAL,
    PRIMARY KEY (sales_date, customer_id)
);


CREATE TABLE IF NOT EXISTS bi.daily_traffic_source_performance (
    event_date DATE,
    traffic_source TEXT,
    total_sessions INT,
    total_events INT,
    unique_events INT,
    PRIMARY KEY (event_date, traffic_source)
);