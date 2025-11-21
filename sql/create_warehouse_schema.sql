-- Connect to database 'dw' as dw_user
CREATE SCHEMA IF NOT EXISTS ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce.dim_customer (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS ecommerce.dim_product (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT
);

CREATE TABLE IF NOT EXISTS ecommerce.fact_orders (
    order_id TEXT PRIMARY KEY,
    order_date DATE,
    customer_id TEXT,
    product_id TEXT,
    quantity INTEGER,
    price NUMERIC,
    revenue NUMERIC
);
