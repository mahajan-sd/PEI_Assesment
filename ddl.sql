-- Databricks notebook source
-- Bronze

CREATE SCHEMA IF NOT EXISTS maddy_test.bronze;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS maddy_test.bronze.raw_orders (
    row_id        BIGINT,
    order_id      STRING,
    order_date    STRING,
    ship_date     STRING,
    ship_mode     STRING,
    customer_id   STRING,
    product_id    STRING,
    quantity      BIGINT,
    price         DOUBLE,
    discount      DOUBLE,
    profit        DOUBLE,
    _ingested_at  TIMESTAMP,
    _source_file  STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS maddy_test.bronze.raw_products (
    product_id         STRING,
    category           STRING,
    sub_category       STRING,
    product_name       STRING,
    state              STRING,
    price_per_product  DOUBLE,
    _ingested_at       TIMESTAMP,
    _source_file       STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS maddy_test.bronze.raw_customers (
    customer_id    STRING,
    customer_name  STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    segment        STRING,
    country        STRING,
    city           STRING,
    state          STRING,
    postal_code    STRING,
    region         STRING,
    _ingested_at   TIMESTAMP,
    _source_file   STRING
) USING DELTA;

-- COMMAND ----------

-- Silver

CREATE SCHEMA IF NOT EXISTS maddy_test.silver;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS maddy_test.silver.customers (
    customer_id    STRING,
    customer_name  STRING,
    email          STRING,
    phone          STRING,
    address        STRING,
    segment        STRING,
    country        STRING,
    city           STRING,
    state          STRING,
    postal_code    STRING,
    region         STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS maddy_test.silver.products (
    product_id     STRING,
    product_name   STRING,
    category       STRING,
    sub_category   STRING
) USING DELTA;

-- orders enriched with customer and product details
CREATE TABLE IF NOT EXISTS maddy_test.silver.orders (
    row_id         BIGINT,
    order_id       STRING,
    order_date     DATE,
    ship_date      DATE,
    ship_mode      STRING,
    customer_id    STRING,
    customer_name  STRING,
    country        STRING,
    product_id     STRING,
    category       STRING,
    sub_category   STRING,
    quantity       INT,
    price          DOUBLE,
    discount       DOUBLE,
    profit         DECIMAL(18,2)
) USING DELTA;

-- COMMAND ----------

-- Gold

CREATE SCHEMA IF NOT EXISTS maddy_test.gold;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS maddy_test.gold.profit_summary (
    order_year     INT,
    category       STRING,
    sub_category   STRING,
    customer_id    STRING,
    customer_name  STRING,
    total_profit   DECIMAL(18,2)
) USING DELTA;
