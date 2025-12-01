#!/bin/bash

echo "Заапуск ClickHouse..."
sleep 10

echo "Создание таблиц в ClickHouse..."

clickhouse-client --query="
CREATE DATABASE IF NOT EXISTS pet_shop_analytics;
"

clickhouse-client --query="
CREATE USER IF NOT EXISTS spark_etl IDENTIFIED WITH plaintext_password BY 'spark_pass' HOST ANY;
"

clickhouse-client --query="
GRANT ALL PRIVILEGES ON pet_shop_analytics.* TO spark_etl;
"

clickhouse-client --query="
GRANT SHOW TABLES, CREATE TABLE, DROP TABLE ON pet_shop_analytics.* TO spark_etl;
"

clickhouse-client --query="
CREATE TABLE IF NOT EXISTS pet_shop_analytics.report_products_sales
(
    product_name String,
    category_name String,
    total_quantity_sold UInt64,
    total_revenue Decimal(18, 2),
    sales_count UInt64,
    rating Nullable(Decimal(3, 1)),
    reviews_count Nullable(UInt32)
)
ENGINE = MergeTree()
ORDER BY (product_name, category_name);
"

clickhouse-client --query="
CREATE TABLE IF NOT EXISTS pet_shop_analytics.report_customers_sales
(
    customer_key Int32,
    first_name String,
    last_name String,
    country String,
    total_purchases Decimal(18, 2),
    total_items UInt64,
    order_count UInt64,
    avg_check Decimal(18, 2)
)
ENGINE = MergeTree()
ORDER BY (customer_key);
"

clickhouse-client --query="
CREATE TABLE IF NOT EXISTS pet_shop_analytics.report_time_sales
(
    date_key Int32,
    full_date Date,
    year UInt16,
    month UInt8,
    quarter UInt8,
    month_name String,
    daily_revenue Decimal(18, 2),
    daily_quantity UInt64,
    sales_count UInt64,
    avg_order_size Decimal(18, 2)
)
ENGINE = MergeTree()
ORDER BY (date_key);
"

clickhouse-client --query="
CREATE TABLE IF NOT EXISTS pet_shop_analytics.report_stores_sales
(
    store_key Int32,
    store_name String,
    city String,
    country String,
    total_revenue Decimal(18, 2),
    total_quantity UInt64,
    sales_count UInt64,
    avg_check Decimal(18, 2)
)
ENGINE = MergeTree()
ORDER BY (store_key);
"

clickhouse-client --query="
CREATE TABLE IF NOT EXISTS pet_shop_analytics.report_suppliers_sales
(
    supplier_key Int32,
    supplier_name String,
    country String,
    total_revenue Decimal(18, 2),
    total_quantity UInt64,
    sales_count UInt64,
    avg_product_price Decimal(18, 2)
)
ENGINE = MergeTree()
ORDER BY (supplier_key);
"

clickhouse-client --query="
CREATE TABLE IF NOT EXISTS pet_shop_analytics.report_product_quality
(
    product_name String,
    rating Nullable(Decimal(3, 1)),
    reviews_count Nullable(UInt32),
    total_quantity_sold UInt64,
    total_revenue Decimal(18, 2),
    sales_count UInt64,
    avg_rating Decimal(3, 1)
)
ENGINE = MergeTree()
ORDER BY (product_name);
"

echo "Таблицы созданы"



