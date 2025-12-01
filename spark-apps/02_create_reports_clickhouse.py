from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct, avg, desc, asc,
    year, month, quarter, rank, window, row_number
)
from pyspark.sql.window import Window
import sys
import os
from clickhouse_driver import Client

POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "pet_shop_dw"
POSTGRES_USER = "chistyakovartem214"
POSTGRES_PASSWORD = "passartem"

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_NATIVE_PORT = os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")
CLICKHOUSE_DB = "pet_shop_analytics"
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "spark_etl")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "spark_pass")

CLICKHOUSE_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
CLICKHOUSE_PROPS = {
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

EXTERNAL_JARS_DIR = "/opt/spark/external-jars"

def configure_spark_jars(builder, required_jars, fallback_packages):
    local_paths = []
    for jar in required_jars:
        jar_path = os.path.join(EXTERNAL_JARS_DIR, jar)
        if os.path.exists(jar_path):
            local_paths.append(jar_path)
    if local_paths:
        builder = builder.config("spark.jars", ",".join(local_paths))
    else:
        builder = builder.config("spark.jars.packages", fallback_packages)
    return builder

def get_clickhouse_client():
    return Client(
        host=CLICKHOUSE_HOST,
        port=int(CLICKHOUSE_NATIVE_PORT),
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

def write_report_to_clickhouse(table_name, df):
    client = get_clickhouse_client()
    client.execute(f"TRUNCATE TABLE {table_name}")
    if df.rdd.isEmpty():
        return 0
    pdf = df.toPandas()

    for col_name in pdf.columns:
        if pdf[col_name].dtype == "object":
            try:
                pdf[col_name] = pdf[col_name].astype(float)
            except Exception:
                pass
    data = list(map(tuple, pdf.itertuples(index=False, name=None)))
    columns = ",".join([f"`{c}`" for c in pdf.columns])
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES"
    client.execute(insert_sql, data)
    return len(data)

def create_spark_session():
    if 'SPARK_HOME' not in os.environ:
        os.environ['SPARK_HOME'] = '/opt/spark'
    if 'JAVA_HOME' not in os.environ:
        os.environ['JAVA_HOME'] = '/usr/lib/jvm/default-java'
    os.environ.setdefault('SPARK_CLASSPATH', '/opt/spark/jars/*:/opt/spark/external-jars/*')
    
    builder = SparkSession.builder \
        .appName("CreateReportsClickHouse") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/*:/opt/spark/external-jars/*") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/*:/opt/spark/external-jars/*") \
        .master("local[*]")
    
    builder = configure_spark_jars(
        builder,
        ["postgresql-42.7.1.jar", "clickhouse-jdbc-0.6.0-all.jar", "clickhouse-jdbc-0.6.0.jar"],
        "org.postgresql:postgresql:42.7.1,com.clickhouse:clickhouse-jdbc:0.6.0"
    )
    return builder.getOrCreate()

def create_clickhouse_tables(spark):
    print("Проверка таблиц в ClickHouse...")

def read_star_schema(spark):
    print("Чтение данных из модели звезда...")
    
    fact_sales = spark.read.jdbc(
        url=POSTGRES_URL,
        table="fact_sales",
        properties=POSTGRES_PROPS
    )
    
    dimension_products = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_products",
        properties=POSTGRES_PROPS
    )
    
    dimension_customers = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_customers",
        properties=POSTGRES_PROPS
    )
    
    dimension_sellers = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_sellers",
        properties=POSTGRES_PROPS
    )
    
    dimension_stores = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_stores",
        properties=POSTGRES_PROPS
    )
    
    dimension_store_locations = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_store_locations",
        properties=POSTGRES_PROPS
    )
    
    dimension_suppliers = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_suppliers",
        properties=POSTGRES_PROPS
    )
    
    dimension_product_categories = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_product_categories",
        properties=POSTGRES_PROPS
    )
    
    dimension_dates = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_dates",
        properties=POSTGRES_PROPS
    )
    
    print(f"Загружено {fact_sales.count()} продаж")
    
    return {
        "fact_sales": fact_sales,
        "dimension_products": dimension_products,
        "dimension_customers": dimension_customers,
        "dimension_sellers": dimension_sellers,
        "dimension_stores": dimension_stores,
        "dimension_store_locations": dimension_store_locations,
        "dimension_suppliers": dimension_suppliers,
        "dimension_product_categories": dimension_product_categories,
        "dimension_dates": dimension_dates
    }

def create_report_1_products(spark, data):
    print("\nСоздание витрины продаж по продуктам...")
    
    sales_products = data["fact_sales"].join(
        data["dimension_products"],
        "product_key",
        "inner"
    ).join(
        data["dimension_product_categories"],
        "category_key",
        "inner"
    )
    
    top_products = sales_products.groupBy(
        col("product_name"),
        col("category_name")
    ).agg(
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("total_price").alias("total_revenue"),
        count("*").alias("sales_count")
    ).orderBy(desc("total_quantity")).limit(10)
    
    revenue_by_category = sales_products.groupBy(
        col("category_name")
    ).agg(
        spark_sum("total_price").alias("total_revenue"),
        spark_sum("quantity").alias("total_quantity")
    )
    
    product_ratings = data["dimension_products"].groupBy(
        col("product_name")
    ).agg(
        avg("rating").alias("avg_rating"),
        spark_sum("reviews_count").alias("total_reviews"),
        count("*").alias("product_variants")
    )
    
    report_df = sales_products.groupBy(
        col("product_name"),
        col("category_name"),
        col("rating"),
        col("reviews_count")
    ).agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        spark_sum("total_price").alias("total_revenue"),
        count("*").alias("sales_count")
    ).select(
        col("product_name"),
        col("category_name"),
        col("total_quantity_sold"),
        col("total_revenue"),
        col("sales_count"),
        col("rating"),
        col("reviews_count")
    )
    
    inserted = write_report_to_clickhouse("pet_shop_analytics.report_products_sales", report_df)
    print(f"Создано {inserted} записей в report_products_sales")
    return report_df

def create_report_2_customers(spark, data):
    print("\nСоздание витрины продаж по клиентам...")
    
    sales_customers = data["fact_sales"].join(
        data["dimension_customers"],
        "customer_key",
        "inner"
    )
    
    top_customers = sales_customers.groupBy(
        col("customer_key"),
        col("first_name"),
        col("last_name"),
        col("country")
    ).agg(
        spark_sum("total_price").alias("total_purchases"),
        spark_sum("quantity").alias("total_items"),
        count("*").alias("order_count"),
        avg("total_price").alias("avg_order_value")
    ).orderBy(desc("total_purchases")).limit(10)
    
    customers_by_country = sales_customers.groupBy(
        col("country")
    ).agg(
        countDistinct("customer_key").alias("customer_count"),
        spark_sum("total_price").alias("total_revenue"),
        avg("total_price").alias("avg_order_value")
    )
    
    customer_avg_check = sales_customers.groupBy(
        col("customer_key"),
        col("first_name"),
        col("last_name"),
        col("country")
    ).agg(
        avg("total_price").alias("avg_check"),
        spark_sum("total_price").alias("total_spent"),
        count("*").alias("orders_count")
    )
    
    report_df = sales_customers.groupBy(
        col("customer_key"),
        col("first_name"),
        col("last_name"),
        col("country")
    ).agg(
        spark_sum("total_price").alias("total_purchases"),
        spark_sum("quantity").alias("total_items"),
        count("*").alias("order_count"),
        avg("total_price").alias("avg_check")
    ).select(
        col("customer_key"),
        col("first_name"),
        col("last_name"),
        col("country"),
        col("total_purchases"),
        col("total_items"),
        col("order_count"),
        col("avg_check")
    )
    
    inserted = write_report_to_clickhouse("pet_shop_analytics.report_customers_sales", report_df)
    print(f"Создано {inserted} записей в report_customers_sales")
    return report_df

def create_report_3_time(spark, data):
    print("\nСоздание витрины продаж по времени...")
    
    sales_time = data["fact_sales"].join(
        data["dimension_dates"],
        "date_key",
        "inner"
    )
    
    monthly_trends = sales_time.groupBy(
        col("year"),
        col("month"),
        col("month_name")
    ).agg(
        spark_sum("total_price").alias("monthly_revenue"),
        spark_sum("quantity").alias("monthly_quantity"),
        count("*").alias("sales_count"),
        avg("total_price").alias("avg_order_size")
    ).orderBy("year", "month")
    
    yearly_trends = sales_time.groupBy(
        col("year")
    ).agg(
        spark_sum("total_price").alias("yearly_revenue"),
        spark_sum("quantity").alias("yearly_quantity"),
        count("*").alias("sales_count"),
        avg("total_price").alias("avg_order_size")
    ).orderBy("year")
    
    quarterly_trends = sales_time.groupBy(
        col("year"),
        col("quarter")
    ).agg(
        spark_sum("total_price").alias("quarterly_revenue"),
        spark_sum("quantity").alias("quarterly_quantity"),
        count("*").alias("sales_count")
    ).orderBy("year", "quarter")
    
    report_df = sales_time.groupBy(
        col("date_key"),
        col("full_date"),
        col("year"),
        col("month"),
        col("quarter"),
        col("month_name")
    ).agg(
        spark_sum("total_price").alias("daily_revenue"),
        spark_sum("quantity").alias("daily_quantity"),
        count("*").alias("sales_count"),
        avg("total_price").alias("avg_order_size")
    ).select(
        col("date_key"),
        col("full_date"),
        col("year"),
        col("month"),
        col("quarter"),
        col("month_name"),
        col("daily_revenue"),
        col("daily_quantity"),
        col("sales_count"),
        col("avg_order_size")
    )
    
    inserted = write_report_to_clickhouse("pet_shop_analytics.report_time_sales", report_df)
    print(f"Создано {inserted} записей в report_time_sales")
    return report_df

def create_report_4_stores(spark, data):
    print("\nСоздание витрины продаж по магазинам...")
    
    sales_stores = data["fact_sales"].join(
        data["dimension_stores"],
        "store_key",
        "inner"
    ).join(
        data["dimension_store_locations"],
        "location_key",
        "inner"
    )
    
    top_stores = sales_stores.groupBy(
        col("store_name"),
        col("city"),
        col("country")
    ).agg(
        spark_sum("total_price").alias("total_revenue"),
        spark_sum("quantity").alias("total_quantity"),
        count("*").alias("sales_count")
    ).orderBy(desc("total_revenue")).limit(5)
    
    sales_by_location = sales_stores.groupBy(
        col("city"),
        col("country")
    ).agg(
        spark_sum("total_price").alias("total_revenue"),
        spark_sum("quantity").alias("total_quantity"),
        count("*").alias("sales_count"),
        countDistinct("store_key").alias("store_count")
    )
    
    store_avg_check = sales_stores.groupBy(
        col("store_name"),
        col("city"),
        col("country")
    ).agg(
        avg("total_price").alias("avg_check"),
        spark_sum("total_price").alias("total_revenue"),
        count("*").alias("sales_count")
    )
    
    report_df = sales_stores.groupBy(
        col("store_key"),
        col("store_name"),
        col("city"),
        col("country")
    ).agg(
        spark_sum("total_price").alias("total_revenue"),
        spark_sum("quantity").alias("total_quantity"),
        count("*").alias("sales_count"),
        avg("total_price").alias("avg_check")
    ).select(
        col("store_key"),
        col("store_name"),
        col("city"),
        col("country"),
        col("total_revenue"),
        col("total_quantity"),
        col("sales_count"),
        col("avg_check")
    )
    
    inserted = write_report_to_clickhouse("pet_shop_analytics.report_stores_sales", report_df)
    print(f"Создано {inserted} записей в report_stores_sales")
    return report_df

def create_report_5_suppliers(spark, data):
    print("\nСоздание витрины продаж по поставщикам...")
    
    sales_suppliers = data["fact_sales"].join(
        data["dimension_products"],
        "product_key",
        "inner"
    ).join(
        data["dimension_suppliers"],
        "supplier_key",
        "inner"
    )
    
    top_suppliers = sales_suppliers.groupBy(
        col("supplier_name"),
        col("country")
    ).agg(
        spark_sum("total_price").alias("total_revenue"),
        spark_sum("quantity").alias("total_quantity"),
        count("*").alias("sales_count")
    ).orderBy(desc("total_revenue")).limit(5)
    
    supplier_avg_price = data["dimension_products"].join(
        data["dimension_suppliers"],
        "supplier_key",
        "inner"
    ).groupBy(
        col("supplier_name"),
        col("country")
    ).agg(
        avg("price").alias("avg_product_price"),
        count("*").alias("product_count")
    )
    
    sales_by_supplier_country = sales_suppliers.groupBy(
        col("country")
    ).agg(
        spark_sum("total_price").alias("total_revenue"),
        spark_sum("quantity").alias("total_quantity"),
        count("*").alias("sales_count"),
        countDistinct("supplier_key").alias("supplier_count")
    )
    
    report_df = sales_suppliers.groupBy(
        col("supplier_key"),
        col("supplier_name"),
        col("country")
    ).agg(
        spark_sum("total_price").alias("total_revenue"),
        spark_sum("quantity").alias("total_quantity"),
        count("*").alias("sales_count"),
        avg(col("price")).alias("avg_product_price")
    ).select(
        col("supplier_key"),
        col("supplier_name"),
        col("country"),
        col("total_revenue"),
        col("total_quantity"),
        col("sales_count"),
        col("avg_product_price")
    )
    
    inserted = write_report_to_clickhouse("pet_shop_analytics.report_suppliers_sales", report_df)
    print(f"Создано {inserted} записей в report_suppliers_sales")
    return report_df

def create_report_6_quality(spark, data):
    print("\nСоздание витрины качества продукции...")
    
    sales_quality = data["fact_sales"].join(
        data["dimension_products"],
        "product_key",
        "inner"
    )
    
    products_by_rating = data["dimension_products"].groupBy(
        col("product_name")
    ).agg(
        avg("rating").alias("avg_rating"),
        spark_sum("reviews_count").alias("total_reviews"),
        count("*").alias("variants_count")
    )
    
    rating_sales_correlation = sales_quality.groupBy(
        col("product_name"),
        col("rating")
    ).agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        spark_sum("total_price").alias("total_revenue"),
        count("*").alias("sales_count"),
        avg("rating").alias("avg_rating")
    )
    
    products_by_reviews = data["dimension_products"].groupBy(
        col("product_name")
    ).agg(
        spark_sum("reviews_count").alias("total_reviews"),
        avg("rating").alias("avg_rating"),
        count("*").alias("variants_count")
    ).orderBy(desc("total_reviews"))
    
    report_df = sales_quality.groupBy(
        col("product_name"),
        col("rating"),
        col("reviews_count")
    ).agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        spark_sum("total_price").alias("total_revenue"),
        count("*").alias("sales_count"),
        avg("rating").alias("avg_rating")
    ).select(
        col("product_name"),
        col("rating"),
        col("reviews_count"),
        col("total_quantity_sold"),
        col("total_revenue"),
        col("sales_count"),
        col("avg_rating")
    )
    
    inserted = write_report_to_clickhouse("pet_shop_analytics.report_product_quality", report_df)
    print(f"Создано {inserted} записей в report_product_quality")
    return report_df

def main():
    spark = create_spark_session()
    
    try:
        print("=" * 60)
        print("Создание отчетов в ClickHouse")
        print("=" * 60)
        
        create_clickhouse_tables(spark)
        
        data = read_star_schema(spark)
        
        create_report_1_products(spark, data)
        create_report_2_customers(spark, data)
        create_report_3_time(spark, data)
        create_report_4_stores(spark, data)
        create_report_5_suppliers(spark, data)
        create_report_6_quality(spark, data)
        
        print("\n" + "=" * 60)
        print("Все отчеты созданы в ClickHouse")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

