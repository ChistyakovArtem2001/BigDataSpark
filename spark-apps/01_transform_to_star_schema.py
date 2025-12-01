from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, coalesce, when, lit, to_date,
    date_format, year, month, dayofmonth, quarter,
    dayofweek, date_format as df_format, regexp_replace,
    first
)
from pyspark.sql.types import IntegerType, DateType
import sys
import psycopg2
import os

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

def create_spark_session():
    if 'SPARK_HOME' not in os.environ:
        os.environ['SPARK_HOME'] = '/opt/spark'
    if 'JAVA_HOME' not in os.environ:
        os.environ['JAVA_HOME'] = '/usr/lib/jvm/default-java'
    os.environ.setdefault('SPARK_CLASSPATH', '/opt/spark/jars/*:/opt/spark/external-jars/*')
    
    builder = SparkSession.builder \
        .appName("TransformToStarSchema") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/*:/opt/spark/external-jars/*") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/*:/opt/spark/external-jars/*") \
        .master("local[*]")
    
    builder = configure_spark_jars(builder, ["postgresql-42.7.1.jar"], "org.postgresql:postgresql:42.7.1")
    return builder.getOrCreate()

def clear_target_tables():
    print("\nОчистка таблиц в PostgreSQL...")
    statements = [
        "TRUNCATE TABLE fact_sales RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dimension_products RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dimension_customers RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dimension_sellers RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dimension_stores RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dimension_store_locations RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dimension_suppliers RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dimension_product_categories RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dimension_pets RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dimension_pet_breeds RESTART IDENTITY CASCADE",
        "TRUNCATE TABLE dimension_dates RESTART IDENTITY CASCADE"
    ]
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    try:
        with conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
        print("Очистка завершена")
    finally:
        conn.close()

def read_staging_data(spark):
    print("Чтение данных из staging_mock_data...")
    df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="staging_mock_data",
        properties=POSTGRES_PROPS
    )
    print(f"Загружено {df.count()} записей")
    return df

def transform_pet_breeds(spark, staging_df):
    print("Трансформация dimension_pet_breeds...")
    
    breeds_df = staging_df.select(
        trim(col("customer_pet_breed")).alias("breed_name"),
        trim(col("customer_pet_type")).alias("pet_type")
    ).filter(
        col("customer_pet_breed").isNotNull() &
        (trim(col("customer_pet_breed")) != "")
    ).groupBy("breed_name").agg(
        first("pet_type", ignorenulls=True).alias("pet_type")
    )
    
    breeds_df.write.jdbc(
        url=POSTGRES_URL,
        table="dimension_pet_breeds",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {breeds_df.count()} пород")
    return breeds_df

def transform_pets(spark, staging_df, breeds_df):
    print("Трансформация dimension_pets...")
    
    existing_breeds = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_pet_breeds",
        properties=POSTGRES_PROPS
    )
    
    pets_df = staging_df.select(
        trim(col("customer_pet_name")).alias("pet_name"),
        trim(col("customer_pet_breed")).alias("breed_name"),
        trim(col("customer_pet_type")).alias("pet_pet_type")
    ).filter(
        col("customer_pet_name").isNotNull() &
        (trim(col("customer_pet_name")) != "")
    ).distinct()
    
    pets_with_breeds = pets_df.join(
        existing_breeds.alias("breeds"),
        pets_df["breed_name"] == col("breeds.breed_name"),
        "left"
    ).select(
        col("pet_name"),
        col("breed_key"),
        col("pet_pet_type").alias("pet_type")
    )
    
    pets_with_breeds.write.jdbc(
        url=POSTGRES_URL,
        table="dimension_pets",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {pets_with_breeds.count()} питомцев")
    return pets_with_breeds

def transform_customers(spark, staging_df):
    print("Трансформация dimension_customers...")
    
    existing_pets = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_pets",
        properties=POSTGRES_PROPS
    )
    
    customers_df = staging_df.select(
        col("sale_customer_id").alias("customer_id"),
        trim(col("customer_first_name")).alias("first_name"),
        trim(col("customer_last_name")).alias("last_name"),
        col("customer_age").alias("age"),
        trim(lower(col("customer_email"))).alias("email"),
        trim(col("customer_country")).alias("country"),
        trim(col("customer_postal_code")).alias("postal_code"),
        trim(col("customer_pet_name")).alias("pet_name")
    ).filter(
        col("customer_email").isNotNull() &
        (trim(col("customer_email")) != "")
    )
    
    customers_with_pets = customers_df.join(
        existing_pets,
        customers_df["pet_name"] == existing_pets["pet_name"],
        "left"
    ).select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("age"),
        col("email"),
        col("country"),
        col("postal_code"),
        col("pet_key")
    ).dropDuplicates(["email"])
    
    customers_with_pets.write.jdbc(
        url=POSTGRES_URL,
        table="dimension_customers",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {customers_with_pets.count()} покупателей")
    return customers_with_pets

def transform_sellers(spark, staging_df):
    print("Трансформация dimension_sellers...")
    
    sellers_df = staging_df.select(
        col("sale_seller_id").alias("seller_id"),
        trim(col("seller_first_name")).alias("first_name"),
        trim(col("seller_last_name")).alias("last_name"),
        trim(lower(col("seller_email"))).alias("email"),
        trim(col("seller_country")).alias("country"),
        trim(col("seller_postal_code")).alias("postal_code")
    ).filter(
        col("seller_email").isNotNull() &
        (trim(col("seller_email")) != "")
    ).dropDuplicates(["email"])
    
    sellers_df.write.jdbc(
        url=POSTGRES_URL,
        table="dimension_sellers",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {sellers_df.count()} продавцов")
    return sellers_df

def transform_store_locations(spark, staging_df):
    print("Трансформация dimension_store_locations...")
    
    locations_df = staging_df.select(
        coalesce(trim(col("store_city")), lit("Unknown")).alias("city"),
        trim(col("store_state")).alias("state"),
        coalesce(trim(col("store_country")), lit("Unknown")).alias("country")
    ).filter(
        col("store_country").isNotNull() &
        (trim(col("store_country")) != "")
    ).dropDuplicates(["city", "state", "country"])
    
    locations_df.write.jdbc(
        url=POSTGRES_URL,
        table="dimension_store_locations",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {locations_df.count()} локаций")
    return locations_df

def transform_stores(spark, staging_df):
    print("Трансформация dimension_stores...")
    
    existing_locations = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_store_locations",
        properties=POSTGRES_PROPS
    )
    
    stores_df = staging_df.select(
        trim(col("store_name")).alias("store_name"),
        trim(col("store_location")).alias("location"),
        coalesce(trim(col("store_city")), lit("Unknown")).alias("city"),
        trim(col("store_state")).alias("state"),
        coalesce(trim(col("store_country")), lit("Unknown")).alias("country"),
        trim(col("store_phone")).alias("phone"),
        trim(col("store_email")).alias("email")
    ).filter(
        col("store_name").isNotNull() &
        (trim(col("store_name")) != "")
    )
    
    stores_with_locations = stores_df.join(
        existing_locations,
        (stores_df["city"] == existing_locations["city"]) &
        (coalesce(stores_df["state"], lit("")) == coalesce(existing_locations["state"], lit(""))) &
        (stores_df["country"] == existing_locations["country"]),
        "left"
    ).select(
        col("store_name"),
        col("location"),
        col("location_key"),
        col("phone"),
        col("email")
    ).dropDuplicates(["store_name"])
    
    stores_with_locations.write.jdbc(
        url=POSTGRES_URL,
        table="dimension_stores",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {stores_with_locations.count()} магазинов")
    return stores_with_locations

def transform_product_categories(spark, staging_df):
    print("Трансформация dimension_product_categories...")
    
    categories_df = staging_df.select(
        trim(col("product_category")).alias("category_name"),
        trim(col("pet_category")).alias("pet_category")
    ).filter(
        col("product_category").isNotNull() &
        (trim(col("product_category")) != "")
    ).dropDuplicates(["category_name"])
    
    categories_df.write.jdbc(
        url=POSTGRES_URL,
        table="dimension_product_categories",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {categories_df.count()} категорий")
    return categories_df

def transform_suppliers(spark, staging_df):
    print("Трансформация dimension_suppliers...")
    
    suppliers_df = staging_df.select(
        trim(col("supplier_name")).alias("supplier_name"),
        trim(col("supplier_contact")).alias("contact_name"),
        trim(lower(col("supplier_email"))).alias("email"),
        trim(col("supplier_phone")).alias("phone"),
        trim(col("supplier_address")).alias("address"),
        trim(col("supplier_city")).alias("city"),
        trim(col("supplier_country")).alias("country")
    ).filter(
        col("supplier_email").isNotNull() &
        (trim(col("supplier_email")) != "")
    ).dropDuplicates(["email"])
    
    suppliers_df.write.jdbc(
        url=POSTGRES_URL,
        table="dimension_suppliers",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {suppliers_df.count()} поставщиков")
    return suppliers_df

def transform_products(spark, staging_df):
    print("Трансформация dimension_products...")
    
    existing_categories = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_product_categories",
        properties=POSTGRES_PROPS
    )
    
    existing_suppliers = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_suppliers",
        properties=POSTGRES_PROPS
    )
    
    products_df = staging_df.select(
        col("sale_product_id").alias("product_id"),
        trim(col("product_name")).alias("product_name"),
        trim(col("product_category")).alias("category_name"),
        trim(lower(col("supplier_email"))).alias("supplier_email"),
        col("product_price").alias("price"),
        col("product_weight").alias("weight"),
        when(trim(col("product_color")) == "", None).otherwise(trim(col("product_color"))).alias("color"),
        when(trim(col("product_size")) == "", None).otherwise(trim(col("product_size"))).alias("size"),
        trim(col("product_brand")).alias("brand"),
        trim(col("product_material")).alias("material"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews_count"),
        col("product_release_date").alias("release_date_str"),
        col("product_expiry_date").alias("expiry_date_str")
    ).filter(
        col("product_name").isNotNull() &
        (trim(col("product_name")) != "")
    )
    
    products_df = products_df.withColumn(
        "release_date",
        to_date(col("release_date_str"), "M/d/yyyy")
    ).withColumn(
        "expiry_date",
        to_date(col("expiry_date_str"), "M/d/yyyy")
    )
    
    products_with_refs = products_df.join(
        existing_categories,
        products_df["category_name"] == existing_categories["category_name"],
        "left"
    ).join(
        existing_suppliers,
        products_df["supplier_email"] == existing_suppliers["email"],
        "left"
    ).select(
        col("product_id"),
        col("product_name"),
        col("category_key"),
        col("supplier_key"),
        col("price"),
        col("weight"),
        col("color"),
        col("size"),
        col("brand"),
        col("material"),
        col("description"),
        col("rating"),
        col("reviews_count"),
        col("release_date"),
        col("expiry_date")
    ).dropDuplicates(["product_name", "color", "size", "price"])
    
    products_with_refs.write.jdbc(
        url=POSTGRES_URL,
        table="dimension_products",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {products_with_refs.count()} товаров")
    return products_with_refs

def transform_dates(spark):
    print("Трансформация dimension_dates...")
    
    from datetime import datetime, timedelta
    
    dates = []
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2030, 12, 31)
    current_date = start_date
    
    while current_date <= end_date:
        dates.append({
            "date_key": int(current_date.strftime("%Y%m%d")),
            "full_date": current_date.date(),
            "day": current_date.day,
            "month": current_date.month,
            "year": current_date.year,
            "quarter": (current_date.month - 1) // 3 + 1,
            "day_of_week": current_date.isoweekday(),
            "day_name": current_date.strftime("%A"),
            "month_name": current_date.strftime("%B"),
            "is_weekend": current_date.weekday() >= 5
        })
        current_date += timedelta(days=1)
    
    dates_df = spark.createDataFrame(dates)
    
    dates_df.write.jdbc(
        url=POSTGRES_URL,
        table="dimension_dates",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {dates_df.count()} дат")
    return dates_df

def transform_fact_sales(spark, staging_df):
    print("Трансформация fact_sales...")
    
    customers = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_customers",
        properties=POSTGRES_PROPS
    )
    
    sellers = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_sellers",
        properties=POSTGRES_PROPS
    )
    
    products = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_products",
        properties=POSTGRES_PROPS
    )
    
    stores = spark.read.jdbc(
        url=POSTGRES_URL,
        table="dimension_stores",
        properties=POSTGRES_PROPS
    )
    
    sales_df = staging_df.select(
        trim(lower(col("customer_email"))).alias("customer_email"),
        trim(lower(col("seller_email"))).alias("seller_email"),
        trim(col("product_name")).alias("product_name"),
        when(trim(col("product_color")) == "", None).otherwise(trim(col("product_color"))).alias("product_color"),
        when(trim(col("product_size")) == "", None).otherwise(trim(col("product_size"))).alias("product_size"),
        col("product_price"),
        trim(col("store_name")).alias("store_name"),
        col("sale_date").alias("sale_date_str"),
        col("sale_quantity").alias("quantity"),
        col("product_price").alias("unit_price"),
        col("sale_total_price").alias("total_price")
    ).filter(
        col("sale_date").isNotNull() &
        col("sale_quantity").isNotNull() &
        col("sale_total_price").isNotNull()
    )
    
    sales_df = sales_df.withColumn(
        "sale_date",
        to_date(col("sale_date_str"), "M/d/yyyy")
    ).withColumn(
        "date_key",
        date_format(col("sale_date"), "yyyyMMdd").cast(IntegerType())
    )
    
    sales_with_dimensions = sales_df.join(
        customers,
        sales_df["customer_email"] == customers["email"],
        "inner"
    ).join(
        sellers,
        sales_df["seller_email"] == sellers["email"],
        "inner"
    ).join(
        products,
        (sales_df["product_name"] == products["product_name"]) &
        (coalesce(sales_df["product_color"], lit("")) == coalesce(products["color"], lit(""))) &
        (coalesce(sales_df["product_size"], lit("")) == coalesce(products["size"], lit(""))) &
        (sales_df["product_price"] == products["price"]),
        "inner"
    ).join(
        stores,
        sales_df["store_name"] == stores["store_name"],
        "inner"
    ).select(
        col("date_key"),
        col("customer_key"),
        col("seller_key"),
        col("product_key"),
        col("store_key"),
        col("quantity"),
        col("unit_price"),
        col("total_price")
    )
    
    sales_with_dimensions.write.jdbc(
        url=POSTGRES_URL,
        table="fact_sales",
        mode="append",
        properties=POSTGRES_PROPS
    )
    
    print(f"Добавлено {sales_with_dimensions.count()} продаж")
    return sales_with_dimensions

def main():
    spark = create_spark_session()
    
    try:
        print("Начало трансформация данных в модель звезда")
 
        
        staging_df = read_staging_data(spark)
        
        clear_target_tables()
        
        transform_pet_breeds(spark, staging_df)
        transform_pets(spark, staging_df, None)
        transform_store_locations(spark, staging_df)
        transform_product_categories(spark, staging_df)
        transform_suppliers(spark, staging_df)
        transform_dates(spark)
        transform_customers(spark, staging_df)
        transform_sellers(spark, staging_df)
        transform_stores(spark, staging_df)
        transform_products(spark, staging_df)
        
        transform_fact_sales(spark, staging_df)
        
        print("\n" + "=" * 60)
        print("Трансформация завершена")
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

