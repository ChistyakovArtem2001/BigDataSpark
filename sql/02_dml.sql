-- === --
-- DML --
-- === --

-- очистка таб
TRUNCATE TABLE fact_sales CASCADE;
TRUNCATE TABLE dimension_products CASCADE;
TRUNCATE TABLE dimension_customers CASCADE;
TRUNCATE TABLE dimension_sellers CASCADE;
TRUNCATE TABLE dimension_stores CASCADE;
TRUNCATE TABLE dimension_store_locations CASCADE;
TRUNCATE TABLE dimension_suppliers CASCADE;
TRUNCATE TABLE dimension_product_categories CASCADE;
TRUNCATE TABLE dimension_pets CASCADE;
TRUNCATE TABLE dimension_pet_breeds CASCADE;
TRUNCATE TABLE dimension_dates CASCADE;

-- заполняем данные породы
INSERT INTO dimension_pet_breeds (breed_name, pet_type)
SELECT DISTINCT 
    TRIM(customer_pet_breed),
    TRIM(customer_pet_type)
FROM staging_mock_data
WHERE customer_pet_breed IS NOT NULL 
  AND TRIM(customer_pet_breed) != ''
  AND customer_pet_type IS NOT NULL
ON CONFLICT (breed_name) DO NOTHING;

-- заполняем данные животных
INSERT INTO dimension_pets (pet_name, breed_key, pet_type)
SELECT DISTINCT 
    TRIM(s.customer_pet_name),
    b.breed_key,
    TRIM(s.customer_pet_type)
FROM staging_mock_data s
LEFT JOIN dimension_pet_breeds b ON b.breed_name = TRIM(s.customer_pet_breed)
WHERE s.customer_pet_name IS NOT NULL 
  AND TRIM(s.customer_pet_name) != '';

-- заполняем данные покупателей
INSERT INTO dimension_customers (
    customer_id, first_name, last_name, age, email, 
    country, postal_code, pet_key
)
SELECT DISTINCT ON (TRIM(LOWER(s.customer_email)))
    s.sale_customer_id,
    TRIM(s.customer_first_name),
    TRIM(s.customer_last_name),
    s.customer_age,
    TRIM(LOWER(s.customer_email)),
    TRIM(s.customer_country),
    TRIM(s.customer_postal_code),
    p.pet_key
FROM staging_mock_data s
LEFT JOIN dimension_pets p ON TRIM(p.pet_name) = TRIM(s.customer_pet_name)
WHERE s.customer_email IS NOT NULL
  AND TRIM(s.customer_email) != ''
ORDER BY TRIM(LOWER(s.customer_email)), s.id;

-- заполняем данные продавцов
INSERT INTO dimension_sellers (
    seller_id, first_name, last_name, email, 
    country, postal_code
)
SELECT DISTINCT ON (TRIM(LOWER(s.seller_email)))
    s.sale_seller_id,
    TRIM(s.seller_first_name),
    TRIM(s.seller_last_name),
    TRIM(LOWER(s.seller_email)),
    TRIM(s.seller_country),
    TRIM(s.seller_postal_code)
FROM staging_mock_data s
WHERE s.seller_email IS NOT NULL
  AND TRIM(s.seller_email) != ''
ORDER BY TRIM(LOWER(s.seller_email)), s.id;

-- заполняем данные рас-е магазинов
INSERT INTO dimension_store_locations (city, state, country)
SELECT DISTINCT 
    COALESCE(TRIM(store_city), 'Unknown'),
    TRIM(store_state),
    COALESCE(TRIM(store_country), 'Unknown')
FROM staging_mock_data
WHERE store_country IS NOT NULL
  AND TRIM(store_country) != ''
ON CONFLICT (city, state, country) DO NOTHING;

-- заполняем данные магазинов
INSERT INTO dimension_stores (store_name, location, location_key, phone, email)
SELECT DISTINCT ON (TRIM(s.store_name))
    TRIM(s.store_name),
    TRIM(s.store_location),
    l.location_key,
    TRIM(s.store_phone),
    TRIM(s.store_email)
FROM staging_mock_data s
LEFT JOIN dimension_store_locations l ON 
    l.city = COALESCE(TRIM(s.store_city), 'Unknown') AND
    COALESCE(TRIM(l.state), '') = COALESCE(TRIM(s.store_state), '') AND
    l.country = COALESCE(TRIM(s.store_country), 'Unknown')
WHERE s.store_name IS NOT NULL
  AND TRIM(s.store_name) != ''
ORDER BY TRIM(s.store_name), s.id;

-- заполняем данные категории товаров
INSERT INTO dimension_product_categories (category_name, pet_category)
SELECT DISTINCT 
    TRIM(product_category),
    TRIM(pet_category)
FROM staging_mock_data
WHERE product_category IS NOT NULL
  AND TRIM(product_category) != ''
ON CONFLICT (category_name) DO NOTHING;

-- заполняем данные поставщикоы
INSERT INTO dimension_suppliers (
    supplier_name, contact_name, email, phone, 
    address, city, country
)
SELECT DISTINCT ON (TRIM(LOWER(s.supplier_email)))
    TRIM(s.supplier_name),
    TRIM(s.supplier_contact),
    TRIM(LOWER(s.supplier_email)),
    TRIM(s.supplier_phone),
    TRIM(s.supplier_address),
    TRIM(s.supplier_city),
    TRIM(s.supplier_country)
FROM staging_mock_data s
WHERE s.supplier_email IS NOT NULL
  AND TRIM(s.supplier_email) != ''
ORDER BY TRIM(LOWER(s.supplier_email)), s.id;

-- заполняем данные товаров
WITH unique_products AS (
    SELECT DISTINCT ON (
        TRIM(product_name),
        NULLIF(TRIM(product_color), ''),
        NULLIF(TRIM(product_size), ''),
        product_price
    )
        sale_product_id,
        TRIM(product_name) as product_name,
        product_category,
        supplier_email,
        product_price,
        product_weight,
        NULLIF(TRIM(product_color), '') as product_color,
        NULLIF(TRIM(product_size), '') as product_size,
        TRIM(product_brand) as product_brand,
        TRIM(product_material) as product_material,
        product_description,
        product_rating,
        product_reviews,
        product_release_date,
        product_expiry_date,
        id
    FROM staging_mock_data
    WHERE product_name IS NOT NULL
      AND TRIM(product_name) != ''
    ORDER BY 
        TRIM(product_name),
        NULLIF(TRIM(product_color), ''),
        NULLIF(TRIM(product_size), ''),
        product_price,
        id
)
INSERT INTO dimension_products (
    product_id, product_name, category_key, supplier_key,
    price, weight, color, size, brand, material,
    description, rating, reviews_count, 
    release_date, expiry_date
)
SELECT 
    up.sale_product_id,
    up.product_name,
    c.category_key,
    sup.supplier_key,
    up.product_price,
    up.product_weight,
    up.product_color,
    up.product_size,
    up.product_brand,
    up.product_material,
    up.product_description,
    up.product_rating,
    up.product_reviews,
    TO_DATE(up.product_release_date, 'MM/DD/YYYY'),
    TO_DATE(up.product_expiry_date, 'MM/DD/YYYY')
FROM unique_products up
LEFT JOIN dimension_product_categories c ON c.category_name = TRIM(up.product_category)
LEFT JOIN dimension_suppliers sup ON sup.email = TRIM(LOWER(up.supplier_email));

-- заполняем данные дат
WITH date_range AS (
    SELECT generate_series(
        '2010-01-01'::date,
        '2030-12-31'::date,
        '1 day'::interval
    )::date AS full_date
)
INSERT INTO dimension_dates (
    date_key, full_date, day, month, year, quarter,
    day_of_week, day_name, month_name, is_weekend
)
SELECT 
    TO_CHAR(full_date, 'YYYYMMDD')::INTEGER,
    full_date,
    EXTRACT(DAY FROM full_date)::INTEGER,
    EXTRACT(MONTH FROM full_date)::INTEGER,
    EXTRACT(YEAR FROM full_date)::INTEGER,
    EXTRACT(QUARTER FROM full_date)::INTEGER,
    EXTRACT(ISODOW FROM full_date)::INTEGER,
    TO_CHAR(full_date, 'Day'),
    TO_CHAR(full_date, 'Month'),
    CASE WHEN EXTRACT(ISODOW FROM full_date) IN (6, 7) 
         THEN TRUE ELSE FALSE END
FROM date_range
ON CONFLICT (full_date) DO NOTHING;

-- заполняем данные продаж
INSERT INTO fact_sales (
    date_key, customer_key, seller_key, 
    product_key, store_key,
    quantity, unit_price, total_price
)
SELECT 
    TO_CHAR(TO_DATE(s.sale_date, 'MM/DD/YYYY'), 'YYYYMMDD')::INTEGER,
    c.customer_key,
    sel.seller_key,
    p.product_key,
    st.store_key,
    s.sale_quantity,
    s.product_price,
    s.sale_total_price
FROM staging_mock_data s
INNER JOIN dimension_customers c 
    ON c.email = TRIM(LOWER(s.customer_email))
INNER JOIN dimension_sellers sel 
    ON sel.email = TRIM(LOWER(s.seller_email))
INNER JOIN dimension_products p 
    ON p.product_name = TRIM(s.product_name)
    AND p.color IS NOT DISTINCT FROM NULLIF(TRIM(s.product_color), '')
    AND p.size IS NOT DISTINCT FROM NULLIF(TRIM(s.product_size), '')
    AND p.price = s.product_price
INNER JOIN dimension_stores st 
    ON st.store_name = TRIM(s.store_name)
WHERE s.sale_date IS NOT NULL
    AND s.sale_quantity IS NOT NULL
    AND s.sale_total_price IS NOT NULL;


-- проверка
SELECT 
    'staging_mock_data' AS table_name, 
    COUNT(*) as records,
    'Исходные данные из CSV' as description
FROM staging_mock_data
UNION ALL 
SELECT 'fact_sales', COUNT(*), 'Транзакции продаж' 
FROM fact_sales
UNION ALL 
SELECT 'dimension_customers', COUNT(*), 'Уникальные покупатели' 
FROM dimension_customers
UNION ALL 
SELECT 'dimension_sellers', COUNT(*), 'Уникальные продавцы' 
FROM dimension_sellers
UNION ALL 
SELECT 'dimension_products', COUNT(*), 'Варианты товаров' 
FROM dimension_products
UNION ALL 
SELECT 'dimension_stores', COUNT(*), 'Уникальные магазины' 
FROM dimension_stores
UNION ALL 
SELECT 'dimension_suppliers', COUNT(*), 'Уникальные поставщики' 
FROM dimension_suppliers
UNION ALL 
SELECT 'dimension_pet_breeds', COUNT(*), 'Породы питомцев' 
FROM dimension_pet_breeds
UNION ALL 
SELECT 'dimension_product_categories', COUNT(*), 'Категории товаров' 
FROM dimension_product_categories
UNION ALL 
SELECT 'dimension_dates', COUNT(*), 'Календарь' 
FROM dimension_dates
ORDER BY table_name;