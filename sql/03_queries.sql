-- аналитические запросы


-- статистика продаж
SELECT 
    COUNT(*) AS total_sales,
    SUM(quantity) AS total_items_sold,
    SUM(total_price) AS total_revenue,
    AVG(total_price) AS avg_sale_amount
FROM fact_sales;

-- продажи по категориям товаров
SELECT 
    pc.category_name,
    pc.pet_category,
    COUNT(fs.sale_key) AS sales_count,
    SUM(fs.total_price) AS total_revenue
FROM fact_sales fs
JOIN dimension_products p ON fs.product_key = p.product_key
JOIN dimension_product_categories pc ON p.category_key = pc.category_key
GROUP BY pc.category_name, pc.pet_category
ORDER BY total_revenue DESC;

-- топ-10 покупателей
SELECT 
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    c.country,
    COUNT(fs.sale_key) AS purchase_count,
    SUM(fs.total_price) AS total_spent
FROM fact_sales fs
JOIN dimension_customers c ON fs.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name, c.email, c.country
ORDER BY total_spent DESC
LIMIT 10;

-- топ-10 продавцов
SELECT 
    s.first_name || ' ' || s.last_name AS seller_name,
    s.country,
    COUNT(fs.sale_key) AS sales_count,
    SUM(fs.total_price) AS total_revenue
FROM fact_sales fs
JOIN dimension_sellers s ON fs.seller_key = s.seller_key
GROUP BY s.seller_key, s.first_name, s.last_name, s.country
ORDER BY total_revenue DESC
LIMIT 10;

-- самые популярные товары
SELECT 
    p.product_name,
    pc.category_name,
    p.brand,
    COUNT(fs.sale_key) AS times_sold,
    SUM(fs.total_price) AS total_revenue
FROM fact_sales fs
JOIN dimension_products p ON fs.product_key = p.product_key
JOIN dimension_product_categories pc ON p.category_key = pc.category_key
GROUP BY p.product_key, p.product_name, pc.category_name, p.brand
ORDER BY times_sold DESC
LIMIT 20;

-- продажи по магазинам
SELECT 
    st.store_name,
    l.city,
    l.country,
    COUNT(fs.sale_key) AS sales_count,
    SUM(fs.total_price) AS total_revenue
FROM fact_sales fs
JOIN dimension_stores st ON fs.store_key = st.store_key
JOIN dimension_store_locations l ON st.location_key = l.location_key
GROUP BY st.store_name, l.city, l.country
ORDER BY total_revenue DESC;

-- продажи по месяцам
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(fs.sale_key) AS sales_count,
    SUM(fs.total_price) AS total_revenue
FROM fact_sales fs
JOIN dimension_dates d ON fs.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- анализ по странам покупателей
SELECT 
    c.country,
    COUNT(DISTINCT c.customer_key) AS customer_count,
    COUNT(fs.sale_key) AS sales_count,
    SUM(fs.total_price) AS total_revenue
FROM fact_sales fs
JOIN dimension_customers c ON fs.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_revenue DESC;

-- породы питомцев покупателей
SELECT 
    pb.breed_name,
    pb.pet_type,
    COUNT(DISTINCT c.customer_key) AS customer_count,
    COUNT(fs.sale_key) AS purchases_count,
    SUM(fs.total_price) AS total_spent
FROM dimension_customers c
JOIN dimension_pets p ON c.pet_key = p.pet_key
JOIN dimension_pet_breeds pb ON p.breed_key = pb.breed_key
LEFT JOIN fact_sales fs ON c.customer_key = fs.customer_key
GROUP BY pb.breed_name, pb.pet_type
ORDER BY customer_count DESC;

-- avg возраст покупателей по категориям
SELECT 
    pc.category_name,
    AVG(c.age) AS avg_customer_age,
    COUNT(DISTINCT c.customer_key) AS customer_count,
    SUM(fs.total_price) AS total_revenue
FROM fact_sales fs
JOIN dimension_customers c ON fs.customer_key = c.customer_key
JOIN dimension_products p ON fs.product_key = p.product_key
JOIN dimension_product_categories pc ON p.category_key = pc.category_key
GROUP BY pc.category_name
ORDER BY total_revenue DESC;