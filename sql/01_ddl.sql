-- === --
-- DDL --
-- === --

-- удаляем дубли
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dimension_products CASCADE;
DROP TABLE IF EXISTS dimension_product_categories CASCADE;
DROP TABLE IF EXISTS dimension_suppliers CASCADE;
DROP TABLE IF EXISTS dimension_stores CASCADE;
DROP TABLE IF EXISTS dimension_store_locations CASCADE;
DROP TABLE IF EXISTS dimension_sellers CASCADE;
DROP TABLE IF EXISTS dimension_customers CASCADE;
DROP TABLE IF EXISTS dimension_pets CASCADE;
DROP TABLE IF EXISTS dimension_pet_breeds CASCADE;
DROP TABLE IF EXISTS dimension_dates CASCADE;




-- измерение (породы)
CREATE TABLE dimension_pet_breeds (
    breed_key SERIAL PRIMARY KEY,
    breed_name VARCHAR(100) NOT NULL UNIQUE,
    pet_type VARCHAR(50) NOT NULL
);


-- измерение (питомцы)
CREATE TABLE dimension_pets (
    pet_key SERIAL PRIMARY KEY,
    pet_name VARCHAR(100),
    breed_key INTEGER REFERENCES dimension_pet_breeds(breed_key),
    pet_type VARCHAR(50) NOT NULL
);

-- измерение (покупатели)
CREATE TABLE dimension_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    email VARCHAR(200) NOT NULL UNIQUE,
    country VARCHAR(100),
    postal_code VARCHAR(50),
    pet_key INTEGER REFERENCES dimension_pets(pet_key),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- измерение (продавцы)
CREATE TABLE dimension_sellers (
    seller_key SERIAL PRIMARY KEY,
    seller_id INTEGER NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200) NOT NULL UNIQUE,
    country VARCHAR(100),
    postal_code VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- измерение (локации магазов)
CREATE TABLE dimension_store_locations (
    location_key SERIAL PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100) NOT NULL,
    UNIQUE(city, state, country)
);

-- измерение (магазины)
CREATE TABLE dimension_stores (
    store_key SERIAL PRIMARY KEY,
    store_name VARCHAR(200) NOT NULL UNIQUE,
    location VARCHAR(200),
    location_key INTEGER REFERENCES dimension_store_locations(location_key),
    phone VARCHAR(50),
    email VARCHAR(200)
);

-- измерение (категории товаров)
CREATE TABLE dimension_product_categories (
    category_key SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    pet_category VARCHAR(100)
);

-- измерение (поставщики)
CREATE TABLE dimension_suppliers (
    supplier_key SERIAL PRIMARY KEY,
    supplier_name VARCHAR(200) NOT NULL,
    contact_name VARCHAR(200),
    email VARCHAR(200) NOT NULL UNIQUE,
    phone VARCHAR(50),
    address VARCHAR(200),
    city VARCHAR(100),
    country VARCHAR(100)
);

-- измерение (тоывары)
CREATE TABLE dimension_products (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    category_key INTEGER REFERENCES dimension_product_categories(category_key),
    supplier_key INTEGER REFERENCES dimension_suppliers(supplier_key),
    price NUMERIC(10,2),
    weight NUMERIC(10,2),
    color VARCHAR(50),
    size VARCHAR(50),
    brand VARCHAR(100),
    material VARCHAR(100),
    description TEXT,
    rating NUMERIC(3,1),
    reviews_count INTEGER,
    release_date DATE,
    expiry_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_name, color, size, price)
);

-- измерение (давт)
CREATE TABLE dimension_dates (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- факты (продажи)
CREATE TABLE fact_sales (
    sale_key SERIAL PRIMARY KEY,
    date_key INTEGER REFERENCES dimension_dates(date_key),
    customer_key INTEGER REFERENCES dimension_customers(customer_key),
    seller_key INTEGER REFERENCES dimension_sellers(seller_key),
    product_key INTEGER REFERENCES dimension_products(product_key),
    store_key INTEGER REFERENCES dimension_stores(store_key),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    total_price NUMERIC(10,2) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- индексы
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_seller ON fact_sales(seller_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_key);
CREATE INDEX idx_customers_country ON dimension_customers(country);
CREATE INDEX idx_sellers_country ON dimension_sellers(country);
CREATE INDEX idx_products_category ON dimension_products(category_key);
CREATE INDEX idx_stores_location ON dimension_stores(location_key);


COMMENT ON TABLE fact_sales IS 'Таблица фактов продаж товаров для домашних питомцев';
COMMENT ON TABLE dimension_customers IS 'Измерение покупателей';
COMMENT ON TABLE dimension_sellers IS 'Измерение продавцов';
COMMENT ON TABLE dimension_products IS 'Измерение товаров';
COMMENT ON TABLE dimension_stores IS 'Измерение магазинов';
COMMENT ON TABLE dimension_suppliers IS 'Измерение поставщиков';
COMMENT ON TABLE dimension_pets IS 'Измерение питомцев покупателей';
COMMENT ON TABLE dimension_pet_breeds IS 'Справочник пород питомцев';
COMMENT ON TABLE dimension_product_categories IS 'Справочник категорий товаров';
COMMENT ON TABLE dimension_store_locations IS 'Справочник локаций магазинов';
COMMENT ON TABLE dimension_dates IS 'Измерение дат';

COMMENT ON COLUMN fact_sales.quantity IS 'Количество проданных единиц товара';
COMMENT ON COLUMN fact_sales.unit_price IS 'Цена за единицу товара';
COMMENT ON COLUMN fact_sales.total_price IS 'Общая сумма продажи';