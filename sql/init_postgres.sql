-- =========================================================
-- 0. ШИРОКАЯ ТАБЛИЦА
-- =========================================================
DROP TABLE IF EXISTS mock_data CASCADE;

CREATE TABLE mock_data (
    source_file TEXT NOT NULL,
    id INT,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INT,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(10,2),
    product_quantity INT,
    sale_date TEXT,
    sale_customer_id INT,
    sale_seller_id INT,
    sale_product_id INT,
    sale_quantity INT,
    sale_total_price NUMERIC(12,2),
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight NUMERIC(10,2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(3,1),
    product_reviews INT,
    product_release_date TEXT,
    product_expiry_date TEXT,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

-- Импорт 10 файлов с подстановкой имени файла
COPY mock_data (source_file, id, customer_first_name, customer_last_name, customer_age,
                customer_email, customer_country, customer_postal_code, customer_pet_type,
                customer_pet_name, customer_pet_breed, seller_first_name, seller_last_name,
                seller_email, seller_country, seller_postal_code, product_name, product_category,
                product_price, product_quantity, sale_date, sale_customer_id, sale_seller_id,
                sale_product_id, sale_quantity, sale_total_price, store_name, store_location,
                store_city, store_state, store_country, store_phone, store_email, pet_category,
                product_weight, product_color, product_size, product_brand, product_material,
                product_description, product_rating, product_reviews, product_release_date,
                product_expiry_date, supplier_name, supplier_contact, supplier_email,
                supplier_phone, supplier_address, supplier_city, supplier_country)
FROM PROGRAM 'for f in /data/mock_data_*.csv; do tail -n +2 "$f" | sed "s|^|$(basename $f),|"; done'
WITH (FORMAT csv);

-- =========================================================
-- 1. СХЕМА ЗВЕЗДА
-- =========================================================
CREATE SCHEMA IF NOT EXISTS star;

DROP TABLE IF EXISTS star.fact_sale CASCADE;
DROP TABLE IF EXISTS star.dim_product CASCADE;
DROP TABLE IF EXISTS star.dim_customer CASCADE;
DROP TABLE IF EXISTS star.dim_seller CASCADE;
DROP TABLE IF EXISTS star.dim_store CASCADE;
DROP TABLE IF EXISTS star.dim_supplier CASCADE;
DROP TABLE IF EXISTS star.dim_date CASCADE;
DROP TABLE IF EXISTS star.dim_country CASCADE;
DROP TABLE IF EXISTS star.dim_pet_category CASCADE;
DROP TABLE IF EXISTS star.dim_product_category CASCADE;
DROP TABLE IF EXISTS star.dim_brand CASCADE;
DROP TABLE IF EXISTS star.dim_material CASCADE;

CREATE TABLE star.dim_country (
    country_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE star.dim_pet_category (
    pet_category_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE star.dim_product_category (
    product_category_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE star.dim_brand (
    brand_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE star.dim_material (
    material_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE star.dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL,
    weekday INT NOT NULL
);

CREATE TABLE star.dim_store (
    store_id SERIAL PRIMARY KEY,
    name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country_id INT REFERENCES star.dim_country(country_id),
    phone TEXT,
    email TEXT,
    UNIQUE (name, location, city)
);

CREATE TABLE star.dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name TEXT,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country_id INT REFERENCES star.dim_country(country_id),
    UNIQUE (name, contact, email)
);

CREATE TABLE star.dim_customer (
    customer_id SERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_customer_id INT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT,
    country_id INT REFERENCES star.dim_country(country_id),
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT,
    pet_category_id INT REFERENCES star.dim_pet_category(pet_category_id),
    UNIQUE (source_file, source_customer_id)
);

CREATE TABLE star.dim_seller (
    seller_id SERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_seller_id INT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country_id INT REFERENCES star.dim_country(country_id),
    postal_code TEXT,
    UNIQUE (source_file, source_seller_id)
);

CREATE TABLE star.dim_product (
    product_id SERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_product_id INT NOT NULL,
    name TEXT,
    category_id INT REFERENCES star.dim_product_category(product_category_id),
    price NUMERIC(10,2),
    quantity INT,
    weight NUMERIC(10,2),
    color TEXT,
    size TEXT,
    brand_id INT REFERENCES star.dim_brand(brand_id),
    material_id INT REFERENCES star.dim_material(material_id),
    description TEXT,
    rating NUMERIC(3,1),
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    supplier_id INT REFERENCES star.dim_supplier(supplier_id),
    UNIQUE (source_file, source_product_id)
);

-- =========================================================
-- 2. ФАКТ (с date_id вместо sale_date)
-- =========================================================
CREATE TABLE star.fact_sale (
    sale_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_sale_id INT NOT NULL,
    date_id INT REFERENCES star.dim_date(date_id),        -- ← добавлено
    customer_id INT REFERENCES star.dim_customer(customer_id),
    seller_id INT REFERENCES star.dim_seller(seller_id),
    product_id INT REFERENCES star.dim_product(product_id),
    store_id INT REFERENCES star.dim_store(store_id),
    sale_quantity INT NOT NULL,
    sale_total_price NUMERIC(12,2) NOT NULL,
    UNIQUE (source_file, source_sale_id)
);

CREATE INDEX idx_fact_sale_date     ON star.fact_sale(date_id);       -- ← заменено
CREATE INDEX idx_fact_sale_customer ON star.fact_sale(customer_id);
CREATE INDEX idx_fact_sale_seller   ON star.fact_sale(seller_id);
CREATE INDEX idx_fact_sale_product  ON star.fact_sale(product_id);
CREATE INDEX idx_fact_sale_store    ON star.fact_sale(store_id);