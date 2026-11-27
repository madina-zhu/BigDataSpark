CREATE DATABASE IF NOT EXISTS reports;

-- ===================== 1. PRODUCT =====================
CREATE TABLE IF NOT EXISTS reports.top10_products (
    product_id Int32,
    product_name String,
    total_revenue Decimal(18,2),
    total_quantity Int64
) ENGINE = MergeTree ORDER BY product_id;

CREATE TABLE IF NOT EXISTS reports.revenue_by_category (
    category String,
    total_revenue Decimal(18,2),
    total_quantity Int64
) ENGINE = MergeTree ORDER BY category;

CREATE TABLE IF NOT EXISTS reports.product_rating_reviews (
    product_id Int32,
    product_name String,
    avg_rating Float64,
    reviews Int64
) ENGINE = MergeTree ORDER BY product_id;

-- ===================== 2. CUSTOMER =====================
CREATE TABLE IF NOT EXISTS reports.top10_customers (
    customer_id Int32,
    customer_name String,
    total_spent Decimal(18,2)
) ENGINE = MergeTree ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS reports.customers_by_country (
    country String,
    customers_count Int64,
    total_spent Decimal(18,2)
) ENGINE = MergeTree ORDER BY country;

CREATE TABLE IF NOT EXISTS reports.customer_avg_check (
    customer_id Int32,
    customer_name String,
    avg_check Decimal(18,2)
) ENGINE = MergeTree ORDER BY customer_id;

-- ===================== 3. TIME =====================
CREATE TABLE IF NOT EXISTS reports.monthly_sales (
    year Int32,
    month Int32,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS reports.yearly_sales (
    year Int32,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree ORDER BY year;

CREATE TABLE IF NOT EXISTS reports.avg_order_by_month (
    year Int32,
    month Int32,
    avg_order Decimal(18,2)
) ENGINE = MergeTree ORDER BY (year, month);

-- ===================== 4. STORE =====================
CREATE TABLE IF NOT EXISTS reports.top5_stores (
    store_id Int32,
    store_name String,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree ORDER BY store_id;

CREATE TABLE IF NOT EXISTS reports.sales_by_city_country (
    city String,
    country String,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree ORDER BY (country, city);

CREATE TABLE IF NOT EXISTS reports.store_avg_check (
    store_id Int32,
    store_name String,
    avg_check Decimal(18,2)
) ENGINE = MergeTree ORDER BY store_id;

-- ===================== 5. SUPPLIER =====================
CREATE TABLE IF NOT EXISTS reports.top5_suppliers (
    supplier_id Int32,
    supplier_name String,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS reports.supplier_avg_price (
    supplier_id Int32,
    supplier_name String,
    avg_price Decimal(18,2)
) ENGINE = MergeTree ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS reports.suppliers_by_country (
    country String,
    suppliers_count Int64,
    total_revenue Decimal(18,2)
) ENGINE = MergeTree ORDER BY country;

-- ===================== 6. QUALITY =====================
CREATE TABLE IF NOT EXISTS reports.products_best_worst_rating (
    product_id Int32,
    product_name String,
    rating Float64,
    rank_type String
) ENGINE = MergeTree ORDER BY product_id;

CREATE TABLE IF NOT EXISTS reports.rating_vs_sales (
    product_id Int32,
    product_name String,
    rating Float64,
    total_sold Int64
) ENGINE = MergeTree ORDER BY product_id;

CREATE TABLE IF NOT EXISTS reports.top_products_by_reviews (
    product_id Int32,
    product_name String,
    reviews Int64
) ENGINE = MergeTree ORDER BY product_id;