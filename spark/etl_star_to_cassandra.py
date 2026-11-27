CREATE KEYSPACE IF NOT EXISTS reports
  WITH replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE IF NOT EXISTS reports.product_sales (
  product_id int PRIMARY KEY,
  product_name text, total_revenue double, total_quantity int,
  avg_rating double, reviews int);

CREATE TABLE IF NOT EXISTS reports.customer_sales (
  customer_id int PRIMARY KEY,
  customer_name text, total_spent double, country text, avg_check double);

CREATE TABLE IF NOT EXISTS reports.time_sales (
  year int, month int, total_revenue double, avg_order double,
  PRIMARY KEY (year, month));

CREATE TABLE IF NOT EXISTS reports.store_sales (
  store_id int PRIMARY KEY,
  store_name text, city text, country text,
  total_revenue double, avg_check double);

CREATE TABLE IF NOT EXISTS reports.supplier_sales (
  supplier_id int PRIMARY KEY,
  supplier_name text, country text,
  total_revenue double, avg_price double);

CREATE TABLE IF NOT EXISTS reports.product_quality (
  product_id int PRIMARY KEY,
  product_name text, rating double, reviews int, total_sold int);