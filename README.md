# BigDataSpark — Лабораторная работа №2

ETL-пайплайн на Apache Spark: `mock_data` (PostgreSQL) → star schema (PostgreSQL) → отчёты в ClickHouse (обязательно) и опционально в Cassandra / Neo4j / MongoDB / Valkey.

## Структура

- `data/mock_data_1..10.csv` — исходные данные (10 файлов по 1000 строк)
- `sql/init_postgres.sql` — инициализация PostgreSQL + star schema
- `sql/init_clickhouse.sql` — инициализация ClickHouse (18 таблиц отчётов)
- `spark/` — Spark-джобы и Dockerfile (JDBC-драйверы скачиваются при сборке)
- `scripts/run_all.sh` — запуск всего пайплайна

## Быстрый старт

### 1. Запустить инфраструктуру
```bash
docker compose up -d --build
```

Проверить, что все контейнеры поднялись:
```bash
docker ps
```

Ожидаемые контейнеры: `hs_postgres`, `hs_spark`, `hs_clickhouse`.

### 2. Запустить ETL
```bash
bash scripts/run_all.sh
```

Скрипт последовательно запускает:
1. `etl_source_to_star.py` — PostgreSQL (`mock_data`) → star schema в PostgreSQL
2. `etl_star_to_clickhouse.py` — star schema → 18 таблиц в ClickHouse

Опциональные джобы (`etl_star_to_cassandra.py`, `etl_star_to_neo4j.py`, `etl_star_to_mongodb.py`, `etl_star_to_valkey.py`) запускаются автоматически, если соответствующие контейнеры подняты.

### 3. Проверить отчёты

**PostgreSQL:**
```bash
docker exec -it hs_postgres psql -U hs_user -d hs_db
```
```sql
SELECT COUNT(*) FROM star.fact_sale;         -- 10000
SELECT COUNT(*) FROM star.dim_customer;      -- 10000
SELECT COUNT(*) FROM star.dim_product;       -- 10000
SELECT COUNT(*) FROM star.dim_store;         -- 10000
```

Проверка целостности факта:
```sql
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE date_id IS NULL) AS null_dates,
       COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customers,
       SUM(sale_total_price) AS total_revenue
FROM star.fact_sale;
-- total = 10000, null_dates = 0, total_revenue = 2529852.12
```

**ClickHouse:**
```bash
docker exec -it hs_clickhouse clickhouse-client
```
```sql
SHOW TABLES FROM reports;          -- 18 таблиц
SELECT * FROM reports.top10_products LIMIT 10;
SELECT COUNT(*) FROM reports.top10_products;   -- 10
```

## Подключение с хоста

| Сервис | Хост:порт | Пользователь | Пароль | База |
|---|---|---|---|---|
| PostgreSQL | `localhost:55432` | `hs_user` | `hs_pass` | `hs_db` |
| ClickHouse (HTTP) | `localhost:8123` | `default` | (пусто) | `reports` |
| ClickHouse (native) | `localhost:9000` | `default` | (пусто) | `reports` |

Пример:
```bash
psql -h localhost -p 55432 -U hs_user -d hs_db
```

## Отчёты в ClickHouse (db `reports`)

18 таблиц, сгруппированы по 6 витринам:

### Витрина 1. Продажи по продуктам
| Таблица | Что считает |
|---|---|
| `top10_products` | ТОП-10 товаров по выручке |
| `revenue_by_category` | Выручка по категориям |
| `product_rating_reviews` | Рейтинг и отзывы по товарам (атрибуты товара из `dim_product`) |

### Витрина 2. Продажи по клиентам
| Таблица | Что считает |
|---|---|
| `top10_customers` | ТОП-10 клиентов по тратам |
| `customers_by_country` | Клиенты и траты по странам |
| `customer_avg_check` | Средний чек по клиентам |

### Витрина 3. Продажи по времени
| Таблица | Что считает |
|---|---|
| `monthly_sales` | Выручка по месяцам |
| `yearly_sales` | Выручка по годам |
| `avg_order_by_month` | Средний чек по месяцам |

### Витрина 4. Продажи по магазинам
| Таблица | Что считает |
|---|---|
| `top5_stores` | ТОП-5 магазинов по выручке |
| `sales_by_city_country` | Продажи по городам/странам |
| `store_avg_check` | Средний чек по магазинам |

### Витрина 5. Продажи по поставщикам
| Таблица | Что считает |
|---|---|
| `top5_suppliers` | ТОП-5 поставщиков по выручке |
| `supplier_avg_price` | Средняя цена товаров по поставщикам |
| `suppliers_by_country` | Поставщики по странам |

### Витрина 6. Качество продукции
| Таблица | Что считает |
|---|---|
| `products_best_worst_rating` | ТОП-10 лучших и худших по рейтингу |
| `rating_vs_sales` | Рейтинг vs объём продаж |
| `top_products_by_reviews` | ТОП-10 по числу отзывов |

## Опциональные NoSQL-БД

Реализованы джобы (в `spark/`), но соответствующие сервисы **не входят в `docker-compose.yml`** по умолчанию:

| БД | Джоба | Коллекции/сущности |
|---|---|---|
| Cassandra | `etl_star_to_cassandra.py` | 6 таблиц в keyspace `reports` |
| Neo4j | `etl_star_to_neo4j.py` | узлы Product, Customer, Seller, Store, Supplier, Country, Category + связи SOLD, BOUGHT, SUPPLIED, LOCATED_IN, IN_CATEGORY |
| MongoDB | `etl_star_to_mongodb.py` | 6 коллекций в db `reports` |
| Valkey | `etl_star_to_valkey.py` | 6 hash-отчётов `report:<name>` |

Чтобы включить бонусную БД — добавьте сервис в `docker-compose.yml` и поднимите контейнер; `run_all.sh` подхватит его автоматически.

## Модель данных

**Измерения (PostgreSQL, схема `star`):**
`dim_country`, `dim_pet_category`, `dim_product_category`, `dim_brand`, `dim_material`, `dim_date`, `dim_store`, `dim_supplier`, `dim_customer`, `dim_seller`, `dim_product`.

**Факт:**
`fact_sale` — 10 000 строк, метрики `sale_quantity`, `sale_total_price`, FK на все измерения (включая `date_id`).

Уникальность:
- `dim_customer`, `dim_seller`, `dim_product` — по `(source_file, source_*_id)` (в исходнике id повторяется 1..1000 в каждом файле);
- `dim_store` — по `(name, location, city)`;
- `dim_supplier` — по `(name, contact, email)`;
- `fact_sale` — по `(source_file, source_sale_id)`.

**Проверка целостности:**
```sql
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE date_id IS NULL) AS null_dates,
       COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customers,
       SUM(sale_total_price) AS total_revenue
FROM star.fact_sale;
-- total = 10000, null_dates = 0, total_revenue = 2529852.12
```

## Результаты

Проверено после `docker compose up -d --build && bash scripts/run_all.sh`.

### PostgreSQL (схема `star`)

| Проверка | Значение |
|---|---|
| `SELECT COUNT(*) FROM star.fact_sale` | 10000 |
| `SELECT SUM(sale_total_price) FROM star.fact_sale` | 2529852.12 |

Сумма выручки в факте совпадает с исходной wide-таблицей (`mock_data`), целостность FK проверена (NULL в `date_id`, `customer_id` и др. отсутствуют).

### ClickHouse (db `reports`)

`SHOW TABLES FROM reports` → 18 таблиц:

```
avg_order_by_month
customer_avg_check
customers_by_country
monthly_sales
product_rating_reviews
products_best_worst_rating
rating_vs_sales
revenue_by_category
sales_by_city_country
store_avg_check
supplier_avg_price
suppliers_by_country
top10_customers
top10_products
top5_stores
top5_suppliers
top_products_by_reviews
yearly_sales
```

Пример содержимого `reports.top10_products` (ТОП-10 по выручке):

| product_id | product_name | total_revenue | total_quantity |
|---|---|---|---|
| 314 | Dog Food | 499.76 | 2 |
| 2348 | Dog Food | 499.80 | 9 |
| 2882 | Dog Food | 499.59 | 10 |
| 6424 | Cat Toy | 499.85 | 7 |
| 7173 | Dog Food | 499.76 | 8 |
| 7955 | Cat Toy | 499.73 | 10 |
| 8129 | Dog Food | 499.71 | 8 |
| 8205 | Bird Cage | 499.62 | 9 |
| 9577 | Bird Cage | 499.62 | 3 |
| 9688 | Bird Cage | 499.69 | 4 |

## Порядок остановки

```bash
docker compose down -v   # -v удалит volumes (данные)
```
