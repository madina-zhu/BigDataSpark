# BigDataSpark — Лабораторная работа №2

ETL-пайплайн на Apache Spark: `mock_data` (PostgreSQL) → star schema (PostgreSQL) → 18 отчётов (ClickHouse).

## Структура

- `data/mock_data_1..10.csv` — исходные данные
- `sql/init_postgres.sql` — инициализация PostgreSQL + star schema
- `sql/init_clickhouse.sql` — инициализация ClickHouse (18 витрин отчётов)
- `spark/` — Spark-джобы и Dockerfile (JDBC-драйверы скачиваются автоматически при сборке)
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

### 2. Запустить ETL
```bash
bash scripts/run_all.sh
```

### 3. Проверить отчёты

**PostgreSQL:**
```bash
docker exec -it hs_postgres psql -U hs_user -d hs_db
```
```sql
SELECT COUNT(*) FROM star.fact_sale;         -- должно быть 10000
SELECT COUNT(*) FROM star.dim_customer;      -- 10000
SELECT COUNT(*) FROM star.dim_product;       -- 10000
```

**ClickHouse:**
```bash
docker exec -it hs_clickhouse clickhouse-client
```
```sql
SHOW TABLES FROM reports;
SELECT * FROM reports.top10_products;
```

## Подключение с хоста

| Сервис | Хост:порт | Пользователь | Пароль | База |
|---|---|---|---|---|
| PostgreSQL | `localhost:55432` | `hs_user` | `hs_pass` | `hs_db` |
| ClickHouse (HTTP) | `localhost:8123` | `default` | (пусто) | `reports` |
| ClickHouse (native) | `localhost:9000` | `default` | (пусто) | `reports` |

Пример подключения через DBeaver / psql:
```bash
psql -h localhost -p 55432 -U hs_user -d hs_db
```

## Отчёты в ClickHouse (db `reports`)

| Таблица | Что считает |
|---|---|
| `top10_products` | ТОП-10 товаров по выручке |
| `revenue_by_category` | Выручка по категориям |
| `product_rating_reviews` | Рейтинг и отзывы по товарам |
| `top10_customers` | ТОП-10 клиентов по тратам |
| `customers_by_country` | Клиенты и траты по странам |
| `customer_avg_check` | Средний чек по клиентам |
| `monthly_sales` | Выручка по месяцам |
| `yearly_sales` | Выручка по годам |
| `avg_order_by_month` | Средний чек по месяцам |
| `top5_stores` | ТОП-5 магазинов по выручке |
| `sales_by_city_country` | Продажи по городам/странам |
| `store_avg_check` | Средний чек по магазинам |
| `top5_suppliers` | ТОП-5 поставщиков по выручке |
| `supplier_avg_price` | Средняя цена по поставщикам |
| `suppliers_by_country` | Поставщики по странам |
| `products_best_worst_rating` | ТОП-10 лучших/худших по рейтингу |
| `rating_vs_sales` | Рейтинг vs продажи |
| `top_products_by_reviews` | ТОП-10 по отзывам |

## Модель данных

**Измерения (PostgreSQL, схема `star`):**
`dim_country`, `dim_pet_category`, `dim_product_category`, `dim_brand`, `dim_material`, `dim_date`, `dim_store`, `dim_supplier`, `dim_customer`, `dim_seller`, `dim_product`.

**Факт:**
`fact_sale` — 10 000 строк, метрики `sale_quantity`, `sale_total_price`, FK на все измерения (включая `date_id`).

**Проверка целостности:**
```sql
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE date_id IS NULL) AS null_dates,
       COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customers,
       SUM(sale_total_price) AS total_revenue
FROM star.fact_sale;
-- total = 10000, null_dates = 0, total_revenue = 2529852.12
```

## Порядок остановки
```bash
docker compose down -v   # -v удалит volumes (данные)
```
```

---

## 🔍 Что добавлено

| Раздел | Зачем |
|---|---|
| **«Подключение с хоста»** | Чтобы лаборант мог подключиться через DBeaver/psql, а не только через `docker exec` |
| **«Отчёты в ClickHouse»** | Полный список 18 таблиц — лаборант сразу видит, что проверять |
| **«Модель данных»** | Краткое описание измерений, факта и проверки целостности |


