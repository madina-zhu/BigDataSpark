from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, countDistinct, year, month, desc,
    concat_ws, lit
)
from clickhouse_driver import Client as CHClient

spark = (SparkSession.builder
         .appName("etl_star_to_clickhouse")
         .config("spark.jars",
                 "/opt/spark/jars/postgresql-42.7.3.jar,"
                 "/opt/spark/jars/clickhouse-jdbc-0.6.0-all.jar")
         .getOrCreate())

PG_URL = "jdbc:postgresql://postgres:5432/hs_db"
PG_PROPS = {"user": "hs_user", "password": "hs_pass",
            "driver": "org.postgresql.Driver"}

CH_HOST = "clickhouse"
CH_PORT = 9000
CH_DB = "reports"


def ch_client():
    return CHClient(host=CH_HOST, port=CH_PORT, user="default", password="")


def ch_truncate(table):
    c = ch_client()
    c.execute(f"TRUNCATE TABLE IF EXISTS {CH_DB}.{table}")
    c.disconnect()


def read(t):
    return spark.read.jdbc(PG_URL, t, properties=PG_PROPS)


def write_ch(df, table):
    ch_truncate(table)
    (df.write
     .format("jdbc")
     .option("url", f"jdbc:clickhouse://{CH_HOST}:8123/{CH_DB}")
     .option("dbtable", table)
     .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
     .option("user", "default")
     .option("password", "")
     .mode("append")
     .save())


# ============================================================
# Чтение измерений
# ============================================================
fact = read("star.fact_sale")

# «Возвращаем» sale_date через dim_date (в факте теперь date_id)
dim_date = read("star.dim_date").select(
    col("date_id"),
    col("full_date").alias("sale_date")
)
fact = fact.join(dim_date, "date_id", "left")

prod = read("star.dim_product")
prod_cat = read("star.dim_product_category").select(
    col("product_category_id").alias("category_id"),
    col("name").alias("category")
)
cust = read("star.dim_customer")
store = read("star.dim_store")
supp = read("star.dim_supplier").withColumnRenamed("name", "supplier_name")
country = read("star.dim_country").withColumnRenamed("name", "country")

prod = prod.join(prod_cat, "category_id", "left")
cust = cust.join(country, "country_id", "left")
store = store.join(country, "country_id", "left")
supp = supp.join(country, "country_id", "left") \
           .withColumnRenamed("country", "supplier_country")


# ============================================================
# 1. PRODUCT
# ============================================================
fact_prod = fact.join(prod, "product_id")

top10_products = (fact_prod
                  .groupBy("product_id", "name")
                  .agg(_sum("sale_total_price").alias("total_revenue"),
                       _sum("sale_quantity").alias("total_quantity"))
                  .withColumnRenamed("name", "product_name")
                  .orderBy(desc("total_revenue"))
                  .limit(10))
write_ch(top10_products, "top10_products")

revenue_by_category = (fact_prod
                       .groupBy("category")
                       .agg(_sum("sale_total_price").alias("total_revenue"),
                            _sum("sale_quantity").alias("total_quantity")))
write_ch(revenue_by_category, "revenue_by_category")

product_rating_reviews = (prod
                          .select(col("product_id"),
                                  col("name").alias("product_name"),
                                  col("rating").alias("avg_rating"),
                                  col("reviews")))
write_ch(product_rating_reviews, "product_rating_reviews")


# ============================================================
# 2. CUSTOMER
# ============================================================
fact_cust = fact.join(cust, "customer_id")

top10_customers = (fact_cust
                   .groupBy("customer_id", "first_name", "last_name")
                   .agg(_sum("sale_total_price").alias("total_spent"))
                   .withColumn("customer_name",
                               concat_ws(" ", "first_name", "last_name"))
                   .select("customer_id", "customer_name", "total_spent")
                   .orderBy(desc("total_spent"))
                   .limit(10))
write_ch(top10_customers, "top10_customers")

customers_by_country = (fact_cust
                        .groupBy("country")
                        .agg(countDistinct("customer_id").alias("customers_count"),
                             _sum("sale_total_price").alias("total_spent")))
write_ch(customers_by_country, "customers_by_country")

customer_avg_check = (fact_cust
                      .groupBy("customer_id", "first_name", "last_name")
                      .agg(avg("sale_total_price").alias("avg_check"))
                      .withColumn("customer_name",
                                  concat_ws(" ", "first_name", "last_name"))
                      .select("customer_id", "customer_name", "avg_check"))
write_ch(customer_avg_check, "customer_avg_check")


# ============================================================
# 3. TIME
# ============================================================
fact_time = (fact
             .withColumn("year", year("sale_date"))
             .withColumn("month", month("sale_date")))

monthly_sales = (fact_time
                 .groupBy("year", "month")
                 .agg(_sum("sale_total_price").alias("total_revenue")))
write_ch(monthly_sales, "monthly_sales")

yearly_sales = (fact_time
                .groupBy("year")
                .agg(_sum("sale_total_price").alias("total_revenue")))
write_ch(yearly_sales, "yearly_sales")

avg_order_by_month = (fact_time
                      .groupBy("year", "month")
                      .agg(avg("sale_total_price").alias("avg_order")))
write_ch(avg_order_by_month, "avg_order_by_month")


# ============================================================
# 4. STORE
# ============================================================
fact_store = fact.join(store, "store_id")

top5_stores = (fact_store
               .groupBy("store_id", "name")
               .agg(_sum("sale_total_price").alias("total_revenue"))
               .withColumnRenamed("name", "store_name")
               .orderBy(desc("total_revenue"))
               .limit(5))
write_ch(top5_stores, "top5_stores")

sales_by_city_country = (fact_store
                         .groupBy("city", "country")
                         .agg(_sum("sale_total_price").alias("total_revenue")))
write_ch(sales_by_city_country, "sales_by_city_country")

store_avg_check = (fact_store
                   .groupBy("store_id", "name")
                   .agg(avg("sale_total_price").alias("avg_check"))
                   .withColumnRenamed("name", "store_name"))
write_ch(store_avg_check, "store_avg_check")


# ============================================================
# 5. SUPPLIER
# ============================================================
fact_supp = fact.join(prod, "product_id").join(supp, "supplier_id")

top5_suppliers = (fact_supp
                  .groupBy("supplier_id", "supplier_name")
                  .agg(_sum("sale_total_price").alias("total_revenue"))
                  .orderBy(desc("total_revenue"))
                  .limit(5))
write_ch(top5_suppliers, "top5_suppliers")

supplier_avg_price = (prod
                      .join(supp, "supplier_id")
                      .groupBy("supplier_id", "supplier_name")
                      .agg(avg("price").alias("avg_price")))
write_ch(supplier_avg_price, "supplier_avg_price")

suppliers_by_country = (fact_supp
                        .groupBy("supplier_country")
                        .agg(countDistinct("supplier_id").alias("suppliers_count"),
                             _sum("sale_total_price").alias("total_revenue"))
                        .withColumnRenamed("supplier_country", "country"))
write_ch(suppliers_by_country, "suppliers_by_country")


# ============================================================
# 6. QUALITY
# ============================================================
fact_prod_q = fact.join(prod, "product_id")

best = (prod.select("product_id", "name", "rating")
        .orderBy(desc("rating")).limit(10)
        .withColumn("rank_type", lit("best")))
worst = (prod.select("product_id", "name", "rating")
         .orderBy("rating").limit(10)
         .withColumn("rank_type", lit("worst")))
best_worst = best.union(worst).withColumnRenamed("name", "product_name")
write_ch(best_worst, "products_best_worst_rating")

rating_vs_sales = (fact_prod_q
                   .groupBy("product_id", "name", "rating")
                   .agg(_sum("sale_quantity").alias("total_sold"))
                   .withColumnRenamed("name", "product_name"))
write_ch(rating_vs_sales, "rating_vs_sales")

top_products_by_reviews = (prod
                           .select(col("product_id"),
                                   col("name").alias("product_name"),
                                   col("reviews"))
                           .orderBy(desc("reviews"))
                           .limit(10))
write_ch(top_products_by_reviews, "top_products_by_reviews")

print("ETL star -> ClickHouse finished")
spark.stop()