"""
ETL: star schema (PostgreSQL) -> key/value reports in Valkey (Redis-compatible).
Каждый отчёт хранится как Redis-hash: report:<name> -> field -> JSON.
"""
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, year, month, to_json, struct
)

spark = (SparkSession.builder
         .appName("etl_star_to_valkey")
         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
         .getOrCreate())

PG_URL = "jdbc:postgresql://postgres:5432/hs_db"
PG_PROPS = {"user": "hs_user", "password": "hs_pass",
            "driver": "org.postgresql.Driver"}

VALKEY_HOST = "valkey"
VALKEY_PORT = 6379

def read(t): return spark.read.jdbc(PG_URL, t, properties=PG_PROPS)

def write_valkey(df, report_name, key_col):
    """Пишем по одной JSON-записи на ключ report_name:key_col."""
    json_df = (df.select(
                  col(key_col).cast("string").alias("k"),
                  to_json(struct(*[c for c in df.columns])).alias("v")))
    def _writer(rows):
        import redis
        r = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, db=0)
        pipe = r.pipeline()
        for row in rows:
            pipe.hset(f"report:{report_name}", row["k"], row["v"])
        pipe.execute()
    json_df.foreachPartition(_writer)

fact    = read("star.fact_sale")
prod    = read("star.dim_product")
cust    = read("star.dim_customer")
store   = read("star.dim_store")
supp    = read("star.dim_supplier")
country = read("star.dim_country")

# 1. product_sales
df1 = (fact.join(prod, "product_id")
       .groupBy("product_id", "name")
       .agg(_sum("sale_total_price").alias("total_revenue"),
            _sum("sale_quantity").alias("total_quantity"),
            avg("rating").alias("avg_rating"),
            _sum("reviews").alias("reviews"))
       .withColumnRenamed("name", "product_name"))
write_valkey(df1, "product_sales", "product_id")

# 2. customer_sales
df2 = (fact.join(cust, "customer_id").join(country, "country_id")
       .groupBy("customer_id", "first_name", "last_name", "name")
       .agg(_sum("sale_total_price").alias("total_spent"),
            avg("sale_total_price").alias("avg_check"))
       .withColumnRenamed("name", "country")
       .withColumnRenamed("first_name", "customer_name"))
write_valkey(df2, "customer_sales", "customer_id")

# 3. time_sales
df3 = (fact.withColumn("year", year("sale_date"))
            .withColumn("month", month("sale_date"))
            .groupBy("year", "month")
            .agg(_sum("sale_total_price").alias("total_revenue"),
                 avg("sale_total_price").alias("avg_order")))
write_valkey(df3, "time_sales", "year")

# 4. store_sales
df4 = (fact.join(store, "store_id").join(country, "country_id")
       .groupBy("store_id", "name", "city", "country.name")
       .agg(_sum("sale_total_price").alias("total_revenue"),
            avg("sale_total_price").alias("avg_check"))
       .withColumnRenamed("name", "store_name"))
write_valkey(df4, "store_sales", "store_id")

# 5. supplier_sales
df5 = (fact.join(prod, "product_id").join(supp, "supplier_id").join(country, "country_id")
       .groupBy("supplier_id", "supplier.name", "country.name")
       .agg(_sum("sale_total_price").alias("total_revenue"),
            avg("price").alias("avg_price"))
       .withColumnRenamed("name", "supplier_name"))
write_valkey(df5, "supplier_sales", "supplier_id")

# 6. product_quality
df6 = (fact.join(prod, "product_id")
       .groupBy("product_id", "name", "rating", "reviews")
       .agg(_sum("sale_quantity").alias("total_sold"))
       .withColumnRenamed("name", "product_name"))
write_valkey(df6, "product_quality", "product_id")

print("ETL star -> Valkey finished")
spark.stop()