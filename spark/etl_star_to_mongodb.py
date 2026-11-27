"""
ETL: star schema (PostgreSQL) -> 6 collections in MongoDB.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, avg, year, month
)

spark = (SparkSession.builder
         .appName("etl_star_to_mongodb")
         .config("spark.jars.packages",
                 "org.postgresql:postgresql:42.7.3,"
                 "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
         .config("spark.mongodb.output.uri",
                 "mongodb://hs_user:hs_pass@mongodb:27017/reports")
         .getOrCreate())

PG_URL = "jdbc:postgresql://postgres:5432/hs_db"
PG_PROPS = {"user": "hs_user", "password": "hs_pass",
            "driver": "org.postgresql.Driver"}

def read(t): return spark.read.jdbc(PG_URL, t, properties=PG_PROPS)

def write_mongo(df, coll):
    (df.write.format("mongodb")
       .option("database", "reports")
       .option("collection", coll)
       .mode("overwrite")
       .save())

fact    = read("star.fact_sale")
prod    = read("star.dim_product")
cust    = read("star.dim_customer")
store   = read("star.dim_store")
supp    = read("star.dim_supplier")
country = read("star.dim_country")

# 1. product_sales
write_mongo(
    fact.join(prod, "product_id")
        .groupBy("product_id", "name")
        .agg(_sum("sale_total_price").alias("total_revenue"),
             _sum("sale_quantity").alias("total_quantity"),
             avg("rating").alias("avg_rating"),
             _sum("reviews").alias("reviews"))
        .withColumnRenamed("name", "product_name"),
    "product_sales")

# 2. customer_sales
write_mongo(
    fact.join(cust, "customer_id").join(country, "country_id")
        .groupBy("customer_id", "first_name", "last_name", "name")
        .agg(_sum("sale_total_price").alias("total_spent"),
             avg("sale_total_price").alias("avg_check"))
        .withColumnRenamed("name", "country")
        .withColumnRenamed("first_name", "customer_name"),
    "customer_sales")

# 3. time_sales
write_mongo(
    fact.withColumn("year", year("sale_date"))
        .withColumn("month", month("sale_date"))
        .groupBy("year", "month")
        .agg(_sum("sale_total_price").alias("total_revenue"),
             avg("sale_total_price").alias("avg_order")),
    "time_sales")

# 4. store_sales
write_mongo(
    fact.join(store, "store_id").join(country, "country_id")
        .groupBy("store_id", "name", "city", "country.name")
        .agg(_sum("sale_total_price").alias("total_revenue"),
             avg("sale_total_price").alias("avg_check"))
        .withColumnRenamed("name", "store_name"),
    "store_sales")

# 5. supplier_sales
write_mongo(
    fact.join(prod, "product_id").join(supp, "supplier_id").join(country, "country_id")
        .groupBy("supplier_id", "supplier.name", "country.name")
        .agg(_sum("sale_total_price").alias("total_revenue"),
             avg("price").alias("avg_price"))
        .withColumnRenamed("name", "supplier_name"),
    "supplier_sales")

# 6. product_quality
write_mongo(
    fact.join(prod, "product_id")
        .groupBy("product_id", "name", "rating", "reviews")
        .agg(_sum("sale_quantity").alias("total_sold"))
        .withColumnRenamed("name", "product_name"),
    "product_quality")

print("ETL star -> MongoDB finished")
spark.stop()