"""
ETL: star schema (PostgreSQL) -> graph in Neo4j.
Узлы: Product, Customer, Seller, Store, Supplier, Country, Category, Time.
Связи: SOLD, BOUGHT, SUPPLIED, LOCATED_IN, IN_CATEGORY, ON_DATE.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, year, month

spark = (SparkSession.builder
         .appName("etl_star_to_neo4j")
         .config("spark.jars.packages",
                 "org.postgresql:postgresql:42.7.3,"
                 "org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3")
         .config("spark.neo4j.url", "bolt://neo4j:7687")
         .config("spark.neo4j.user", "neo4j")
         .config("spark.neo4j.password", "hs_password")
         .getOrCreate())

PG_URL = "jdbc:postgresql://postgres:5432/hs_db"
PG_PROPS = {"user": "hs_user", "password": "hs_pass",
            "driver": "org.postgresql.Driver"}

def read(t): return spark.read.jdbc(PG_URL, t, properties=PG_PROPS)

def write_neo4j(df, node_label, keys):
    (df.write.format("org.neo4j.spark.DataSource")
       .option("labels", node_label)
       .option("node.keys", ",".join(keys))
       .mode("Overwrite")
       .save())

fact    = read("star.fact_sale")
prod    = read("star.dim_product")
cust    = read("star.dim_customer")
seller  = read("star.dim_seller")
store   = read("star.dim_store")
supp    = read("star.dim_supplier")
country = read("star.dim_country")
pcat    = read("star.dim_product_category")
petcat  = read("star.dim_pet_category")
brand   = read("star.dim_brand")
material= read("star.dim_material")

# --- Узлы -------------------------------------------------------------------
write_neo4j(prod.select("product_id", "name", "price", "rating", "reviews")
                .withColumnRenamed("name", "product_name"),
            "Product", ["product_id"])

write_neo4j(cust.select("customer_id", "first_name", "last_name",
                        "age", "email"),
            "Customer", ["customer_id"])

write_neo4j(seller.select("seller_id", "first_name", "last_name", "email"),
            "Seller", ["seller_id"])

write_neo4j(store.select("store_id", "name", "city", "state"),
            "Store", ["store_id"])

write_neo4j(supp.select("supplier_id", "name", "city"),
            "Supplier", ["supplier_id"])

write_neo4j(country.select("country_id", "name"),
            "Country", ["country_id"])

write_neo4j(pcat.select("product_category_id", "name"),
            "ProductCategory", ["product_category_id"])

write_neo4j(petcat.select("pet_category_id", "name"),
            "PetCategory", ["pet_category_id"])

write_neo4j(brand.select("brand_id", "name"),
            "Brand", ["brand_id"])

write_neo4j(material.select("material_id", "name"),
            "Material", ["material_id"])

# --- Связи ------------------------------------------------------------------
# SOLD: Store -[SOLD]-> Product
sold = (fact.join(store, "store_id").join(prod, "product_id")
        .select(col("store_id").alias("src"), col("product_id").alias("dst"),
                col("sale_quantity").alias("qty"),
                col("sale_total_price").alias("total")))
(sold.write.format("org.neo4j.spark.DataSource")
     .option("relationship", "SOLD")
     .option("relationship.save.strategy", "keys")
     .option("relationship.source.labels", ":Store")
     .option("relationship.source.node.keys", "src:store_id")
     .option("relationship.target.labels", ":Product")
     .option("relationship.target.node.keys", "dst:product_id")
     .option("relationship.properties", "qty,total")
     .mode("Overwrite")
     .save())

# BOUGHT: Customer -[BOUGHT]-> Product
bought = (fact.join(cust, "customer_id").join(prod, "product_id")
          .select(col("customer_id").alias("src"),
                  col("product_id").alias("dst"),
                  col("sale_quantity").alias("qty"),
                  col("sale_total_price").alias("total")))
(bought.write.format("org.neo4j.spark.DataSource")
      .option("relationship", "BOUGHT")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Customer")
      .option("relationship.source.node.keys", "src:customer_id")
      .option("relationship.target.labels", ":Product")
      .option("relationship.target.node.keys", "dst:product_id")
      .option("relationship.properties", "qty,total")
      .mode("Overwrite")
      .save())

# SUPPLIED: Supplier -[SUPPLIED]-> Product
supplied = (prod.join(supp, "supplier_id")
            .select(col("supplier_id").alias("src"),
                    col("product_id").alias("dst")))
(supplied.write.format("org.neo4j.spark.DataSource")
        .option("relationship", "SUPPLIED")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", ":Supplier")
        .option("relationship.source.node.keys", "src:supplier_id")
        .option("relationship.target.labels", ":Product")
        .option("relationship.target.node.keys", "dst:product_id")
        .mode("Overwrite")
        .save())

# LOCATED_IN: Store -[LOCATED_IN]-> Country
loc_store = (store.select(col("store_id").alias("src"),
                          col("country_id").alias("dst")))
(loc_store.write.format("org.neo4j.spark.DataSource")
        .option("relationship", "LOCATED_IN")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", ":Store")
        .option("relationship.source.node.keys", "src:store_id")
        .option("relationship.target.labels", ":Country")
        .option("relationship.target.node.keys", "dst:country_id")
        .mode("Overwrite")
        .save())

# IN_CATEGORY: Product -[IN_CATEGORY]-> ProductCategory
in_cat = (prod.select(col("product_id").alias("src"),
                      col("category_id").alias("dst")))
(in_cat.write.format("org.neo4j.spark.DataSource")
       .option("relationship", "IN_CATEGORY")
       .option("relationship.save.strategy", "keys")
       .option("relationship.source.labels", ":Product")
       .option("relationship.source.node.keys", "src:product_id")
       .option("relationship.target.labels", ":ProductCategory")
       .option("relationship.target.node.keys", "dst:product_category_id")
       .mode("Overwrite")
       .save())

print("ETL star -> Neo4j finished")
spark.stop()