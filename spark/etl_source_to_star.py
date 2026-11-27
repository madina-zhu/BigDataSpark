from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, lit, year, month, dayofmonth, quarter, dayofweek
)

spark = (SparkSession.builder
         .appName("etl_source_to_star")
         .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
         .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
         .getOrCreate())

PG_URL = "jdbc:postgresql://postgres:5432/hs_db"
PG_PROPS = {"user": "hs_user", "password": "hs_pass",
            "driver": "org.postgresql.Driver"}

DATE_FMT = "MM/dd/yyyy"


def read(t):
    return spark.read.jdbc(PG_URL, t, properties=PG_PROPS)


def write(df, t):
    df.write.jdbc(PG_URL, t, mode="append", properties=PG_PROPS)


def truncate(table):
    jvm = spark._jvm
    conn = jvm.java.sql.DriverManager.getConnection(PG_URL, "hs_user", "hs_pass")
    try:
        stmt = conn.createStatement()
        stmt.execute(f"TRUNCATE TABLE {table} CASCADE")
        stmt.close()
    finally:
        conn.close()


wide = read("mock_data").cache()
wide = (wide
        .withColumn("sale_date_parsed", to_date(col("sale_date"), DATE_FMT))
        .withColumn("release_date_parsed", to_date(col("product_release_date"), DATE_FMT))
        .withColumn("expiry_date_parsed", to_date(col("product_expiry_date"), DATE_FMT)))

# --- dim_country ---
truncate("star.dim_country")
countries = (wide.select(col("customer_country").alias("name"))
             .union(wide.select(col("seller_country").alias("name")))
             .union(wide.select(col("store_country").alias("name")))
             .union(wide.select(col("supplier_country").alias("name")))
             .distinct()
             .filter(col("name").isNotNull() & (col("name") != "")))
write(countries, "star.dim_country")

# --- dim_pet_category ---
truncate("star.dim_pet_category")
pet_categories = (wide.select(col("pet_category").alias("name"))
                  .distinct().filter(col("name").isNotNull() & (col("name") != "")))
write(pet_categories, "star.dim_pet_category")

# --- dim_product_category ---
truncate("star.dim_product_category")
product_categories = (wide.select(col("product_category").alias("name"))
                      .distinct().filter(col("name").isNotNull() & (col("name") != "")))
write(product_categories, "star.dim_product_category")

# --- dim_brand ---
truncate("star.dim_brand")
brands = (wide.select(col("product_brand").alias("name"))
          .distinct().filter(col("name").isNotNull() & (col("name") != "")))
write(brands, "star.dim_brand")

# --- dim_material ---
truncate("star.dim_material")
materials = (wide.select(col("product_material").alias("name"))
             .distinct().filter(col("name").isNotNull() & (col("name") != "")))
write(materials, "star.dim_material")

dim_country  = read("star.dim_country").select(
                  col("country_id"), col("name").alias("country_name"))
dim_pet_cat  = read("star.dim_pet_category").select(
                  col("pet_category_id"), col("name").alias("pet_category_name"))
dim_prod_cat = read("star.dim_product_category").select(
                  col("product_category_id"), col("name").alias("product_category_name"))
dim_brand    = read("star.dim_brand").select(
                  col("brand_id"), col("name").alias("brand_name"))
dim_material = read("star.dim_material").select(
                  col("material_id"), col("name").alias("material_name"))

# --- dim_store ---
truncate("star.dim_store")
stores = (wide.select(
            col("store_name").alias("name"),
            col("store_location").alias("location"),
            col("store_city").alias("city"),
            col("store_state").alias("state"),
            col("store_country").alias("country_name"),
            col("store_phone").alias("phone"),
            col("store_email").alias("email"))
          .filter(col("name").isNotNull() & (col("name") != ""))
          .distinct()
          .join(dim_country, "country_name", "left")
          .select("name", "location", "city", "state",
                  col("country_id"), "phone", "email"))
write(stores, "star.dim_store")

# --- dim_supplier ---
truncate("star.dim_supplier")
suppliers = (wide.select(
                col("supplier_name").alias("name"),
                col("supplier_contact").alias("contact"),
                col("supplier_email").alias("email"),
                col("supplier_phone").alias("phone"),
                col("supplier_address").alias("address"),
                col("supplier_city").alias("city"),
                col("supplier_country").alias("country_name"))
             .filter(col("name").isNotNull() & (col("name") != ""))
             .distinct()
             .join(dim_country, "country_name", "left")
             .select("name", "contact", "email", "phone", "address", "city",
                     col("country_id")))
write(suppliers, "star.dim_supplier")

# --- dim_customer ---
truncate("star.dim_customer")
customers = (wide.select(
                col("source_file"),
                col("sale_customer_id").alias("source_customer_id"),
                col("customer_first_name").alias("first_name"),
                col("customer_last_name").alias("last_name"),
                col("customer_age").alias("age"),
                col("customer_email").alias("email"),
                col("customer_country").alias("country_name"),
                col("customer_postal_code").alias("postal_code"),
                col("customer_pet_type").alias("pet_type"),
                col("customer_pet_name").alias("pet_name"),
                col("customer_pet_breed").alias("pet_breed"),
                col("pet_category").alias("pet_category_name"))
             .distinct()
             .join(dim_country, "country_name", "left")
             .join(dim_pet_cat, "pet_category_name", "left")
             .select("source_file", "source_customer_id",
                     "first_name", "last_name", "age", "email",
                     col("country_id"), "postal_code",
                     "pet_type", "pet_name", "pet_breed",
                     col("pet_category_id")))
write(customers, "star.dim_customer")

# --- dim_seller ---
truncate("star.dim_seller")
sellers = (wide.select(
              col("source_file"),
              col("sale_seller_id").alias("source_seller_id"),
              col("seller_first_name").alias("first_name"),
              col("seller_last_name").alias("last_name"),
              col("seller_email").alias("email"),
              col("seller_country").alias("country_name"),
              col("seller_postal_code").alias("postal_code"))
           .distinct()
           .join(dim_country, "country_name", "left")
           .select("source_file", "source_seller_id",
                   "first_name", "last_name", "email",
                   col("country_id"), "postal_code"))
write(sellers, "star.dim_seller")

# --- dim_product ---
dim_supplier = read("star.dim_supplier").select(
    col("supplier_id"),
    col("name").alias("sup_name"),
    col("city").alias("sup_city"),
    col("country_id").alias("sup_country_id")
)

truncate("star.dim_product")
products = (wide.select(
               col("source_file"),
               col("sale_product_id").alias("source_product_id"),
               col("product_name").alias("name"),
               col("product_category").alias("product_category_name"),
               col("product_price").alias("price"),
               col("product_quantity").alias("quantity"),
               col("product_weight").alias("weight"),
               col("product_color").alias("color"),
               col("product_size").alias("size"),
               col("product_brand").alias("brand_name"),
               col("product_material").alias("material_name"),
               col("product_description").alias("description"),
               col("product_rating").alias("rating"),
               col("product_reviews").alias("reviews"),
               col("release_date_parsed").alias("release_date"),
               col("expiry_date_parsed").alias("expiry_date"),
               col("supplier_name"),
               col("supplier_city"),
               col("supplier_country"))
            .distinct()
            .join(dim_prod_cat, "product_category_name", "left")
            .join(dim_brand,    "brand_name",           "left")
            .join(dim_material, "material_name",        "left")
            .join(dim_country,
                  col("supplier_country") == col("country_name"), "left")
            .join(dim_supplier,
                  (col("supplier_name") == col("sup_name"))
                  & (col("supplier_city") == col("sup_city"))
                  & (col("sup_country_id") == col("country_id")), "left")
            .select("source_file", "source_product_id", "name",
                    col("product_category_id").alias("category_id"),
                    "price", "quantity", "weight", "color", "size",
                    col("brand_id"), col("material_id"),
                    "description", "rating", "reviews",
                    "release_date", "expiry_date",
                    col("supplier_id")))
write(products, "star.dim_product")

# --- dim_date ---
truncate("star.dim_date")
dates = (wide.select(col("sale_date_parsed").alias("full_date"))
         .filter(col("full_date").isNotNull())
         .distinct()
         .withColumn("year", year("full_date"))
         .withColumn("month", month("full_date"))
         .withColumn("day", dayofmonth("full_date"))
         .withColumn("quarter", quarter("full_date"))
         .withColumn("weekday", dayofweek("full_date")))
write(dates, "star.dim_date")

# --- lookup для факта ---
dim_date_lkp = read("star.dim_date").select(
    col("date_id"),
    col("full_date").alias("date_full"))

dim_customer_lkp = read("star.dim_customer").select(
                      col("source_file").alias("cf_file"),
                      col("source_customer_id").alias("cf_cust_id"),
                      col("customer_id"))
dim_seller_lkp   = read("star.dim_seller").select(
                      col("source_file").alias("sf_file"),
                      col("source_seller_id").alias("sf_sell_id"),
                      col("seller_id"))
dim_product_lkp  = read("star.dim_product").select(
                      col("source_file").alias("pf_file"),
                      col("source_product_id").alias("pf_prod_id"),
                      col("product_id"))
dim_store_lkp = read("star.dim_store").select(
    col("store_id"),
    col("name").alias("store_name_lkp"),
    col("city").alias("store_city_lkp"),
    col("email").alias("store_email_lkp"))

# --- fact_sale ---
truncate("star.fact_sale")
fact = (wide.select(
            col("source_file"),
            col("id").alias("source_sale_id"),
            col("sale_customer_id"),
            col("sale_seller_id"),
            col("sale_product_id"),
            col("store_name"),
            col("store_city"),
            col("store_email"),
            col("sale_date_parsed").alias("sale_date"),
            col("sale_quantity"),
            col("sale_total_price"))
        .join(dim_date_lkp,
              col("sale_date") == col("date_full"), "left")
        .join(dim_customer_lkp,
              (col("source_file") == col("cf_file")) &
              (col("sale_customer_id") == col("cf_cust_id")), "left")
        .join(dim_seller_lkp,
              (col("source_file") == col("sf_file")) &
              (col("sale_seller_id") == col("sf_sell_id")), "left")
        .join(dim_product_lkp,
              (col("source_file") == col("pf_file")) &
              (col("sale_product_id") == col("pf_prod_id")), "left")
        .join(dim_store_lkp,
              (col("store_name") == col("store_name_lkp")) &
              (col("store_city") == col("store_city_lkp")) &
              (col("store_email") == col("store_email_lkp")), "left")
        .select(col("source_file"),
                col("source_sale_id"),
                col("date_id"),
                col("customer_id"),
                col("seller_id"),
                col("product_id"),
                col("store_id"),
                col("sale_quantity"),
                col("sale_total_price")))

write(fact, "star.fact_sale")

print("ETL source -> star finished")
spark.stop()