# Databricks notebook source
# MAGIC %run ./transformations

# COMMAND ----------

BRONZE = "maddy_test.bronze"
SILVER = "maddy_test.silver"

# COMMAND ----------

# Customers

customers = clean_customers(spark.table(f"{BRONZE}.raw_customers"))
customers.write.mode("overwrite").saveAsTable(f"{SILVER}.customers")

# COMMAND ----------

# Products

products = clean_products(spark.table(f"{BRONZE}.raw_products"))
products.write.mode("overwrite").saveAsTable(f"{SILVER}.products")

# COMMAND ----------

# Orders with customer + product details

orders = enrich_orders(
    spark.table(f"{BRONZE}.raw_orders"),
    spark.table(f"{SILVER}.customers"),
    spark.table(f"{SILVER}.products"),
)

orders.write.mode("overwrite").saveAsTable(f"{SILVER}.orders")

# COMMAND ----------

for table in ["customers", "products", "orders"]:
    print(f"{table}: {spark.table(f'{SILVER}.{table}').count()} rows")

print("orders with unknown product:",
      spark.table(f"{SILVER}.orders").filter("category = 'Unknown'").count())

# COMMAND ----------

# quick checks on the real data

raw_orders = spark.table(f"{BRONZE}.raw_orders")
silver_orders = spark.table(f"{SILVER}.orders")

assert silver_orders.count() == raw_orders.count(), "orders lost or duplicated"
assert silver_orders.filter("order_date IS NULL").count() == 0, "some dates did not convert"

dup_products = spark.table(f"{SILVER}.products").groupBy("product_id").count().filter("count > 1")
assert dup_products.count() == 0, "duplicate product ids"

print("checks passed")
