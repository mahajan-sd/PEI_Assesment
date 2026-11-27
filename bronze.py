# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

INPUT_PATH = "/Volumes/maddy_test/raw_data/pei_asses_data"
BRONZE = "maddy_test.bronze"


# "Customer ID" -> "customer_id", "Sub-Category" -> "sub_category"
def clean_column_names(df):
    new_names = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df.toDF(*new_names)

# COMMAND ----------

# fail early if any file is missing
files_in_volume = [f.name for f in dbutils.fs.ls(INPUT_PATH)]

for name in ["Orders.json", "Products.csv", "Customer.xlsx"]:
    if name not in files_in_volume:
        raise FileNotFoundError(f"{name} not found in {INPUT_PATH}")

# COMMAND ----------

# bronze = data as it comes, only column names are changed

# Orders

orders_df = spark.read.option("multiLine", True).json(f"{INPUT_PATH}/Orders.json")

orders_df = (clean_column_names(orders_df)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("Orders.json")))

orders_df.write.mode("overwrite").saveAsTable(f"{BRONZE}.raw_orders")

# COMMAND ----------

# Products
# product names have "" inside them (inches), so escape is needed

products_df = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("escape", '"')
    .csv(f"{INPUT_PATH}/Products.csv"))

products_df = (clean_column_names(products_df)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("Products.csv")))

products_df.write.mode("overwrite").saveAsTable(f"{BRONZE}.raw_products")

# COMMAND ----------

# Customers
# reading everything as string, phone has mixed values

pdf = pd.read_excel(f"{INPUT_PATH}/Customer.xlsx", dtype=str)
pdf = pdf.astype(object).where(pdf.notna(), None)

customers_df = (clean_column_names(spark.createDataFrame(pdf))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("Customer.xlsx")))

customers_df.write.mode("overwrite").saveAsTable(f"{BRONZE}.raw_customers")

# COMMAND ----------

for table in ["raw_orders", "raw_products", "raw_customers"]:
    print(f"{table}: {spark.table(f'{BRONZE}.{table}').count()} rows")
