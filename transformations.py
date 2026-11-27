# Databricks notebook source
from pyspark.sql import functions as F


# note: digits used as letters (Bi1l, Wa55erman) can't be fixed by a rule,
# grouping is done on customer_id so totals are not affected

def clean_customer_name(name):
    # junk inside a word, eg "Victo 3456ria" -> "Victoria"
    name = F.regexp_replace(name, r"(?<=\p{L})[^\p{L}]*[^\p{L}\s'\-](?=\p{L})", "")

    # remove remaining digits / symbols
    name = F.regexp_replace(name, r"[^\p{L}\s'\-]", "")

    # keep ' and - only between letters (O'Brien, Corey-Lock)
    name = F.regexp_replace(name, r"(?<!\p{L})['\-]|['\-](?!\p{L})", "")

    name = F.trim(F.regexp_replace(name, r"\s+", " "))
    return F.when(name == "", None).otherwise(name)


def clean_customers(raw_customers):
    return (raw_customers
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("customer_name", clean_customer_name(F.col("customer_name")))
        .dropDuplicates(["customer_id"])
        .select("customer_id", "customer_name", "email", "phone", "address", "segment",
                "country", "city", "state", "postal_code", "region"))


def clean_products(raw_products):
    return (raw_products
        .withColumn("product_id", F.trim("product_id"))
        .withColumn("product_name", F.trim("product_name"))
        .withColumn("category", F.trim("category"))
        .withColumn("sub_category", F.trim("sub_category"))
        # same product_id comes more than once in the file
        .dropDuplicates(["product_id"])
        .select("product_id", "product_name", "category", "sub_category"))


def enrich_orders(raw_orders, customers, products):
    # dates in the file look like 21/8/2016
    orders = (raw_orders
        .withColumn("order_date", F.to_date("order_date", "d/M/yyyy"))
        .withColumn("ship_date", F.to_date("ship_date", "d/M/yyyy"))
        .withColumn("profit", F.round("profit", 2).cast("decimal(18,2)"))
        .withColumn("quantity", F.col("quantity").cast("int")))

    customer_cols = customers.select("customer_id", "customer_name", "country")
    product_cols = products.select("product_id", "category", "sub_category")

    # customers and products are small, broadcast them to avoid a shuffle
    # left join so no order gets dropped
    return (orders
        .join(F.broadcast(customer_cols), "customer_id", "left")
        .join(F.broadcast(product_cols), "product_id", "left")
        # some orders have products which are not in the product file
        .fillna("Unknown", subset=["category", "sub_category"])
        .select("row_id", "order_id", "order_date", "ship_date", "ship_mode",
                "customer_id", "customer_name", "country",
                "product_id", "category", "sub_category",
                "quantity", "price", "discount", "profit"))


def aggregate_profit(orders):
    return (orders
        .withColumn("order_year", F.year("order_date"))
        .groupBy("order_year", "category", "sub_category", "customer_id", "customer_name")
        .agg(F.sum("profit").cast("decimal(18,2)").alias("total_profit")))
