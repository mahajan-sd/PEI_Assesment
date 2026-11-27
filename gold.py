# Databricks notebook source
# MAGIC %run ./transformations

# COMMAND ----------

SILVER = "maddy_test.silver"
GOLD = "maddy_test.gold"

# COMMAND ----------

# Profit by year, category, sub category and customer

profit_summary = aggregate_profit(spark.table(f"{SILVER}.orders"))
profit_summary.write.mode("overwrite").saveAsTable(f"{GOLD}.profit_summary")

print(f"profit_summary: {spark.table(f'{GOLD}.profit_summary').count()} rows")

# COMMAND ----------

# all reports below just roll up this table, no need to read orders again
profit_summary = spark.table(f"{GOLD}.profit_summary")

# COMMAND ----------

# Profit by Year

profit_by_year = (profit_summary
    .groupBy("order_year")
    .agg(F.sum("total_profit").alias("total_profit"))
    .orderBy("order_year"))

display(profit_by_year)

# COMMAND ----------

# Profit by Year + Product Category

profit_by_year_category = (profit_summary
    .groupBy("order_year", "category")
    .agg(F.sum("total_profit").alias("total_profit"))
    .orderBy("order_year", "category"))

display(profit_by_year_category)

# COMMAND ----------

# Profit by Customer

profit_by_customer = (profit_summary
    .groupBy("customer_id", "customer_name")
    .agg(F.sum("total_profit").alias("total_profit"))
    .orderBy(F.col("total_profit").desc()))

display(profit_by_customer)

# COMMAND ----------

# Profit by Customer + Year

profit_by_customer_year = (profit_summary
    .groupBy("customer_id", "customer_name", "order_year")
    .agg(F.sum("total_profit").alias("total_profit"))
    .orderBy("customer_id", "order_year"))

display(profit_by_customer_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5 - same aggregates using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profit by Year
# MAGIC SELECT order_year, SUM(total_profit) AS total_profit
# MAGIC FROM maddy_test.gold.profit_summary
# MAGIC GROUP BY order_year
# MAGIC ORDER BY order_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profit by Year + Product Category
# MAGIC SELECT order_year, category, SUM(total_profit) AS total_profit
# MAGIC FROM maddy_test.gold.profit_summary
# MAGIC GROUP BY order_year, category
# MAGIC ORDER BY order_year, category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profit by Customer
# MAGIC SELECT customer_id, customer_name, SUM(total_profit) AS total_profit
# MAGIC FROM maddy_test.gold.profit_summary
# MAGIC GROUP BY customer_id, customer_name
# MAGIC ORDER BY total_profit DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profit by Customer + Year
# MAGIC SELECT customer_id, customer_name, order_year, SUM(total_profit) AS total_profit
# MAGIC FROM maddy_test.gold.profit_summary
# MAGIC GROUP BY customer_id, customer_name, order_year
# MAGIC ORDER BY customer_id, order_year;
