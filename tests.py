# Databricks notebook source
# MAGIC %run ./transformations

# COMMAND ----------

import unittest
from datetime import date
from decimal import Decimal

CUSTOMER_COLS = ["customer_id", "customer_name", "email", "phone", "address", "segment",
                 "country", "city", "state", "postal_code", "region"]
PRODUCT_COLS = ["product_id", "category", "sub_category", "product_name", "state", "price_per_product"]
ORDER_COLS = ["row_id", "order_id", "order_date", "ship_date", "ship_mode", "customer_id",
              "product_id", "quantity", "price", "discount", "profit"]


# only id and name matter for the tests, rest is dummy
def make_customer(customer_id, name):
    return (customer_id, name, "a@b.com", "123", "addr", "Consumer",
            "United States", "Dallas", "Texas", "75001", "Central")


def make_order(row_id, order_date, customer_id, product_id, profit):
    return (row_id, f"O-{row_id}", order_date, order_date, "Standard Class",
            customer_id, product_id, 1, 10.0, 0.0, profit)

# COMMAND ----------

class TestCustomers(unittest.TestCase):

    def clean_name(self, name):
        df = spark.createDataFrame([make_customer("C1", name)], CUSTOMER_COLS)
        return clean_customers(df).first()["customer_name"]

    def test_removes_digits_and_symbols(self):
        self.assertEqual(self.clean_name("Gary567 Hansen"), "Gary Hansen")
        self.assertEqual(self.clean_name("Pete@#$ Takahito"), "Pete Takahito")

    def test_fixes_extra_spaces(self):
        self.assertEqual(self.clean_name("Shahi  Hopkins "), "Shahi Hopkins")

    def test_keeps_accented_letters(self):
        self.assertEqual(self.clean_name("Peter Bühler"), "Peter Bühler")

    def test_joins_word_split_by_junk(self):
        self.assertEqual(self.clean_name("Victo 3456ria Brennan"), "Victoria Brennan")
        self.assertEqual(self.clean_name("Ad.       ..am Hart"), "Adam Hart")

    def test_removes_leading_and_trailing_symbols(self):
        self.assertEqual(self.clean_name("=--Katharine Harms"), "Katharine Harms")
        self.assertEqual(self.clean_name("[]-=;''Becky Pak"), "Becky Pak")
        self.assertEqual(self.clean_name("Joy Bell-"), "Joy Bell")

    def test_keeps_real_apostrophes_and_hyphens(self):
        self.assertEqual(self.clean_name("Mary O'Rourke"), "Mary O'Rourke")
        self.assertEqual(self.clean_name("Corey-Lock"), "Corey-Lock")

    def test_junk_only_name_becomes_null(self):
        self.assertIsNone(self.clean_name("&^*(5678"))

    def test_duplicate_customers_removed(self):
        df = spark.createDataFrame([make_customer("C1", "Ann"), make_customer("C1", "Ann")], CUSTOMER_COLS)
        self.assertEqual(clean_customers(df).count(), 1)

# COMMAND ----------

class TestProducts(unittest.TestCase):

    def test_one_row_per_product_id(self):
        df = spark.createDataFrame([
            ("P1", "Furniture", "Chairs", "Chair A", "Texas", 10.0),
            ("P1", "Furniture", "Chairs", "Chair B", "Ohio", 12.0),
            ("P2", "Technology", "Phones", "Phone", "Utah", 99.0),
        ], PRODUCT_COLS)

        self.assertEqual(clean_products(df).count(), 2)

# COMMAND ----------

class TestOrders(unittest.TestCase):

    def setUp(self):
        customers = spark.createDataFrame([make_customer("C1", "Gary567 Hansen")], CUSTOMER_COLS)

        # P1 twice on purpose, same as the real file
        products = spark.createDataFrame([
            ("P1", "Furniture", "Chairs", "Chair A", "Texas", 10.0),
            ("P1", "Furniture", "Chairs", "Chair B", "Ohio", 12.0),
        ], PRODUCT_COLS)

        self.raw_orders = spark.createDataFrame([
            make_order(1, "21/8/2016", "C1", "P1", 10.126),
            make_order(2, "3/10/2017", "C1", "P1", -5.004),
            make_order(3, "1/1/2017", "C1", "P_MISSING", 2.0),
        ], ORDER_COLS)

        result = enrich_orders(self.raw_orders, clean_customers(customers), clean_products(products))
        self.rows = {r.row_id: r for r in result.collect()}

    def test_no_orders_lost_or_duplicated(self):
        self.assertEqual(len(self.rows), self.raw_orders.count())
        self.assertEqual(sorted(self.rows), [1, 2, 3])

    def test_profit_rounded_to_2_decimals(self):
        self.assertEqual(self.rows[1].profit, Decimal("10.13"))
        self.assertEqual(self.rows[2].profit, Decimal("-5.00"))

    def test_dates_converted(self):
        self.assertEqual(self.rows[1].order_date, date(2016, 8, 21))
        self.assertEqual(self.rows[2].order_date, date(2017, 10, 3))

    def test_customer_details_added(self):
        self.assertEqual(self.rows[1].customer_name, "Gary Hansen")
        self.assertEqual(self.rows[1].country, "United States")

    def test_product_details_added(self):
        self.assertEqual(self.rows[1].category, "Furniture")
        self.assertEqual(self.rows[1].sub_category, "Chairs")

    def test_missing_product_marked_unknown(self):
        self.assertEqual(self.rows[3].category, "Unknown")
        self.assertEqual(self.rows[3].sub_category, "Unknown")

# COMMAND ----------

class TestAggregate(unittest.TestCase):

    def setUp(self):
        self.orders = spark.createDataFrame([
            (date(2016, 1, 5), "Furniture", "Chairs", "C1", "Ann", Decimal("10.00")),
            (date(2016, 6, 9), "Furniture", "Chairs", "C1", "Ann", Decimal("5.50")),
            (date(2017, 3, 1), "Furniture", "Chairs", "C1", "Ann", Decimal("-2.25")),
            (date(2016, 2, 2), "Technology", "Phones", "C2", "Bob", Decimal("7.00")),
        ], "order_date date, category string, sub_category string, customer_id string, "
           "customer_name string, profit decimal(18,2)")

        self.result = aggregate_profit(self.orders)

    def test_profit_summed_per_group(self):
        rows = {(r.order_year, r.category, r.customer_id): r.total_profit for r in self.result.collect()}

        self.assertEqual(rows[(2016, "Furniture", "C1")], Decimal("15.50"))
        self.assertEqual(rows[(2017, "Furniture", "C1")], Decimal("-2.25"))
        self.assertEqual(rows[(2016, "Technology", "C2")], Decimal("7.00"))

    def test_one_row_per_group(self):
        self.assertEqual(self.result.count(), 3)

    def test_total_profit_unchanged(self):
        total_before = self.orders.agg(F.sum("profit")).first()[0]
        total_after = self.result.agg(F.sum("total_profit")).first()[0]
        self.assertEqual(total_before, total_after)

# COMMAND ----------

unittest.main(argv=[""], exit=False, verbosity=2)
