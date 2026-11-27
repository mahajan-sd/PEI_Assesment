# Sales Data Pipeline (Databricks + PySpark)
 
Loads raw sales data (Orders, Products, Customers), cleans it and builds profit aggregates using a bronze / silver / gold setup.
 
## Notebooks (run in this order)
1. `ddl` - creates schemas and tables
2. `bronze` - loads the 3 source files as-is
3. `silver` - cleans customers/products and builds the enriched orders table
4. `gold` - profit aggregate table + Task 5 reports
5. `tests` - unit tests (unittest)
`transformations` holds the shared functions and is loaded by the others with `%run`.
 
## Data issues handled
- Customer names had digits/symbols in them (`Gary567 Hansen`), cleaned with regex
- Same product_id appeared more than once, kept one row per id so joins don't double profit
- 204 orders have products missing from the product file, kept them as `Unknown`
- Product names contain `""`, so the CSV is read with `escape='"'`
- Dates come as text (`21/8/2016`), converted to proper dates
## Assumptions
- Profit is rounded to 2 decimals per order, then summed
- Grouping is on customer_id, a few names (eg `Bi1l`) can't be fully fixed by rules
 
