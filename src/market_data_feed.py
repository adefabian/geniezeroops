# Databricks notebook source
# MAGIC %md
# MAGIC # Market data feed (simulated vendor)
# MAGIC
# MAGIC Delivers the trade blotter, end-of-day prices and intraday FX fixings for `business_date`
# MAGIC as latest-delivery snapshot tables (`raw_trades`, `raw_prices`, `raw_fx_rates`) and
# MAGIC refreshes the reference data (`ref_funds`, `ref_instruments`, `ref_opening_positions`).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "zeroops_nav_demo")
dbutils.widgets.text("business_date", "")
dbutils.widgets.text("feed_schema_version", "v1")

# COMMAND ----------

from nav_pipeline import synthetic
from nav_pipeline.config import Tables, parse_business_date
from nav_pipeline.storage import overwrite

tables = Tables(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
business_date = parse_business_date(dbutils.widgets.get("business_date"))
feed_version = dbutils.widgets.get("feed_schema_version")
print(f"Delivering {business_date} (feed {feed_version}) to {tables.catalog}.{tables.schema}")

# COMMAND ----------

# Reference data
overwrite(synthetic.reference_funds(spark), tables.ref_funds)
overwrite(synthetic.reference_instruments(spark), tables.ref_instruments)
overwrite(synthetic.reference_opening_positions(spark), tables.ref_opening_positions)

# COMMAND ----------

# Daily delivery
overwrite(synthetic.deliver_trades(spark, business_date, feed_version), tables.raw_trades)
overwrite(synthetic.deliver_prices(spark, business_date, feed_version), tables.raw_prices)
overwrite(synthetic.deliver_fx_rates(spark, business_date, feed_version), tables.raw_fx_rates)
