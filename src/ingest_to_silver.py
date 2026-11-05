# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest to silver
# MAGIC
# MAGIC Standardizes the vendor delivery for `business_date` and merges it into
# MAGIC `silver_trades`, `silver_prices` and `silver_fx_rates`.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "zeroops_nav_demo")
dbutils.widgets.text("business_date", "")

# COMMAND ----------

from nav_pipeline import checks, transforms
from nav_pipeline.config import Tables, parse_business_date
from nav_pipeline.storage import upsert

tables = Tables(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
business_date = parse_business_date(dbutils.widgets.get("business_date"))
print(f"Ingesting delivery for {business_date}")

# COMMAND ----------

# Trades
trades = transforms.standardize_trades(spark.table(tables.raw_trades))
checks.assert_delivery_for(trades, "trade_date", business_date, "raw_trades")
checks.assert_not_null(
    trades, ["trade_id", "fund_id", "isin", "trade_date", "side", "quantity"], "silver_trades"
)
upsert(spark, trades, tables.silver_trades, ["trade_id"])

# COMMAND ----------

# End-of-day prices
prices = transforms.standardize_prices(spark.table(tables.raw_prices))
checks.assert_delivery_for(prices, "price_date", business_date, "raw_prices")
checks.assert_not_null(prices, ["isin", "price_date", "close_price"], "silver_prices")
checks.assert_positive(prices, "close_price", "silver_prices")
upsert(spark, prices, tables.silver_prices, ["isin", "price_date"])

# COMMAND ----------

# FX fixings
fx_rates = transforms.standardize_fx_rates(spark.table(tables.raw_fx_rates))
checks.assert_delivery_for(fx_rates, "rate_date", business_date, "raw_fx_rates")
checks.assert_not_null(
    fx_rates, ["currency", "rate_date", "fixing_time", "eur_per_unit"], "silver_fx_rates"
)
upsert(spark, fx_rates, tables.silver_fx_rates, ["currency", "rate_date", "fixing_time"])
