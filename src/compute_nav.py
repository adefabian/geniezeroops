# Databricks notebook source
# MAGIC %md
# MAGIC # Compute NAV
# MAGIC
# MAGIC Values all positions for `business_date` with the EOD price and the official FX fixing,
# MAGIC then publishes NAV / AUM per fund. Data contracts run before anything is written to gold.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "zeroops_nav_demo")
dbutils.widgets.text("business_date", "")

# COMMAND ----------

from pyspark.sql import functions as F

from nav_pipeline import checks, transforms
from nav_pipeline.config import Tables, parse_business_date
from nav_pipeline.storage import upsert

MAX_DAILY_AUM_CHANGE = 0.25

tables = Tables(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
business_date = parse_business_date(dbutils.widgets.get("business_date"))
print(f"Computing NAV for {business_date}")

# COMMAND ----------

# Value positions
positions = spark.table(tables.silver_positions).filter(F.col("as_of_date") == F.lit(business_date))
prices = spark.table(tables.silver_prices).filter(F.col("price_date") == F.lit(business_date))
fx_rates = spark.table(tables.silver_fx_rates).filter(F.col("rate_date") == F.lit(business_date))

valued = transforms.value_positions(
    positions=positions,
    prices=prices,
    eod_fx_rates=transforms.latest_fx_rates(fx_rates),
    instruments=spark.table(tables.ref_instruments),
)

checks.assert_not_empty(valued, "gold_positions_valued")
checks.assert_unique(valued, ["fund_id", "isin", "nav_date"], "gold_positions_valued")
checks.assert_not_null(
    valued, ["close_price", "eur_per_unit", "market_value_eur"], "gold_positions_valued"
)
upsert(spark, valued, tables.gold_positions_valued, ["fund_id", "isin", "nav_date"])

# COMMAND ----------

# NAV per fund
nav = transforms.compute_nav(valued, spark.table(tables.ref_funds))
checks.assert_positive(nav, "nav_per_share", "gold_fund_nav")

if spark.catalog.tableExists(tables.gold_fund_nav):
    history = spark.table(tables.gold_fund_nav).filter(F.col("nav_date") < F.lit(business_date))
    previous_date = history.agg(F.max("nav_date")).first()[0]
    if previous_date is not None:
        checks.assert_aum_change_within(
            nav,
            history.filter(F.col("nav_date") == F.lit(previous_date)),
            MAX_DAILY_AUM_CHANGE,
            "gold_fund_nav",
        )

upsert(spark, nav, tables.gold_fund_nav, ["fund_id", "nav_date"])
display(nav.orderBy("fund_id"))
