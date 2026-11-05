# Databricks notebook source
# MAGIC %md
# MAGIC # Build positions
# MAGIC
# MAGIC Holdings per fund and instrument at the end of `business_date`
# MAGIC (opening book plus all trades), merged into `silver_positions`.

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
print(f"Building positions as of {business_date}")

# COMMAND ----------

positions = transforms.compute_positions(
    opening=spark.table(tables.ref_opening_positions),
    trades=spark.table(tables.silver_trades),
    as_of=business_date,
)

checks.assert_not_empty(positions, "silver_positions")
checks.assert_unique(positions, ["fund_id", "isin", "as_of_date"], "silver_positions")
checks.assert_positive(positions, "quantity", "silver_positions")
upsert(spark, positions, tables.silver_positions, ["fund_id", "isin", "as_of_date"])
