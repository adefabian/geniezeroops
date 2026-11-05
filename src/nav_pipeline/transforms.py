"""Pure DataFrame transformations of the fund NAV pipeline (no I/O)."""

from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------------------------
# Silver: standardize the vendor delivery
# ---------------------------------------------------------------------------------------------

def standardize_trades(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("trade_id"),
        F.col("fund_id"),
        F.col("isin"),
        F.col("trade_date").cast("date").alias("trade_date"),
        F.upper(F.trim(F.col("side"))).alias("side"),
        F.col("quantity").cast("bigint").alias("quantity"),
        F.col("trade_price").cast("double").alias("trade_price"),
        F.upper(F.col("currency")).alias("currency"),
    )


def standardize_prices(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("isin"),
        F.col("price_date").cast("date").alias("price_date"),
        F.col("close_price").cast("double").alias("close_price"),
        F.upper(F.col("currency")).alias("currency"),
    )


def standardize_fx_rates(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.upper(F.col("currency")).alias("currency"),
        F.col("rate_date").cast("date").alias("rate_date"),
        F.col("fixing_time"),
        F.col("eur_per_unit").cast("double").alias("eur_per_unit"),
    )


# ---------------------------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------------------------

def signed_quantity() -> Column:
    return F.when(F.col("side") == "BUY", F.col("quantity")).otherwise(-F.col("quantity"))


def compute_positions(opening: DataFrame, trades: DataFrame, as_of: date) -> DataFrame:
    """Holdings per fund and instrument at the end of `as_of`: opening book plus all trades."""
    flows = trades.filter(F.col("trade_date") <= F.lit(as_of)).select(
        "fund_id", "isin", signed_quantity().alias("quantity")
    )
    return (
        opening.select("fund_id", "isin", "quantity")
        .unionByName(flows)
        .groupBy("fund_id", "isin")
        .agg(F.sum("quantity").alias("quantity"))
        .withColumn("as_of_date", F.lit(as_of).cast("date"))
        .select("fund_id", "isin", "as_of_date", "quantity")
    )


# ---------------------------------------------------------------------------------------------
# Valuation
# ---------------------------------------------------------------------------------------------

def latest_fx_rates(fx_rates: DataFrame) -> DataFrame:
    """One rate per currency and day: the last intraday fixing is the official EOD rate."""
    latest_first = Window.partitionBy("currency", "rate_date").orderBy(F.col("fixing_time").desc())
    return (
        fx_rates.withColumn("_rank", F.row_number().over(latest_first))
        .filter(F.col("_rank") == 1)
        .select("currency", "rate_date", "eur_per_unit")
    )


def value_positions(
    positions: DataFrame, prices: DataFrame, eod_fx_rates: DataFrame, instruments: DataFrame
) -> DataFrame:
    """Market value of every position in local currency and in EUR."""
    px = prices.select(
        "isin", F.col("price_date").alias("as_of_date"), "close_price", "currency"
    )
    fx = eod_fx_rates.select(
        "currency", F.col("rate_date").alias("as_of_date"), "eur_per_unit"
    )
    return (
        positions.join(px, ["isin", "as_of_date"], "left")
        .join(fx, ["currency", "as_of_date"], "left")
        .join(instruments.select("isin", "asset_class"), "isin", "left")
        .withColumn("market_value_local", F.col("quantity") * F.col("close_price"))
        .withColumn("market_value_eur", F.col("market_value_local") * F.col("eur_per_unit"))
        .select(
            "fund_id",
            "isin",
            F.col("as_of_date").alias("nav_date"),
            "asset_class",
            "currency",
            "quantity",
            "close_price",
            "eur_per_unit",
            F.round("market_value_local", 2).alias("market_value_local"),
            F.round("market_value_eur", 2).alias("market_value_eur"),
        )
    )


def compute_nav(valued_positions: DataFrame, funds: DataFrame) -> DataFrame:
    """NAV, AUM and number of holdings per fund and day."""
    return (
        valued_positions.groupBy("fund_id", "nav_date")
        .agg(
            F.sum("market_value_eur").alias("aum_eur"),
            F.count(F.lit(1)).alias("n_positions"),
        )
        .join(funds.select("fund_id", "fund_name", "shares_outstanding"), "fund_id")
        .select(
            "fund_id",
            "fund_name",
            "nav_date",
            F.round("aum_eur", 2).alias("aum_eur"),
            "shares_outstanding",
            F.round(F.col("aum_eur") / F.col("shares_outstanding"), 4).alias("nav_per_share"),
            "n_positions",
        )
    )
