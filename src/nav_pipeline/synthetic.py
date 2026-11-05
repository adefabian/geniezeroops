"""Simulated market data vendor.

Produces deterministic synthetic deliveries for a business date: running the feed twice for the
same date yields identical data, so downstream MERGEs stay idempotent. All names and identifiers
are fictional.
"""

import hashlib
import math
import random
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

OPENING_DATE = date(2026, 1, 1)

# Delivery format versions the vendor supports. v2 renames the price field to the vendor's
# terminal naming convention.
PRICE_FIELD_BY_FEED_VERSION = {
    "v1": "close_price",
    "v2": "px_last",
}

# EUR value of one unit of the currency
FX_BASE_RATES = {"EUR": 1.0, "USD": 0.92, "GBP": 1.17, "CHF": 1.05, "JPY": 0.0061}
FX_FIXING_TIMES = ["09:00", "13:00", "16:00"]

TRADES_PER_FUND_PER_DAY = 100


@dataclass(frozen=True)
class Fund:
    fund_id: str
    fund_name: str
    strategy: str
    shares_outstanding: int


@dataclass(frozen=True)
class Instrument:
    isin: str
    name: str
    asset_class: str
    currency: str
    base_price: float


FUNDS = [
    Fund("F001", "Aurora Global Equity", "EQUITY", 4_000_000),
    Fund("F002", "Aurora Euro Bond", "BOND", 6_000_000),
    Fund("F003", "Aurora Multi-Asset ESG", "MULTI_ASSET", 3_000_000),
    Fund("F004", "Aurora Asia Pacific Equity", "EQUITY", 2_500_000),
    Fund("F005", "Aurora US Dividend", "EQUITY", 3_500_000),
]


def _unit(*parts) -> float:
    """Deterministic pseudo-random number in [0, 1) derived from the given parts."""
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def _build_instruments() -> list[Instrument]:
    rng = random.Random("instruments")
    currencies = list(FX_BASE_RATES)
    instruments = []
    for i in range(1, 41):
        asset_class = "EQUITY" if i <= 28 else "BOND"
        currency = currencies[i % len(currencies)]
        base = rng.uniform(15, 400) if asset_class == "EQUITY" else rng.uniform(92, 108)
        if currency == "JPY":
            base *= 150
        instruments.append(
            Instrument(
                isin=f"XX{i:010d}",
                name=f"Synthetic {asset_class.title()} {i:02d}",
                asset_class=asset_class,
                currency=currency,
                base_price=round(base, 2),
            )
        )
    return instruments


INSTRUMENTS = _build_instruments()
INSTRUMENT_BY_ISIN = {i.isin: i for i in INSTRUMENTS}


def _build_holdings() -> dict[str, list[str]]:
    equities = [i.isin for i in INSTRUMENTS if i.asset_class == "EQUITY"]
    bonds = [i.isin for i in INSTRUMENTS if i.asset_class == "BOND"]
    holdings = {}
    for fund in FUNDS:
        rng = random.Random(fund.fund_id)
        if fund.strategy == "EQUITY":
            holdings[fund.fund_id] = rng.sample(equities, 15)
        elif fund.strategy == "BOND":
            holdings[fund.fund_id] = rng.sample(bonds, 10)
        else:
            holdings[fund.fund_id] = rng.sample(equities, 8) + rng.sample(bonds, 6)
    return holdings


HOLDINGS = _build_holdings()


def eod_price(isin: str, d: date) -> float:
    instrument = INSTRUMENT_BY_ISIN[isin]
    t = (d - OPENING_DATE).days
    trend = 1 + 0.12 * math.sin(t / 25 + _unit(isin, "phase") * 2 * math.pi)
    noise = 1 + (_unit(isin, d, "px") - 0.5) * 0.02
    return round(instrument.base_price * trend * noise, 4)


def fx_rate(currency: str, d: date, fixing_time: str) -> float:
    if currency == "EUR":
        return 1.0
    t = (d - OPENING_DATE).days
    drift = 1 + 0.03 * math.sin(t / 40 + _unit(currency, "phase") * 2 * math.pi)
    intraday = 1 + (_unit(currency, d, fixing_time) - 0.5) * 0.004
    return round(FX_BASE_RATES[currency] * drift * intraday, 6)


# ---------------------------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------------------------

def reference_funds(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("fund_id", StringType()),
            StructField("fund_name", StringType()),
            StructField("strategy", StringType()),
            StructField("base_currency", StringType()),
            StructField("shares_outstanding", LongType()),
        ]
    )
    rows = [(f.fund_id, f.fund_name, f.strategy, "EUR", f.shares_outstanding) for f in FUNDS]
    return spark.createDataFrame(rows, schema)


def reference_instruments(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("isin", StringType()),
            StructField("instrument_name", StringType()),
            StructField("asset_class", StringType()),
            StructField("currency", StringType()),
        ]
    )
    rows = [(i.isin, i.name, i.asset_class, i.currency) for i in INSTRUMENTS]
    return spark.createDataFrame(rows, schema)


def reference_opening_positions(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("fund_id", StringType()),
            StructField("isin", StringType()),
            StructField("opening_date", DateType()),
            StructField("quantity", LongType()),
        ]
    )
    rows = []
    for fund_id, isins in HOLDINGS.items():
        rng = random.Random(f"opening|{fund_id}")
        for isin in isins:
            rows.append((fund_id, isin, OPENING_DATE, rng.randint(2_000, 5_000) * 100))
    return spark.createDataFrame(rows, schema)


# ---------------------------------------------------------------------------------------------
# Daily delivery
# ---------------------------------------------------------------------------------------------

def _check_feed_version(feed_version: str) -> None:
    if feed_version not in PRICE_FIELD_BY_FEED_VERSION:
        raise ValueError(
            f"Unknown feed_schema_version {feed_version!r}; "
            f"expected one of {sorted(PRICE_FIELD_BY_FEED_VERSION)}"
        )


def _with_delivery_metadata(df: DataFrame, feed_version: str) -> DataFrame:
    return df.withColumn("feed_version", F.lit(feed_version)).withColumn(
        "delivered_at", F.current_timestamp()
    )


def deliver_trades(spark: SparkSession, d: date, feed_version: str = "v1") -> DataFrame:
    _check_feed_version(feed_version)
    schema = StructType(
        [
            StructField("trade_id", StringType()),
            StructField("fund_id", StringType()),
            StructField("isin", StringType()),
            StructField("trade_date", DateType()),
            StructField("side", StringType()),
            StructField("quantity", LongType()),
            StructField("trade_price", DoubleType()),
            StructField("currency", StringType()),
        ]
    )
    rows = []
    for fund in FUNDS:
        rng = random.Random(f"trades|{fund.fund_id}|{d.isoformat()}")
        for k in range(TRADES_PER_FUND_PER_DAY):
            isin = rng.choice(HOLDINGS[fund.fund_id])
            side = "BUY" if rng.random() < 0.55 else "SELL"
            quantity = rng.randint(1, 50) * 100
            price = round(eod_price(isin, d) * (1 + (rng.random() - 0.5) * 0.01), 4)
            rows.append(
                (
                    f"{fund.fund_id}-{d:%Y%m%d}-{k:04d}",
                    fund.fund_id,
                    isin,
                    d,
                    side,
                    quantity,
                    price,
                    INSTRUMENT_BY_ISIN[isin].currency,
                )
            )
    return _with_delivery_metadata(spark.createDataFrame(rows, schema), feed_version)


def deliver_prices(spark: SparkSession, d: date, feed_version: str = "v1") -> DataFrame:
    _check_feed_version(feed_version)
    schema = StructType(
        [
            StructField("isin", StringType()),
            StructField("price_date", DateType()),
            StructField(PRICE_FIELD_BY_FEED_VERSION[feed_version], DoubleType()),
            StructField("currency", StringType()),
        ]
    )
    rows = [(i.isin, d, eod_price(i.isin, d), i.currency) for i in INSTRUMENTS]
    return _with_delivery_metadata(spark.createDataFrame(rows, schema), feed_version)


def deliver_fx_rates(spark: SparkSession, d: date, feed_version: str = "v1") -> DataFrame:
    _check_feed_version(feed_version)
    schema = StructType(
        [
            StructField("currency", StringType()),
            StructField("rate_date", DateType()),
            StructField("fixing_time", StringType()),
            StructField("eur_per_unit", DoubleType()),
        ]
    )
    rows = [
        (ccy, d, fixing, fx_rate(ccy, d, fixing))
        for ccy in FX_BASE_RATES
        for fixing in FX_FIXING_TIMES
    ]
    return _with_delivery_metadata(spark.createDataFrame(rows, schema), feed_version)
