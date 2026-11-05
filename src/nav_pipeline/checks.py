"""Data contracts that fail the task instead of publishing bad numbers."""

from datetime import date
from functools import reduce
from operator import or_

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataContractError(Exception):
    """Raised when a dataset violates its contract."""


def _sample(df: DataFrame, n: int = 5) -> list[dict]:
    return [row.asDict() for row in df.limit(n).collect()]


def assert_unique(df: DataFrame, keys: list[str], name: str) -> None:
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1)
    n = duplicates.count()
    if n:
        raise DataContractError(
            f"{name}: {n} duplicate key(s) on {keys}. Sample: {_sample(duplicates)}"
        )


def assert_not_null(df: DataFrame, columns: list[str], name: str) -> None:
    violations = df.filter(reduce(or_, [F.col(c).isNull() for c in columns]))
    n = violations.count()
    if n:
        raise DataContractError(
            f"{name}: {n} row(s) with NULL in {columns}. Sample: {_sample(violations)}"
        )


def assert_positive(df: DataFrame, column: str, name: str) -> None:
    violations = df.filter(F.col(column) <= 0)
    n = violations.count()
    if n:
        raise DataContractError(
            f"{name}: {n} row(s) with non-positive {column}. Sample: {_sample(violations)}"
        )


def assert_not_empty(df: DataFrame, name: str) -> None:
    if df.limit(1).count() == 0:
        raise DataContractError(f"{name}: no rows")


def assert_delivery_for(df: DataFrame, date_column: str, expected: date, name: str) -> None:
    """The vendor delivery must be for the business date being processed."""
    delivered = sorted(r[0] for r in df.select(date_column).distinct().collect())
    if delivered != [expected]:
        raise DataContractError(
            f"{name}: delivery is for {[d.isoformat() for d in delivered]}, "
            f"expected {expected.isoformat()}"
        )


def assert_aum_change_within(
    nav: DataFrame, previous_nav: DataFrame, max_relative_change: float, name: str
) -> None:
    """Fail if any fund's AUM moved by more than max_relative_change versus its previous NAV."""
    prev = previous_nav.select("fund_id", F.col("aum_eur").alias("previous_aum_eur"))
    violations = (
        nav.join(prev, "fund_id")
        .withColumn(
            "relative_change",
            F.abs(F.col("aum_eur") / F.col("previous_aum_eur") - 1),
        )
        .filter(F.col("relative_change") > max_relative_change)
        .select("fund_id", "nav_date", "aum_eur", "previous_aum_eur", "relative_change")
    )
    n = violations.count()
    if n:
        raise DataContractError(
            f"{name}: AUM of {n} fund(s) moved more than {max_relative_change:.0%} "
            f"day over day. Sample: {_sample(violations)}"
        )
