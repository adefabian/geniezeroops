"""Run parameters and fully qualified table names."""

from dataclasses import dataclass
from datetime import date


def parse_business_date(value: str | None) -> date:
    """Business date from a job parameter (ISO format); defaults to today when empty."""
    value = (value or "").strip()
    return date.fromisoformat(value) if value else date.today()


@dataclass(frozen=True)
class Tables:
    catalog: str
    schema: str

    def fq(self, name: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{name}`"

    # Vendor delivery (latest-delivery snapshots, overwritten by market_data_feed)
    @property
    def raw_trades(self) -> str:
        return self.fq("raw_trades")

    @property
    def raw_prices(self) -> str:
        return self.fq("raw_prices")

    @property
    def raw_fx_rates(self) -> str:
        return self.fq("raw_fx_rates")

    # Reference data
    @property
    def ref_funds(self) -> str:
        return self.fq("ref_funds")

    @property
    def ref_instruments(self) -> str:
        return self.fq("ref_instruments")

    @property
    def ref_opening_positions(self) -> str:
        return self.fq("ref_opening_positions")

    # Silver
    @property
    def silver_trades(self) -> str:
        return self.fq("silver_trades")

    @property
    def silver_prices(self) -> str:
        return self.fq("silver_prices")

    @property
    def silver_fx_rates(self) -> str:
        return self.fq("silver_fx_rates")

    @property
    def silver_positions(self) -> str:
        return self.fq("silver_positions")

    # Gold
    @property
    def gold_positions_valued(self) -> str:
        return self.fq("gold_positions_valued")

    @property
    def gold_fund_nav(self) -> str:
        return self.fq("gold_fund_nav")
