from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import List, Literal, Tuple
from zoneinfo import ZoneInfo

"""
The base filter that applies to the process of
sourcing the data.
These predicates should be set as narrow
 as possible for the analysis - as they enable
the system to avoid ingesting unneeded data. i.e. if an incident/cost
has occurred in only one account - then listing it means that no other
data from other accounts will be loaded.
"""


@dataclass()
class SourceFilter:

    # a tuple of start/end date (inclusive), interpreted as calendar days in `timezone`
    days_inclusive: Tuple[date, date] = field(init=True)

    # a list of accounts to filter to, or * to mean all accounts found
    accounts: List[str] | Literal["*"] = field(init=False, default="*")

    # a list of regions to filter to, or * to mean all regions found
    regions: List[str] | Literal["*"] = field(init=False, default="*")

    # the timezone in which days_inclusive calendar dates are interpreted
    timezone: ZoneInfo = field(default_factory=lambda: ZoneInfo("UTC"))

    def utc_datetime_range(self) -> Tuple[datetime, datetime]:
        """Return the filter range as a pair of UTC-aware datetimes.

        The start is midnight of the first local calendar day and the end is
        the last microsecond of the last local calendar day, both expressed
        in UTC.  Sources should use these values for precise timestamp
        filtering rather than comparing raw date components.
        """
        start = self.days_inclusive[0]
        end = self.days_inclusive[1]
        utc_start = datetime(
            start.year, start.month, start.day, 0, 0, 0, tzinfo=self.timezone
        ).astimezone(timezone.utc)
        utc_end = datetime(
            end.year, end.month, end.day, 23, 59, 59, 999999, tzinfo=self.timezone
        ).astimezone(timezone.utc)
        return utc_start, utc_end

    def localize_datetimes(self, df: "pl.DataFrame") -> "pl.DataFrame":
        """Convert all UTC datetime columns in `df` to the filter's timezone.

        Naive datetime columns are assumed to be UTC (as all AWS data sources
        store timestamps in UTC without tzinfo).  The result columns carry
        explicit timezone information so downstream code can format or
        arithmetic on them correctly.
        """
        import polars as pl

        tz_str = str(self.timezone)
        exprs = []
        for col_name, dtype in df.schema.items():
            if isinstance(dtype, pl.Datetime):
                if dtype.time_zone is None:
                    # Naive → label as UTC first, then convert
                    exprs.append(
                        pl.col(col_name)
                        .dt.replace_time_zone("UTC")
                        .dt.convert_time_zone(tz_str)
                    )
                elif dtype.time_zone != tz_str:
                    exprs.append(pl.col(col_name).dt.convert_time_zone(tz_str))
        return df.with_columns(exprs) if exprs else df
