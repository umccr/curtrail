from datetime import timedelta
from typing import Set, Tuple
from posixpath import join
import polars as pl

from curtrail.source_bill import SourceBill
from curtrail.source_filter import SourceFilter
from curtrail.common.schema.aws_cur_schema import (
    bill_fields,
    tags_fields,
    line_item_fields,
    pricing_fields,
    product_fields,
    identity_fields,
    line_item_usage_start_date_name,
    line_item_usage_end_date_name,
)


class SourceBillCUR(SourceBill):
    """
    The source of analysis data for AWS CUR billing data.
    """

    _data_prefix: str

    def __init__(self, data_prefix: str) -> None:
        self._data_prefix = data_prefix

    all_fields = (
        bill_fields
        | product_fields
        | identity_fields
        | line_item_fields
        | pricing_fields
        | tags_fields
    )

    def _scan_month(self, scan_root: str, year: int, month: int) -> pl.DataFrame:
        bill_period_scan = join(
            scan_root,
            f"BILLING_PERIOD={year:04d}-{month:02d}",
            "*.snappy.parquet",
        )
        return pl.scan_parquet(
            bill_period_scan, schema=self.all_fields, extra_columns="ignore"
        ).collect()

    def fetch_data(self, source_filter: SourceFilter) -> pl.DataFrame:

        scan_root = join(self._data_prefix, "data")

        cur_df = pl.DataFrame(schema=self.all_fields)

        # Convert local calendar days to a UTC datetime range, then derive the
        # set of billing months to load from the UTC dates.
        utc_start, utc_end = source_filter.utc_datetime_range()

        month_set: Set[Tuple[int, int]] = set()
        current = utc_start.date()
        while current <= utc_end.date():
            month_set.add((current.year, current.month))
            current = current + timedelta(days=1)

        for year, month in month_set:
            cur_df = cur_df.vstack(self._scan_month(scan_root, year, month))

        # Filter to the precise UTC range expressed as naive UTC milliseconds
        # (CUR timestamps are stored as UTC without tzinfo).
        utc_start_naive = (
            pl.lit(utc_start).dt.replace_time_zone(None).cast(pl.Datetime("ms"))
        )
        utc_end_naive = (
            pl.lit(utc_end).dt.replace_time_zone(None).cast(pl.Datetime("ms"))
        )

        cur_df = cur_df.filter(
            pl.col(line_item_usage_start_date_name).ge(utc_start_naive)
        )
        cur_df = cur_df.filter(pl.col(line_item_usage_end_date_name).le(utc_end_naive))

        return source_filter.localize_datetimes(cur_df)
