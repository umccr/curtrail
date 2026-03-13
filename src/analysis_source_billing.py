from dataclasses import dataclass
from datetime import date, timedelta
from typing import Set, Tuple
from posixpath import join
import polars as pl

from analysis_context import AnalysisContext
from common.schema.aws_cur_schema import (
    bill_fields,
    tags_fields,
    line_item_fields,
    pricing_fields,
    product_fields,
    identity_fields,
    line_item_usage_start_date_name,
    line_item_usage_end_date_name,
)

"""
The source of analysis data for CUR bill data.
"""


@dataclass()
class AnalysisSourceCur:
    """A path to the CUR data folders. Can be local or s3://..."""

    data_prefix: str

    all_fields = (
        bill_fields
        | product_fields
        | identity_fields
        | line_item_fields
        | pricing_fields
        | tags_fields
    )

    def get_cur_data(self, ac: AnalysisContext) -> pl.DataFrame:

        scan_root = join(self.data_prefix, "data")

        cur_df = pl.DataFrame(schema=self.all_fields)

        # a set of year/months
        month_set: Set[Tuple[int, int]] = set()

        start_date: date = ac.days_inclusive[0]
        end_date: date = ac.days_inclusive[1]

        # find the set of months that contain our days
        current = start_date
        while current <= end_date:
            month_set.add((current.year, current.month))
            current = current + timedelta(days=1)

        # now fetch each of these months bill data and put them into the larger data frame
        for month in month_set:
            bill_period_scan = join(
                scan_root,
                f"BILLING_PERIOD={month[0]:04d}-{month[1]:02d}",
                "*.snappy.parquet",
            )

            df = pl.scan_parquet(
                bill_period_scan, schema=self.all_fields, extra_columns="ignore"
            ).collect()

            cur_df = cur_df.vstack(df)

        # finally we need to restrict the bill data back down to just the *days* that
        # we have been asked for (bill data comes per month)
        cur_df = cur_df.filter(
            pl.col(line_item_usage_start_date_name).dt.date().ge(start_date)
        )
        cur_df = cur_df.filter(
            pl.col(line_item_usage_end_date_name).dt.date().le(end_date)
        )

        return cur_df
