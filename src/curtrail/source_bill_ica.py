import json
from datetime import timedelta
from typing import Set, Tuple
from posixpath import join
import polars as pl

from curtrail.source_bill import SourceBill
from curtrail.source_filter import SourceFilter
from curtrail.common.schema.ica_schema import (
    all_fields,
    csv_schema,
    metadata_name,
    metadata_struct_schema,
    usage_timestamp_name,
    billing_date_name,
    USAGE_TIMESTAMP_FORMAT,
    BILLING_DATE_FORMAT, metadata_start_time_name, metadata_completion_time_name,
)


def _metadata_to_json(value: str | None) -> str | None:
    """Parse ICA pipe-delimited metadata (key:value|...) into a JSON string."""
    if not value:
        return None
    result = {}
    for pair in value.split("|"):
        if not pair:
            continue
        key, _, val = pair.partition(":")
        result[key] = val or None
    return json.dumps(result)


def _parse_utc_datetime(col: pl.Expr) -> pl.Expr:
    """
    Parse ICA timestamp strings to UTC Datetime.

    Handles two formats seen in the data:
      - ISO 8601 with Z suffix: "2026-02-25T06:09:06.963Z"
      - Space-separated naive:  "2026-02-25 05:01:59.849"

    Both are normalised to naive then labeled as UTC. Microseconds
    are truncated as they are not relevant.
    """
    return (
        col
        .str.replace("T", " ", literal=True)
        .str.replace(r"Z$", "", literal=False)
        .str.to_datetime(format=None, strict=False, time_unit="ms")
        .dt.replace_time_zone("UTC")
        .dt.truncate("1s")
    )


class SourceBillIca(SourceBill):
    """
    The source of analysis data for ICA billing data
    """

    _data_prefix: str

    def __init__(self, data_prefix: str) -> None:
        self._data_prefix = data_prefix

    def _scan_month(self, scan_root: str, year: int, month: int) -> pl.DataFrame:
        bill_period_scan = join(
            scan_root,
            f"BILLING_PERIOD={year:04d}-{month:02d}",
            "*.csv",
        )

        df = (
            pl.scan_csv(bill_period_scan, schema=csv_schema).collect()
            .with_columns(
                pl.col(usage_timestamp_name).str.strptime(pl.Datetime("ms"), format=USAGE_TIMESTAMP_FORMAT),
                pl.col(billing_date_name).str.strptime(pl.Date(), format=BILLING_DATE_FORMAT),
                pl.col(metadata_name).map_elements(_metadata_to_json, return_dtype=pl.String),
            )
        )

        # guard against illumina introducing new metadata subfields
        known_keys = {f.name for f in metadata_struct_schema.fields}
        seen_keys: set[str] = set()
        for val in df[metadata_name].drop_nulls().to_list():
            seen_keys.update(json.loads(val).keys())
        unknown_keys = seen_keys - known_keys
        if unknown_keys:
            raise ValueError(
                f"ICA metadata (column METADATA) contains the following unknown fields not in schema: {sorted(unknown_keys)}"
            )

        df = df.with_columns(
            pl.col(metadata_name).str.json_decode(dtype=metadata_struct_schema)
        )

        # now convert the randomly formatted metadata dates into real dates
        df = df.with_columns(
            pl.col(metadata_name).struct.with_fields(
                _parse_utc_datetime(pl.field(metadata_start_time_name)).alias(metadata_start_time_name),
                _parse_utc_datetime(pl.field(metadata_completion_time_name)).alias(metadata_completion_time_name),
            )
        )

        return df

    def fetch_data(self, source_filter: SourceFilter) -> pl.DataFrame:
        cur_df = pl.DataFrame(schema=all_fields)

        utc_start, utc_end = source_filter.utc_datetime_range()

        month_set: Set[Tuple[int, int]] = set()

        current = utc_start.date()
        while current <= utc_end.date():
            month_set.add((current.year, current.month))
            current = current + timedelta(days=1)

        # stack all the bills from any month that was in our date range
        for year, month in month_set:
            cur_df = cur_df.vstack(self._scan_month(self._data_prefix, year, month))

        utc_start_naive = pl.lit(utc_start).dt.replace_time_zone(None).cast(pl.Datetime("ms"))
        utc_end_naive = pl.lit(utc_end).dt.replace_time_zone(None).cast(pl.Datetime("ms"))

        cur_df = cur_df.filter(pl.col(billing_date_name).ge(utc_start_naive))
        cur_df = cur_df.filter(pl.col(billing_date_name).le(utc_end_naive))

        return source_filter.localize_datetimes(cur_df)
