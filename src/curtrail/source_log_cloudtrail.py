from posixpath import join
from typing import Optional

import polars as pl

from curtrail.common.augment.account_names import account_id_name
from curtrail.common.augment.geo import ip_as_city_name
from curtrail.common.augment.identity import with_identity_summary
from curtrail.common.schema.aws_cloudtrail_schema import cloudtrail_all_fields
from curtrail.source_filter import SourceFilter
from curtrail.source_log import SourceLog


class SourceLogCloudTrail(SourceLog):
    """
    Source log entries from a folder of CloudTrail Parquet data.
    """

    """A path to the CloudTrail data folders. Can be local or s3://..."""
    _data_prefix: str

    """ If present indicates that this CloudTrail source is structured
        as an organisation and hence has an organisation id
        in the directory structure """
    _organisation: Optional[str] = None

    def __init__(self, data_prefix: str, organisation: Optional[str] = None) -> None:
        self._data_prefix = data_prefix
        self._organisation = organisation

    def _scan_to_df(self, scan_root: str) -> pl.LazyFrame:
        """
        Scan Parquet files from the specified scan root with Hive partitioning
        and enforcing our known CloudTrail schema.
        """
        return pl.scan_parquet(
            scan_root,
            hive_partitioning=True,
            hive_schema={
                "account": pl.String,
                "region": pl.String,
                "year": pl.Int16,
                "month": pl.Int8,
                "day": pl.Int8,
            },
            schema=cloudtrail_all_fields,
            allow_missing_columns=False,
        )

    def _augment(self, df: pl.DataFrame) -> pl.DataFrame:
        df = with_identity_summary(df)
        df = self._augment_with_source_ip_address_city(df)
        df = self._augment_with_account_name(df)
        return df

    def fetch_data(self, source_filter: SourceFilter) -> pl.DataFrame:

        scan_root = (
            join(self._data_prefix, "CloudTrail", self._organisation)
            if self._organisation
            else join(self._data_prefix, "CloudTrail")
        )

        df = self._scan_to_df(scan_root)

        # Convert the local calendar-day range to exact UTC datetimes.  For
        # non-UTC timezones the UTC range may span an extra day on either side
        # of the local dates, so we use the UTC dates to filter partitions
        # (coarse pre-filter) and eventTime for the precise cut.
        utc_start, utc_end = source_filter.utc_datetime_range()

        # Hive partition pre-filter: build a synthetic date column from the
        # partition keys and compare it to the UTC date range.  This avoids
        # the broken independent year/month/day predicates and correctly
        # handles ranges that cross month or year boundaries.
        df = df.filter(
            (
                pl.col("year").cast(pl.String)
                + "-"
                + pl.col("month").cast(pl.String).str.zfill(2)
                + "-"
                + pl.col("day").cast(pl.String).str.zfill(2)
            )
            .str.to_date()
            .is_between(utc_start.date(), utc_end.date())
        )

        # Precise eventTime filter expressed in UTC milliseconds.
        df = df.filter(
            pl.col("eventTime").is_between(
                pl.lit(utc_start).dt.replace_time_zone(None).cast(pl.Datetime("ms")),
                pl.lit(utc_end).dt.replace_time_zone(None).cast(pl.Datetime("ms")),
            )
        )

        # filter by account
        if source_filter.accounts != "*":
            df = df.filter(pl.col("account").is_in(source_filter.accounts))

        # filter by region
        if source_filter.regions != "*":
            df = df.filter(pl.col("region").is_in(source_filter.regions))

        return source_filter.localize_datetimes(self._augment(df.collect()))

    def _augment_with_source_ip_address_city(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col("sourceIPAddress").map_batches(
                lambda combined: ip_as_city_name("umccr-cloudtrail-org-root", combined),
                return_dtype=pl.String,
            )
            # .("sourceIPAddressCity")
        )

    # def _augment_with_source_ip_address_country(self, df: pl.DataFrame) -> pl.DataFrame:
    #     return df.with_columns(
    #         pl.struct(["sourceIPAddress"])
    #         .map_batches(
    #             lambda combined: ip_as_iso_country_code("umccr-cloudtrail-org-root",
    #                                                     combined.struct.field("sourceIPAddress")),
    #             return_dtype=pl.String,
    #         )
    #         .alias("sourceIPAddressCountry")
    #     )

    def _augment_with_account_name(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.col("account").map_batches(
                lambda combined: account_id_name(combined),
                return_dtype=pl.String,
            )
            # .alias("accountName")
        )
