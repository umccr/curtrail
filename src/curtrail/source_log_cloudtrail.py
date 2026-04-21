from posixpath import join
from typing import Optional

import polars as pl

from curtrail.common.augment.account_names import _account_id_name_batch, augment_with_account_name
from curtrail.common.augment.geo import ip_as_city_name, augment_with_source_ip_address_city
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
        df = pl.scan_parquet(
            scan_root,
            hive_partitioning=True,
            hive_schema={
                "account": pl.String,
                "region": pl.String,
                "dt": pl.Date
            },
            schema=cloudtrail_all_fields,
            allow_missing_columns=False,
        )

        return df

    def _augment(self, df: pl.DataFrame) -> pl.DataFrame:
        df = with_identity_summary(df)
        df = augment_with_source_ip_address_city(df)
        df = augment_with_account_name(df)
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

        # Hive partition pre-filter on the dt=YYYY-MM-DD partition column.
        df = df.filter(
            pl.col("dt").is_between(utc_start.date(), utc_end.date())
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

        #print(df.explain(optimized=False))
        #print(df.explain(optimized=True))

        #df, timings = df.profile()
        #print(timings)

        df = df.collect()

        return source_filter.localize_datetimes(self._augment(df))


