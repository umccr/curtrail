from dataclasses import dataclass
from typing import Optional
from analysis_context import AnalysisContext
import polars as pl
from posixpath import join

from common.augment.account_names import account_id_name
from common.augment.geo import ip_as_city_name
from common.schema.aws_cloudtrail_schema import cloudtrail_all_fields

"""
The source of analysis data for logging entries.
"""


@dataclass()
class AnalysisSourceCloudTrail:
    """A path to the CloudTrail data folders. Can be local or s3://..."""

    data_prefix: str

    """ If present indicates that this CloudTrail source is structured
        as an organisation and hence has an organisation id
        in the directory structure """
    organisation: Optional[str] = None

    def get_cloudtrail_data(self, context: AnalysisContext) -> pl.DataFrame:

        scan_root = (
            join(self.data_prefix, "CloudTrail", self.organisation)
            if self.organisation
            else join(self.data_prefix, "CloudTrail")
        )

        df = pl.scan_parquet(
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
            allow_missing_columns=False
        )

        if context.accounts != "*":
            df = df.filter(pl.col("account").is_in(context.accounts))

        if context.regions != "*":
            df = df.filter(pl.col("region").is_in(context.regions))

        start = context.days_inclusive[0]
        end = context.days_inclusive[1]
        df = df.filter(pl.col("year").ge(start.year).and_(pl.col("year").le(end.year)))
        df = df.filter(
            pl.col("month").ge(start.month).and_(pl.col("month").le(end.month))
        )
        df = df.filter(pl.col("day").ge(start.day).and_(pl.col("day").le(end.day)))

        df = df.collect()

        df = self._augment_with_source_ip_address_city(df)
        df = self._augment_with_account_name(df)

        return df

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
