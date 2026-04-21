from posixpath import join
from typing import Optional

import polars as pl

from curtrail.common.augment.identity import with_identity_summary
from curtrail.source_log_cloudtrail import SourceLogCloudTrail
from curtrail.common.schema.aws_cloudtrail_schema import cloudtrail_all_fields

"""
The source of analysis data for logging entries, though
with a small substitution to get local JSONLD data
(easier to edit/read/test)
"""


class SourceLogCloudTrailTestData(SourceLogCloudTrail):

    def __init__(self, data_prefix: str, organisation: Optional[str] = None) -> None:
        super().__init__(data_prefix, organisation)

    def _scan_to_df(self, scan_root: str) -> pl.LazyFrame:
        return (
            pl.scan_ndjson(
                join(scan_root, "**", "*.jsonl"),
                schema_overrides=cloudtrail_all_fields,
                include_file_paths="__path__",
            )
            .with_columns(
                [
                    pl.col("__path__").str.extract(r"account=([^/]+)").alias("account"),
                    pl.col("__path__").str.extract(r"region=([^/]+)").alias("region"),
                    pl.col("__path__")
                    .str.extract(r"dt=(\d{4}-\d{2}-\d{2})")
                    .str.to_date()
                    .alias("dt"),
                ]
            )
            .drop("__path__")
        )

    def _augment(self, df: pl.DataFrame) -> pl.DataFrame:
        df = with_identity_summary(df)
        # df = self._augment_with_account_name(df)
        return df
