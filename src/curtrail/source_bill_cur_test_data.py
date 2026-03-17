from posixpath import join
import polars as pl

from curtrail.source_bill_cur import SourceBillCUR


class SourceBillCURTestData(SourceBillCUR):
    """
    SourceBillCUR variant that reads JSONL instead of Parquet — easy to
    edit and inspect in tests and local development.
    """

    def _scan_month(self, scan_root: str, year: int, month: int) -> pl.DataFrame:
        bill_period_scan = join(
            scan_root,
            f"BILLING_PERIOD={year:04d}-{month:02d}",
            "*.jsonl",
        )
        return (
            pl.scan_ndjson(bill_period_scan, schema_overrides=self.all_fields)
            .select(list(self.all_fields.keys()))
            .collect()
        )
