from datetime import date
from pathlib import Path

import polars as pl

from curtrail import LogData, BillDataAws, BillDataIca, SourceBillIca, SourceFilter
from curtrail.source_log_cloudtrail_test_data import SourceLogCloudTrailTestData

# FOR OCCASIONS WHEN WE JUST WANT TO RUN SOME CODE TO ADHOC TESTING
# NOT PART OF ANY REGRESSION SUITE

TEST_DATA = Path(__file__).parent.parent / "test-data"

ICA_TEST_DATA_SOURCE = SourceBillIca(data_prefix=str(TEST_DATA / "ica"))
CUR_TEST_DATA_SOURCE = SourceBillIca(data_prefix=str(TEST_DATA / "cur"))
CLOUDTRAIL_TEST_DATA_SOURCE = SourceLogCloudTrailTestData(data_prefix=str(TEST_DATA / "cloudtrail"))

ALL_2025_2026 = SourceFilter(days_inclusive=(date(2025, 4, 1), date(2026, 3, 31)))
OCT_2025 = SourceFilter(days_inclusive=(date(2025, 10, 1), date(2025, 10, 31)))
FEB_2026 = SourceFilter(days_inclusive=(date(2026, 2, 1), date(2026, 2, 28)))
MARCH_2026 = SourceFilter(days_inclusive=(date(2026, 3, 1), date(2026, 3, 31)))

pl.Config.set_tbl_width_chars(500)
pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_cols(-1)

cur = BillDataAws([CUR_TEST_DATA_SOURCE], ALL_2025_2026)
ica = BillDataIca([ICA_TEST_DATA_SOURCE], ALL_2025_2026)
ct = LogData([CLOUDTRAIL_TEST_DATA_SOURCE], ALL_2025_2026)

df = ct.as_frame()

print(df)
