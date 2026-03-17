from datetime import date
from pathlib import Path

import polars as pl
import pytest

from curtrail.source_bill_cur_test_data import SourceBillCURTestData
from curtrail.bill.bill_data import BillData
from curtrail.log.log_data import LogData
from curtrail.source_filter import SourceFilter
from curtrail.source_log_cloudtrail import SourceLogCloudTrail
from curtrail.source_log_cloudtrail_test_data import SourceLogCloudTrailTestData

TEST_DATA = Path(__file__).parent.parent / "test-data"

CLOUDTRAIL_TEST_DATA_SOURCE = SourceLogCloudTrailTestData(data_prefix=str(TEST_DATA / "cloudtrail"))

MARCH_2026 = SourceFilter(days_inclusive=(date(2026, 3, 1), date(2026, 3, 31)))


@pytest.fixture(scope="session")
def bill_data() -> BillData:
    """
    Loads March 2026 billing test data via AnalysisSourceBill in test_json mode.
    Data lives at test-data/cur/data/BILLING_PERIOD=2026-03/data.jsonl
    """

    source = SourceBillCURTestData(data_prefix=str(TEST_DATA / "cur"))
    return BillData([source], MARCH_2026)


@pytest.fixture(scope="session")
def log_data() -> LogData:
    """
    Loads March 2026 CloudTrail test data directly from JSONL.
    """
    return LogData([CLOUDTRAIL_TEST_DATA_SOURCE], MARCH_2026)


@pytest.fixture(scope="session")
def log_data_one_day() -> LogData:
    """
    Loads data from a single day in March.
    """
    return LogData([CLOUDTRAIL_TEST_DATA_SOURCE], SourceFilter(days_inclusive=(date(2026, 3, 11), date(2026, 3, 11))))
