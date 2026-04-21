from datetime import date
from pathlib import Path

import pytest

from curtrail.bill.bill_data_ica import BillDataIca
from curtrail.source_bill_cur_test_data import SourceBillCURTestData
from curtrail.bill.bill_data_aws import BillDataAws
from curtrail.log.log_data import LogData
from curtrail.source_bill_ica import SourceBillIca
from curtrail.source_filter import SourceFilter
from curtrail.source_log_cloudtrail_test_data import SourceLogCloudTrailTestData

TEST_DATA = Path(__file__).parent.parent / "test-data"

ICA_TEST_DATA_SOURCE = SourceBillIca(data_prefix=str(TEST_DATA / "ica"))
CUR_TEST_DATA_SOURCE = SourceBillCURTestData(data_prefix=str(TEST_DATA / "cur"))
CLOUDTRAIL_TEST_DATA_SOURCE = SourceLogCloudTrailTestData(
    data_prefix=str(TEST_DATA / "cloudtrail")
)

MARCH_2026 = SourceFilter(days_inclusive=(date(2026, 3, 1), date(2026, 3, 31)))


@pytest.fixture(scope="session")
def bill_data_aws() -> BillDataAws:
    """ """
    return BillDataAws([CUR_TEST_DATA_SOURCE], MARCH_2026)


@pytest.fixture(scope="session")
def bill_data_ica() -> BillDataIca:
    """ """
    return BillDataIca([ICA_TEST_DATA_SOURCE], MARCH_2026)


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
    return LogData(
        [CLOUDTRAIL_TEST_DATA_SOURCE],
        SourceFilter(days_inclusive=(date(2026, 3, 11), date(2026, 3, 11))),
    )
