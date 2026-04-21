import polars as pl
import pytest

from curtrail.log.log_data import LogData
from curtrail.log.log_data_api_calls import LogDataApiCall
from curtrail.log.log_data_console import LogDataConsole
from curtrail.log.log_data_service_event import LogDataServiceEvent
from curtrail.log.log_data_vpce_event import LogDataVpceEvents

# ── AwsApiCall ────────────────────────────────────────────────────────────────


def test_api_call_filter(log_data: LogData):
    api = LogDataApiCall(log_data)
    frame = api.as_frame()
    assert (frame["eventType"] == "AwsApiCall").all()


def test_api_call_count(log_data: LogData):
    # 24 AwsApiCall events in the test data
    assert LogDataApiCall(log_data).as_frame().height == 24


def test_api_call_errors_count(log_data: LogData):
    # AccessDenied (GetObject on restricted path) + NoSuchKey (DeleteObject on stale key)
    errors = LogDataApiCall(log_data).errors()
    assert errors.as_frame().height == 2


def test_api_call_error_codes(log_data: LogData):
    error_codes = LogDataApiCall(log_data).errors().as_frame()["errorCode"].to_list()
    assert "AccessDenied" in error_codes
    assert "NoSuchKey" in error_codes


# ── AwsConsoleAction / AwsConsoleSignIn ───────────────────────────────────────


def test_console_filter(log_data: LogData):
    console = LogDataConsole(log_data)
    frame = console.frame()
    assert frame["eventType"].is_in(["AwsConsoleAction", "AwsConsoleSignIn"]).all()


def test_console_count(log_data: LogData):
    # 3 ConsoleSignIn + 2 ConsoleAction
    assert LogDataConsole(log_data).frame().height == 5


def test_console_errors_count(log_data: LogData):
    # 1 failed login (bob, no MFA, unusual IP)
    errors = LogDataConsole(log_data).errors()
    assert errors.height == 1


def test_console_error_is_failed_login(log_data: LogData):
    error = LogDataConsole(log_data).errors()[0]
    assert error["eventName"][0] == "ConsoleLogin"
    assert error["errorCode"][0] == "Failed authentication"


# ── AwsServiceEvent ───────────────────────────────────────────────────────────


def test_service_event_filter(log_data: LogData):
    svc = LogDataServiceEvent(log_data)
    frame = svc.frame()
    assert (frame["eventType"] == "AwsServiceEvent").all()


def test_service_event_count(log_data: LogData):
    # EC2 RunInstances x2 (via Batch) + KMS GenerateDataKey + Config snapshot
    assert LogDataServiceEvent(log_data).frame().height == 4


# ── AwsVpceEvents ─────────────────────────────────────────────────────────────


def test_vpce_event_empty(log_data: LogData):
    # No AwsVpceEvents type in test data — confirms the filter doesn't match wrong types
    assert LogDataVpceEvents(log_data)._vpce_events_logs.height == 0


# ── Read-only flag ────────────────────────────────────────────────────────────


def test_api_calls_include_both_readonly_and_mutating(log_data: LogData):
    frame = LogDataApiCall(log_data).as_frame()
    assert frame["readOnly"].any()  # some reads
    assert (~frame["readOnly"]).any()  # some writes


# ── Day filtering ─────────────────────────────────────────────────────────────


def test_single_day_filter_row_count(log_data_one_day: LogData):
    # March 11 has exactly 3 events across both accounts in the test data
    assert log_data_one_day.as_frame().height == 3


def test_single_day_filter_only_correct_date(log_data_one_day: LogData):
    from datetime import date

    dates = (
        log_data_one_day.as_frame()
        .select(pl.col("eventTime").dt.date())
        .to_series()
        .unique()
        .to_list()
    )
    assert dates == [date(2026, 3, 11)]
