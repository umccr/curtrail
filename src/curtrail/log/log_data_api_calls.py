import polars as pl

from curtrail.log.log_data_api_calls_errors import LogDataApiCallErrors


class LogDataApiCall:
    _api_call_logs: pl.DataFrame

    def __init__(self, parent: "LogData") -> None:
        self._api_call_logs = parent.as_frame().filter(pl.col("eventType") == "AwsApiCall")

    def as_frame(self) -> pl.DataFrame:
        return self._api_call_logs

    def s3_data_plane(self) -> "LogDataApiCall":
        """Return only S3 data-plane events (GetObject, PutObject, DeleteObject, etc.).

        Excludes S3 management events (ListBuckets, CreateBucket, PutBucketPolicy,
        etc.) by filtering to eventSource=s3 and eventCategory=Data.  The
        eventCategory field is the reliable discriminator — data-plane events are
        always recorded with category "Data" regardless of the specific operation.
        """
        result = LogDataApiCall.__new__(LogDataApiCall)
        result._api_call_logs = self._api_call_logs.filter(
            (pl.col("eventSource") == "s3.amazonaws.com")
            & (pl.col("eventCategory") == "Data")
        )
        return result

    def errors(self) -> LogDataApiCallErrors:
        return LogDataApiCallErrors(self)
