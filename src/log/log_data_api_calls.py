import polars as pl


class LogDataApiCall:
    _api_call_logs: pl.DataFrame

    def __init__(self, complete_logs: pl.DataFrame) -> None:
        self._api_call_logs = complete_logs.filter(pl.col("eventType") == "AwsApiCall")

    def frame(self) -> pl.DataFrame:
        return self._api_call_logs

    def errors(self) -> pl.DataFrame:
        return self._api_call_logs.filter(pl.col("errorCode").is_not_null())