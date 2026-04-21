import polars as pl


class LogDataApiCallErrors:
    _api_call_error_logs: pl.DataFrame

    def __init__(self, parent: "LogDataApiCall") -> None:
        self._api_call_error_logs = parent.as_frame().filter(
            pl.col("errorCode").is_not_null()
        )

    def as_frame(self) -> pl.DataFrame:
        return self._api_call_error_logs
