import polars as pl


class LogDataConsole:
    _console_logs: pl.DataFrame

    def __init__(self, parent: "LogData") -> None:
        self._console_logs = parent.as_frame().filter(
            pl.col("eventType").is_in(["AwsConsoleAction", "AwsConsoleSignIn"])
        )

    def frame(self) -> pl.DataFrame:
        return self._console_logs

    def errors(self) -> pl.DataFrame:
        return self._console_logs.filter(pl.col("errorCode").is_not_null())
