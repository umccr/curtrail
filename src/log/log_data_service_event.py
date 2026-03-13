import polars as pl


class LogDataServiceEvent:
    _service_event_logs: pl.DataFrame

    def __init__(self, complete_logs: pl.DataFrame) -> None:
        self._service_event_logs = complete_logs.filter(
            pl.col("eventType") == "AwsServiceEvent"
        )

    def frame(self) -> pl.DataFrame:
        return self._service_event_logs
