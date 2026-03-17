import polars as pl


class LogDataServiceEvent:
    _service_event_logs: pl.DataFrame

    def __init__(self, parent: "LogData") -> None:
        self._service_event_logs = parent.as_frame().filter(
            pl.col("eventType") == "AwsServiceEvent"
        )

    def frame(self) -> pl.DataFrame:
        return self._service_event_logs
