import polars as pl


class LogDataVpceEvents:
    _vpce_events_logs: pl.DataFrame

    def __init__(self, parent: "LogData") -> None:
        self._vpce_events_logs = parent.as_frame().filter(
            pl.col("eventType") == "AwsVpceEvents"
        )
