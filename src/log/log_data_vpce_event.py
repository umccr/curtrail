import polars as pl


class LogDataVpceEvents:
    _vpce_events_logs: pl.DataFrame

    def __init__(self, complete_logs: pl.DataFrame) -> None:
        self._vpce_events_logs = complete_logs.filter(pl.col("eventType") == "AwsVpceEvents")
