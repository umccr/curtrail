from typing import List

import polars as pl

from curtrail.log.log_data_api_calls import LogDataApiCall
from curtrail.log.log_data_console import LogDataConsole
from curtrail.log.log_data_service_event import LogDataServiceEvent
from curtrail.source_filter import SourceFilter
from curtrail.source_log import SourceLog
from curtrail.common.schema.aws_cloudtrail_schema import cloudtrail_all_fields


class LogData:
    _complete_logs: pl.DataFrame

    def __init__(self, sources: List[SourceLog], source_filter: SourceFilter) -> None:
        frames = [s.fetch_data(source_filter) for s in sources]
        self._complete_logs = (
            pl.concat(frames) if frames else pl.DataFrame(schema=cloudtrail_all_fields)
        )

    def as_frame(self) -> pl.DataFrame:
        """The entire set of log entries from all data files found as a data frame"""
        return self._complete_logs

    def api_call_logs(self) -> LogDataApiCall:
        """The set of log entries that are designated API calls"""
        return LogDataApiCall(self)

    def service_event_logs(self) -> LogDataServiceEvent:
        return LogDataServiceEvent(self)

    def console_logs(self) -> LogDataConsole:
        return LogDataConsole(self)

    def accounts(self) -> List[str]:
        """The list of unique account ids contained"""
        return self.as_frame().get_column("account").unique().to_list()

    def regions(self) -> List[str]:
        """The list of unique regions contained"""
        return self.as_frame().get_column("region").unique().to_list()
