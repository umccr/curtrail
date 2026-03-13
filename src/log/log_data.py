from typing import List

import polars as pl

from log.log_data_api_calls import LogDataApiCall
from log.log_data_console import LogDataConsole
from log.log_data_service_event import LogDataServiceEvent


class LogData:
    _complete_logs: pl.DataFrame

    def __init__(self, complete_logs: pl.DataFrame) -> None:
        self._complete_logs = complete_logs

    """ The entire set of log entries from all data files found """

    def complete_logs(self) -> pl.DataFrame:
        return self._complete_logs

    """ The set of log entries that are designated API calls """

    def api_call_logs(self) -> LogDataApiCall:
        return LogDataApiCall(self.complete_logs())

    def service_event_logs(self) -> LogDataServiceEvent:
        return LogDataServiceEvent(self.complete_logs())

    def console_logs(self) -> LogDataConsole:
        return LogDataConsole(self.complete_logs())

    """ The list of unique account ids contained """

    def accounts(self) -> List[str]:
        return self.complete_logs().get_column("account").unique().to_list()

    """ The list of unique regions contained """

    def regions(self) -> List[str]:
        return self.complete_logs().get_column("region").unique().to_list()
