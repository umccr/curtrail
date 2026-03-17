import polars as pl

from curtrail.source_filter import SourceFilter

"""
A source of log data.
"""

from abc import ABC, abstractmethod


class SourceLog(ABC):
    @abstractmethod
    def fetch_data(self, source_filter: SourceFilter) -> pl.DataFrame:
        pass
