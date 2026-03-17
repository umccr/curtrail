from abc import ABC, abstractmethod

import polars as pl

from curtrail.source_filter import SourceFilter


class SourceBill(ABC):
    """
    A source of bill data, abstract base class.
    """

    @abstractmethod
    def fetch_data(self, source_filter: SourceFilter) -> pl.DataFrame:
        pass
