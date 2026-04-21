from typing import List

import polars as pl

from curtrail.bill.bill_data_ica_compute import BillDataIcaCompute
from curtrail.bill.bill_data_ica_storage import BillDataIcaStorage
from curtrail.common.schema.aws_cur_schema import (
    all_fields,
)
from curtrail.source_bill import SourceBill
from curtrail.source_filter import SourceFilter


class BillDataIca:
    _complete_bill: pl.DataFrame

    def __init__(self, sources: List[SourceBill], source_filter: SourceFilter) -> None:
        # initalise a frame across all the sources
        frames = [s.fetch_data(source_filter) for s in sources]
        self._complete_bill = (
            pl.concat(frames) if frames else pl.DataFrame(schema=all_fields)
        )

    """ The entire ICA bill from all data files found """

    def as_frame(self) -> pl.DataFrame:
        return self._complete_bill

    def compute_bill(self) -> BillDataIcaCompute:
        return BillDataIcaCompute(self)

    def storage_bill(self) -> BillDataIcaStorage:
        return BillDataIcaStorage(self)
