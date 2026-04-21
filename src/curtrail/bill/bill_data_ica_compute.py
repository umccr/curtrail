import polars as pl

from curtrail.common.schema.ica_schema import category_name


class BillDataIcaCompute:
    _compute_bill: pl.DataFrame

    def __init__(self, parent: "BillDataIca") -> None:
        # store a new data frame consisting only of storage
        self._compute_bill = parent.as_frame().filter(
            pl.col(category_name).is_in(["Compute"])
        )

    def as_frame(self) -> pl.DataFrame:
        return self._compute_bill
