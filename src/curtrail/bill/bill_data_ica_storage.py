import polars as pl

from curtrail.common.schema.ica_schema import category_name


class BillDataIcaStorage:
    _storage_bill: pl.DataFrame

    def __init__(self, parent: "BillDataIca") -> None:
        # store a new data frame consisting only of storage
        self._storage_bill = parent.as_frame().filter(
            pl.col(category_name).is_in(
                ["Storage"]
            )
        )

    def as_frame(self) -> pl.DataFrame:
        return self._storage_bill
