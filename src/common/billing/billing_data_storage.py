import polars as pl

from common.assertion_guards import assert_all_values_are_the_same
from common.aws_cur_schema import (
    product_servicecode_name,
    pricing_unit_name, )
from common.fetch_cur_data import BillingData


class BillingDataStorage:
    _storage_bill: pl.DataFrame

    def __init__(self, complete_bill: BillingData) -> None:
        # store a new data frame consisting only of storage
        self._storage_bill = (
            complete_bill.aws_usage_bill()
            .filter(pl.col(product_servicecode_name).is_in(["AmazonS3GlacierDeepArchive", "AmazonS3"]))
        )

        # assertions that we believe a true and some of our logic will be wrong if they are not true

        # all pricing units for storage should be in gigabytes
        assert_all_values_are_the_same(
            self._storage_bill.get_column(pricing_unit_name), "GB"
        )
