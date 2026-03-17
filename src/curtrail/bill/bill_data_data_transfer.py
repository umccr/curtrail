from typing import Optional

import polars as pl

from curtrail.bill.bill_data import BillData
from curtrail.common.assertion_guards import assert_all_values_are_the_same
from curtrail.common.schema.aws_cur_schema import (
    line_item_usage_type_name,
    product_servicecode_name,
    product_usagetype_name,
    product_from_location_name,
    product_to_location_name,
    line_item_net_unblended_cost_name,
    line_item_usage_amount_name,
    line_item_line_item_description_name,
    pricing_unit_name,
    product_operation_name,
    product_fee_code_name,
    product_fee_description_name,
)


class BillDataDataTransfer:
    _data_transfer_bill: pl.DataFrame

    def __init__(self, complete_bill: BillData) -> None:
        # store a new data frame consisting only of data transfer records
        self._data_transfer_bill = (
            complete_bill.aws_usage_bill().filter(
                pl.col(product_servicecode_name).eq("AWSDataTransfer")
            )
            # replace the incorrectly name usage types involving us-east-1
            # (this is a legacy from AWs cur that we can easily correct)
            .with_columns(
                pl.col(product_usagetype_name).replace(
                    ["DataTransfer-In-Bytes", "DataTransfer-Out-Bytes"],
                    ["USE1-DataTransfer-In-Bytes", "USE1-DataTransfer-Out-Bytes"],
                )
            )
            # these values are always the same and/or useless in a data transfer setting
            # so we drop them to make that clear
            # .drop([
            #    product_operation_name,
            #    product_fee_code_name
            # ])
        )

        self.data_transfer_bill = self._data_transfer_bill.vstack(
            complete_bill.aws_usage_bill()
            .filter(
                pl.col(product_servicecode_name).str.contains_any(
                    ["AmazonVPC", "AWSELB", "AmazonCloudFront"]
                )
            )
            .filter(
                pl.col(line_item_usage_type_name).str.contains_any(
                    ["DataTransfer", "InterZone"]
                )
            )
        )

        # assertions that we believe a true and some of our logic will be wrong if they are not true

        # all pricing units for data transfers should be in gigabytes
        assert_all_values_are_the_same(
            self._data_transfer_bill.get_column(pricing_unit_name), "GB"
        )

    data_usage_substrings = [
        "DataTransfer-Regional-Bytes",
        "AWS-In-Bytes",
        "AWS-Out-Bytes",
        "DataTransfer-Out-Bytes",
        "DataXfer-In",
        "DataXfer-Out",
    ]

    def transfer_region_matrix(self, min_gb: Optional[float]) -> pl.DataFrame:
        # assert_all_values_are_null(self._data_transfer_bill.get_column(product_fee_code_name))
        # assert_all_values_are_null(self._data_transfer_bill.get_column(product_fee_description_name))

        # print(
        #     self._data_transfer_bill.get_column(product_from_location_name)
        #     .unique()
        #     .to_list()
        # )
        # print(
        #     self._data_transfer_bill.get_column(product_to_location_name)
        #     .unique()
        #     .to_list()
        # )
        #
        # print(
        #     self._data_transfer_bill.get_column(product_usagetype_name)
        #     .unique()
        #     .to_list()
        # )

        df = (
            self._data_transfer_bill.group_by(
                [product_from_location_name, product_to_location_name]
            )
            .agg(
                pl.col(line_item_usage_amount_name).sum().round(5).alias("gb"),
                pl.col(line_item_net_unblended_cost_name).sum().alias("cost"),
                pl.col(product_servicecode_name).unique().alias("servicecode"),
                pl.col(product_operation_name).unique().alias("operation"),
                pl.col(product_fee_code_name).unique().alias("feecode"),
                pl.col(product_fee_description_name).unique().alias("feedesc"),
                pl.col(line_item_line_item_description_name).unique().alias("desc"),
            )
            .sort(["gb"], descending=True)
        )

        if min_gb is not None:
            df = df.filter(pl.col("gb").ge(min_gb))

        return df.pivot(
            index="product_from_location", columns="product_to_location", values="gb"
        ).fill_null("")
