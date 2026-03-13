from typing import Dict, List

import polars as pl

from common.schema.aws_cur_schema import (
    bill_bill_type_name,
    bill_billing_entity_name,
    line_item_legal_entity_name,
    product_servicecode_name,
)


class BillingData:
    _complete_bill: pl.DataFrame

    def __init__(self, complete_bill: pl.DataFrame) -> None:
        self._complete_bill = complete_bill

    """ The entire bill of all line items from all data files found """

    def complete_bill(self) -> pl.DataFrame:
        return self._complete_bill

    """ The marketplace bills keyed by vendor, includes usage, refunds and purchases """

    def aws_marketplace_by_vendor_bill(self) -> Dict[str, pl.DataFrame]:
        # find all legal entities in our bill that are not AWS (i.e. are AWS Marketplace)
        legals_df = (
            self._complete_bill.filter(pl.col(bill_billing_entity_name) != "AWS")
            .select(line_item_legal_entity_name)
            .unique()
        )

        # we have unconfirmed logic (as in it isn't written down anywhere, but is likely true)
        # that says that Amazon Web Services never appears in the AWS Marketplace -
        # this check just confirms that is true
        legals_df_check_count = self._complete_bill.filter(
            pl.col(bill_billing_entity_name) == "AWS"
        ).filter(
            ~(
                pl.col(line_item_legal_entity_name).str.starts_with(
                    "Amazon Web Services"
                )
            )
        )

        if not legals_df_check_count.is_empty():
            raise Exception(
                "Found a billing line item that was AWS Marketplace but also legally billed by AWS"
            )

        # create a frame of line items for each vendor and return
        # a dictionary of these keyed
        result = {}

        for entities in legals_df.iter_rows():
            entity_name = entities[0]
            result[entity_name] = self._complete_bill.filter(
                pl.col(line_item_legal_entity_name) == entity_name
            )

        return result

    """ The main bill for AWS specific items """

    def aws_bill(self) -> pl.DataFrame:
        return self.complete_bill().filter(pl.col(bill_billing_entity_name) == "AWS")

    """ The main bill for AWS specific items where they are documenting resource usage """

    def aws_usage_bill(self) -> pl.DataFrame:
        return self.aws_bill().filter(pl.col(bill_bill_type_name) == "Anniversary")

    """ The main bill for AWS specific items where they are documenting purchases """

    def aws_purchases_bill(self) -> pl.DataFrame:
        return self.aws_bill().filter(pl.col(bill_bill_type_name) == "Purchase")

    """ The main bill for AWS specific items where they are documenting refunds """

    def aws_refunds_bill(self) -> pl.DataFrame:
        return self.aws_bill().filter(pl.col(bill_bill_type_name) == "Refund")

    """ The list of unique service codes contained in this bill """

    def service_codes(self) -> List[str]:
        return (
            self.complete_bill().get_column(product_servicecode_name).unique().to_list()
        )
