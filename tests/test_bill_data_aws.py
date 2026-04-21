import polars as pl
import pytest

from curtrail.bill.bill_data_aws import BillDataAws


# ── Row counts ────────────────────────────────────────────────────────────────

def test_complete_bill_row_count(bill_data_aws: BillDataAws):
    # 21 rows total: 20 Anniversary + 1 Refund
    assert bill_data_aws.complete_bill().height == 21


def test_aws_usage_bill_excludes_non_anniversary(bill_data_aws: BillDataAws):
    # Tax row (line_item_line_item_type="Tax") and Refund (bill_bill_type="Refund")
    # are both excluded since aws_usage_bill filters to bill_bill_type == "Anniversary"
    # AND bill_billing_entity == "AWS". The 2 Marketplace rows are Anniversary but
    # have bill_billing_entity != "AWS", so also excluded.
    usage = bill_data_aws.aws_usage_bill()
    assert (usage["bill_bill_type"] == "Anniversary").all()
    assert (usage["bill_billing_entity"] == "AWS").all()


def test_aws_bill_excludes_marketplace(bill_data_aws: BillDataAws):
    aws = bill_data_aws.aws_bill()
    assert (aws["bill_billing_entity"] == "AWS").all()
    # The 2 AWS Marketplace rows should not appear
    assert "AWS Marketplace" not in aws["bill_billing_entity"].to_list()


def test_aws_refunds_bill(bill_data_aws: BillDataAws):
    refunds = bill_data_aws.aws_refunds_bill()
    assert refunds.height == 1
    assert refunds["bill_bill_type"][0] == "Refund"


# ── Service codes ─────────────────────────────────────────────────────────────

def test_service_codes_contains_expected(bill_data_aws: BillDataAws):
    codes = bill_data_aws.service_codes()
    for expected in ["AmazonEC2", "AmazonS3", "AmazonRDS", "AmazonCloudFront", "AWSLambda"]:
        assert expected in codes, f"Expected service code {expected!r} not found"


# ── Marketplace ───────────────────────────────────────────────────────────────

def test_marketplace_by_vendor_has_illumina(bill_data_aws: BillDataAws):
    vendors = bill_data_aws.aws_marketplace_by_vendor_bill()
    assert "Illumina, Inc." in vendors


def test_marketplace_illumina_rows(bill_data_aws: BillDataAws):
    vendors = bill_data_aws.aws_marketplace_by_vendor_bill()
    illumina_df = vendors["Illumina, Inc."]
    assert illumina_df.height == 2
    assert (illumina_df["line_item_legal_entity"] == "Illumina, Inc.").all()


# ── Cost sanity checks ────────────────────────────────────────────────────────

def test_usage_bill_costs_are_positive(bill_data_aws: BillDataAws):
    usage = bill_data_aws.aws_usage_bill()
    assert (usage["line_item_unblended_cost"] >= 0).all()


def test_refund_cost_is_negative(bill_data_aws: BillDataAws):
    refunds = bill_data_aws.aws_refunds_bill()
    assert (refunds["line_item_unblended_cost"] < 0).all()
