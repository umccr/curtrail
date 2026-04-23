from typing import List

import polars as pl

from common.schema.aws_cur_schema import (
    bill_fields,
    tags_fields,
    line_item_fields,
    pricing_fields,
    product_fields,
    line_item_line_item_type_name,
    line_item_product_code_name,
)


def cost_by_service(df: pl.DataFrame):
    return (
        df.group_by(
            pl.col("bill_billing_period_start_date").alias("billing_period"),
            pl.col("line_item_product_code").alias("service"),
        )
        .agg(pl.col("line_item_unblended_cost").sum().alias("total_cost"))
        .filter(pl.col("total_cost") > 0)
        .sort("total_cost", descending=True)
    )


def ec2_instance_costs_by_instance_type(df: pl.DataFrame):
    return (
        df.filter(
            (pl.col("line_item_product_code") == "AmazonEC2")
            & (pl.col("product_instance_type") != "")
        )
        .group_by(
            pl.col("bill_billing_period_start_date").alias("billing_period"),
            pl.col("line_item_usage_type").alias("usage_type"),
            pl.col("product_instance_type").alias("instance_type"),
        )
        .agg(pl.col("line_item_unblended_cost").sum().alias("total_cost"))
        .sort("total_cost", descending=True)
    )


def s3_buckets_by_name(df: pl.DataFrame):
    return (
        df.filter(
            (pl.col("line_item_product_code") == "AmazonS3")
            & (pl.col("line_item_resource_id") != "")
        )
        .group_by(
            pl.col("line_item_resource_id").alias("bucket_name"),
            pl.col("line_item_usage_type").alias("usage_type"),
        )
        .agg(pl.col("line_item_unblended_cost").sum().alias("cost"))
        .sort("bucket_name", descending=True)
    )


def daily_costs(df: pl.DataFrame):
    return (
        df.with_columns(
            pl.col("line_item_usage_start_date").cast(pl.Date).alias("usage_date")
        )
        .group_by("usage_date")
        .agg(pl.col("line_item_unblended_cost").sum().alias("daily_cost"))
        .sort("usage_date", descending=True)
    )


def costs_by_region(df: pl.DataFrame):
    return (
        df.group_by(pl.col("product_region_code").alias("region"))
        .agg(pl.col("line_item_unblended_cost").sum().alias("cost"))
        .sort("cost", descending=True)
    )


def costs_by_tag(
    df: pl.DataFrame, tag_name: str, tag_keys: List[str], value_regex_remove
):
    return (
        df.with_columns(
            pl.col("tags")
            .list.eval(
                pl.element().filter(pl.element().struct.field("key").is_in(tag_keys))
            )
            .list.first()
            .struct.field("value")
            .str.replace(value_regex_remove, "")
            .alias(tag_name)
        )
        .group_by(tag_name)
        .agg(pl.col("line_item_unblended_cost").sum().alias("cost"))
        .sort("cost", descending=True)
    )


def get_instance_type() -> pl.Expr:
    """Extract instance type from product_instance_type or description"""
    return (
        pl.when(pl.col("product_instance_type") != "")
        .then(pl.col("product_instance_type"))
        .otherwise(
            pl.col("line_item_line_item_description").str.split(" ").list.first()
        )
    )


def get_instance_family() -> pl.Expr:
    """Extract instance family with metal instance handling"""
    return (
        pl.when(pl.col("product_instance_type").str.split(".").list.get(1) == "metal")
        .then(pl.col("product_instance_type"))
        .when(pl.col("product_instance_type") != "")
        .then(pl.col("product_instance_type").str.split(".").list.first())
        .otherwise(
            pl.col("line_item_line_item_description").str.split(".").list.first()
        )
    )


def get_purchase_option() -> pl.Expr:
    """Extract purchase option, defaulting to Spot if empty"""
    return (
        pl.when(pl.col("pricing_term") == "")
        .then(pl.lit("Spot"))
        .otherwise(pl.col("pricing_term"))
    )


def usage_of_ec2_instances(df: pl.DataFrame):
    return (
        df.filter(
            (pl.col(line_item_product_code_name) == "AmazonEC2")
            & (pl.col("line_item_operation").str.starts_with("RunInstance"))
            & (
                pl.col("line_item_usage_type").str.contains("BoxUsage")
                | pl.col("line_item_usage_type").str.contains("SpotUsage")
            )
        )
        .with_columns(
            [
                pl.col("bill_billing_period_end_date")
                .dt.to_string()
                .str.slice(0, 10)
                .alias("bill_date"),
                pl.col("line_item_usage_account_name").alias("account_name"),
                pl.col("product_region_code").alias("region"),
                pl.col("line_item_resource_id").alias("instance_id"),
                get_instance_type().alias("instance_type"),
                get_instance_family().alias("instance_family"),
                get_purchase_option().alias("purchase_option"),
            ]
        )
        .group_by(
            [
                "bill_date",
                "account_name",
                "region",
                "instance_id",
                "instance_type",
                "instance_family",
                "purchase_option",
            ]
        )
        .agg(
            pl.col("line_item_usage_amount").sum().alias("instance_hours"),
            pl.col("line_item_unblended_cost").sum().alias("cost"),
        )
        .with_columns(
            pl.struct(["bill_date", "cost"])
            .map_batches(
                lambda combined: cost_in_aud(
                    combined.struct.field("bill_date"), combined.struct.field("cost")
                ),
                return_dtype=pl.Float64,
            )
            .alias("cost_in_aud")
        )
        .sort("cost", descending=True)
    )


def dump_products():
    df = (
        pl.scan_parquet(source, schema=product_fields, extra_columns="ignore")
        .select("product")
        .explode("product")
        .unnest("product")
        .unique()
        .sort("key")
        .collect()
    )

    print(df)


def dump_tags():
    df = (
        pl.scan_parquet(source, schema=tags_fields, extra_columns="ignore")
        .select("tags")
        .explode("tags")
        .unnest("tags")
        .unique()
        .sort("key")
        .collect()
    )

    print(df)


def p():
    pl.Config.set_tbl_width_chars(1000)
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_rows(-1)
    # pl.Config.set_fmt_str_lengths(500)

    df = (
        pl.scan_parquet(
            source,
            schema=bill_fields
            | product_fields
            | line_item_fields
            | pricing_fields
            | tags_fields,
            extra_columns="ignore",
        )
        .filter(
            (pl.col(line_item_line_item_type_name) != "Credit")
            & (pl.col(line_item_line_item_type_name) != "Refund")
        )
        .unique()
        .collect()
    )

    # print(df)

    # print(usage_of_ec2_instances(df))
    # print(df.select("product").explode("product").unnest("product").unique())

    # -- 1. Total cost by service for the frame period
    # print(cost_by_service(df))

    # -- 2. EC2 instance costs by instance type
    # print(ec2_instance_costs_by_instance_type(df))

    # print(s3_buckets_by_name(df))

    # print(daily_costs(df))

    print(costs_by_region(df))

    # print(costs_by_tag(df, "created by", ["resourceTags/aws:createdBy"], r"AssumedRole:.{21}:"))
