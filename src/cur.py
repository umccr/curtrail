from datetime import timedelta
from typing import Annotated, List, Literal, Optional

import typer
from common.billing.billing_data_data_transfer import BillingDataDataTransfer
from common.billing.billing_data_storage import BillingDataStorage
from common.currency import cost_in_aud
from common.date_ranging import parse_date_range
from common.fetch_cur_data import get_cur_data
from context import AnalysisContext
from umccr import s3_cur_prefix
import polars as pl

app = typer.Typer()


@app.callback()
def main(
        ctx: typer.Context,
        days_description: str = typer.Option("last 3 months", "--days", "-d"),
        regions: List[str] = typer.Option(["*"], "--region", "-r"),
        accounts: List[str] = typer.Option(["*"], "--account", "-a"),
        verbose: bool = typer.Option(False, "--verbose", "-v"),
        config: Optional[str] = typer.Option(None, "--config", "-c"),
):
    start, end = parse_date_range(days_description)

    print(f"\nInput date range of '{days_description}' results in: {start.date()} to {end.date()} inclusive")

    ctx.ensure_object(AnalysisContext)

    ctx.obj.regions = regions
    ctx.obj.days = start.date(), end.date()
    ctx.obj.accounts = accounts


@app.command()
def storage(ctx: typer.Context):
    storage_bills = BillingDataStorage(get_cur_data(s3_cur_prefix, ctx.obj.days))

    print(storage_bills._storage_bill.glimpse())


@app.command()
def datatransfer(ctx: typer.Context):
    data_transfer_bills = BillingDataDataTransfer(get_cur_data(s3_cur_prefix, ctx.obj.days))

    print(data_transfer_bills.transfer_region_matrix(0.01))


@app.command()
def services(ctx: typer.Context):
    bills = get_cur_data(s3_cur_prefix, ctx.obj.days)

    print(
        bills.aws_usage_bill()
        .with_columns(
            pl.col("bill_billing_period_end_date")
            .dt.to_string()
            .str.slice(0, 10)
            .alias("bill_date"),
        )
        .group_by(
            pl.col("bill_date"),
            pl.col("line_item_product_code").alias("service"),
        )
        .agg(pl.col("line_item_unblended_cost").sum().alias("total_cost"))
        .filter(pl.col("total_cost") > 0)
        .sort("total_cost", descending=True)
        .with_columns(
            pl.struct(["bill_date", "total_cost"])
            .map_batches(
                lambda combined: cost_in_aud(
                    combined.struct.field("bill_date"), combined.struct.field("total_cost")
                ),
                return_dtype=pl.Float64,
            )
            .alias("total_cost_in_aud")
        )
        .drop("bill_date")
    )

    # print(bills.service_codes())


if __name__ == "__main__":
    pl.Config.set_tbl_width_chars(500)
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_rows(-1)

    app()

# x = ['AmazonSNS',
#    'AmazonS3GlacierDeepArchive',
#   'AmazonGuardDuty',
#  'AmazonEC2',
#   'AWSSystemsManager',
#   'AmazonInspectorV2',
#   'AmazonAthena', 'AWSBudgets', 'AmazonApiGateway', 'AmazonLocationService', 'AWSXRay',
#   'awskms', None, 'AWSGlue', 'AWSCloudFormation', 'AWSConfig', 'AmazonDataZone', 'AmazonECS', 'AWSELB', 'AmazonSES',
#   'AmazonOmics', 'AmazonVPC', 'AmazonCognito', 'AWSBackup', 'awswaf', 'AWSEvents', 'ACM', 'AmazonRoute53',
#   'AWSCodePipeline', 'AmazonCloudFront', 'AmazonStates', 'AmazonRDS', 'AWSDeveloperSupport', 'AmazonECR',
#   'AmazonDynamoDB', 'AWSCostExplorer', 'AWSLambda', 'AmazonGlacier', 'AWSSecurityHub', 'AWSSecretsManager',
#   'AWSCloudTrail', 'AmazonCloudWatch', 'CodeBuild', 'AWSQueueService', 'AWSLakeFormation', 'AmazonEFS',
#   'AmazonVerifiedPermissions', 'AmazonDetective', 'AmazonS3', 'AWSAppRunner', 'AWSCloudMap', 'AmazonEKS',
#   'AWSDataTransfer']
