import typer
import polars as pl

from app_context import AppContext
from bill.bill_data import BillData
from bill.bill_data_data_transfer import BillDataDataTransfer
from bill.bill_data_storage import BillDataStorage
from common.augment.currency import cost_in_aud

app = typer.Typer()


def _get_dataframe(ctx: typer.Context) -> pl.DataFrame:
    app_context: AppContext = ctx.obj

    if not app_context:
        raise ValueError("AppContext not provided in context")

    if not app_context.bill:
        raise ValueError(
            "Bill sources must be provided on the command line if bill commands are used"
        )

    df = app_context.bill.get_cur_data(app_context.analysis)

    return df


@app.command()
def storage(ctx: typer.Context):
    storage_bills = BillDataStorage(BillData(_get_dataframe(ctx)))

    print(storage_bills._storage_bill.glimpse())


@app.command()
def datatransfer(ctx: typer.Context):
    data_transfer_bills = BillDataDataTransfer(BillData(_get_dataframe(ctx)))

    print(data_transfer_bills.transfer_region_matrix(0.01))


@app.command()
def services(ctx: typer.Context):
    bills = BillData(_get_dataframe(ctx))

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
                    combined.struct.field("bill_date"),
                    combined.struct.field("total_cost"),
                ),
                return_dtype=pl.Float64,
            )
            .alias("total_cost_in_aud")
        )
        .drop("bill_date")
    )
