import typer
import polars as pl
from app_context import AppContext
from common.schema.aws_cloudtrail_schema import (
    cloudtrail_user_identity_name,
    cloudtrail_high_value_fields,
)
from log.log_data import LogData

app = typer.Typer()


def _get_dataframe(ctx: typer.Context) -> pl.DataFrame:
    app_context: AppContext = ctx.obj

    if not app_context:
        raise ValueError("AppContext not provided in context")

    if not app_context.log:
        raise ValueError(
            "Log sources must be provided on the command line if log commands are used"
        )

    df = app_context.log.get_cloudtrail_data(app_context.analysis)

    return df


@app.command()
def complete(ctx: typer.Context):
    df = _get_dataframe(ctx)

    l = LogData(df)

    print(l.complete_logs().glimpse())


@app.command()
def p(ctx: typer.Context):
    df = _get_dataframe(ctx)

    logs = LogData(df)

    df = (
        logs.api_call_logs()
        .frame()
        .filter(
            pl.col(cloudtrail_user_identity_name).struct.field("type").ne("AWSService")
        )
        .filter(
            pl.col(cloudtrail_user_identity_name)
            .struct.field("arn")
            .str.contains("assumed-role/AWSServiceRole")
            .not_()
        )
        .select(cloudtrail_high_value_fields)
    )

    print(df)
