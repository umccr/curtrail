import click
import polars as pl

from curtrail import pass_curtrail, CliContextObj, LogData


@click.command()
@pass_curtrail
@click.option("--bucket", "bucket_filter", default=None, help="Substring filter on bucket name.")
@click.option("--key",    "key_filter",    default=None, help="Substring filter on object key.")
@click.option("--last-n", "last_n", default=1, show_default=True, type=click.IntRange(min=1), help="Number of most recent accesses to show per bucket/key.")
def app(context: CliContextObj, bucket_filter: str | None, key_filter: str | None, last_n: int) -> None:
    """For each S3 object (bucket + key), show the most recent data-plane access(es) and who performed them."""
    api_calls = LogData(context.base_config.logs, context.source_filter).api_call_logs()

    if api_calls.as_frame().filter(pl.col("eventCategory") == "Data").is_empty():
        click.echo(
            "Warning: no Data events found in the CloudTrail logs for this period. "
            "S3 (and other service) data-plane logging may not be enabled in CloudTrail.",
            err=True,
        )

    df = api_calls.s3_data_plane().as_frame()

    df = df.with_columns(
        pl.col("requestParameters").str.json_path_match("$.bucketName").alias("bucket"),
        pl.col("requestParameters").str.json_path_match("$.key").alias("key"),
    ).filter(pl.col("bucket").is_not_null())

    if bucket_filter:
        df = df.filter(pl.col("bucket").str.contains(bucket_filter))
    if key_filter:
        df = df.filter(pl.col("key").str.contains(key_filter))

    df = (
        df
        .sort("eventTime")
        .group_by("bucket", "key")
        .agg(
            pl.col("eventTime").tail(last_n).alias("lastAccess"),
            pl.col("eventName").tail(last_n),
            pl.col("identityType").tail(last_n),
            pl.col("identityActor").tail(last_n),
            pl.col("identityRole").tail(last_n),
        )
        .explode("lastAccess", "eventName", "identityType", "identityActor", "identityRole")
        .sort(["bucket", "key", "lastAccess"], descending=[False, False, True])
    )

    with pl.Config(fmt_str_lengths=1000, tbl_width_chars=1000):
        click.echo(df)


app()