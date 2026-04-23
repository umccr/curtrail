#!/usr/bin/env -S uv run

import polars as pl
import click

from curtrail import BillDataIca, CliContextObj, pass_curtrail

from ica_helper import tidy_ica_for_umccr, rollup_pipeline_line_items, REFERENCE_PIPELINE_COLUMN_NAME


#
# A CLI command that breaks down ICA compute costs by project.
#

@click.command()
@pass_curtrail
@click.option(
    "--context",
    "context_filter",
    default="",
    help="Filter string to apply to the pipeline context",
)
def app(context: CliContextObj, context_filter: str) -> None:
    # note we are only looking at compute bills here, not storage costs for each project
    df = (
        BillDataIca(context.base_config.bill_ica, context.source_filter)
        .compute_bill()
        .as_frame()
    )

    df = tidy_ica_for_umccr(df)

    # collapse multiple billing rows that share the same run id (e.g. compute + DRAGEN license)
    # so that the complete run costs are shown
    df = rollup_pipeline_line_items(df)

    pl.Config.set_tbl_width_chars(600)
    pl.Config.set_fmt_str_lengths(200)
    pl.Config.set_tbl_rows(-1)
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_fmt_table_cell_list_len(100)

    # apply cmd line filters

    # first filter by context like production, dev etc
    df = df.filter(pl.col("USAGE_CONTEXT").str.contains(context_filter))

    cost_by_pipeline = (
        df.group_by(
            pl.col("USAGE_CONTEXT"),
            pl.col("META_referencePipeline"),
            pl.col("META_referenceVersion"),
        )
        .agg(
            success_percent=(pl.col("META_status") == "Succeeded").sum() / pl.len(),
            mean_cost=pl.col("COST").mean(),
            total_cost=pl.col("COST").sum(),
        )
        .sort(["USAGE_CONTEXT", "total_cost"], descending=[False, True])
    )

    click.echo(cost_by_pipeline)


app()
