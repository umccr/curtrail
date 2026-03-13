from typing import List, Optional

import typer
import polars as pl

from analysis_context import AnalysisContext
from analysis_source_billing import AnalysisSourceCur
from analysis_source_log import AnalysisSourceCloudTrail
from app_context import AppContext
from common.date_ranging import parse_date_range
from common.aws_region_shortnames import expand_regions
from common.aws_config_profiles import expand_accounts
import commands_bill
import commands_log

app = typer.Typer()
app.add_typer(commands_bill.app, name="bill")
app.add_typer(commands_log.app, name="log")


@app.callback()
def main(
    ctx: typer.Context,
    cur_prefix: Optional[str] = typer.Option(
        None, "--cur-data-location", "-bill", help="Local or S3 path for CUR data"
    ),
    cloudtrail_prefix: Optional[str] = typer.Option(
        None,
        "--cloudtrail-data-location",
        "-log",
        help="Local or S3 path for CloudTrail data",
    ),
    cloudtrail_organisation: Optional[str] = typer.Option(
        None,
        "--cloudtrail-organisation",
        "-org",
        help="If present indicates that this CloudTrail data is structured as an organisation and hence has an organisation id in the directory structure",
    ),
    geolite2_city_path: Optional[str] = typer.Option(
        None,
        "--geolite2-city-data-location",
        "-geo",
        help="If present specifies the location of the MaxMind GeoLite2 City database",
    ),
    days_description: str = typer.Option("this month", "--days", "-d"),
    regions: List[str] = typer.Option(["*"], "--region", "-r"),
    accounts: List[str] = typer.Option(["*"], "--account", "-a"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    ac = AnalysisContext()

    # we always operate of _some_ form of date range - there is no "wildcard" option
    start, end = parse_date_range(days_description)
    if verbose:
        print(
            f"--days of '{days_description}' results in date range {start.date()} to {end.date()} inclusive"
        )
    ac.days_inclusive = start.date(), end.date()

    for r in regions:
        if r == "*" and len(regions) > 1:
            raise ValueError(
                "Cannot specify '*' in regions when more than one region is specified"
            )

    ac.regions = (
        "*" if len(regions) == 1 and regions[0] == "*" else expand_regions(regions)
    )

    for a in accounts:
        if a == "*" and len(accounts) > 1:
            raise ValueError(
                "Cannot specify '*' in accounts when more than one accounts is specified"
            )

    ac.accounts = (
        "*" if len(accounts) == 1 and accounts[0] == "*" else expand_accounts(accounts)
    )

    log_source = None
    if cloudtrail_prefix is not None:
        log_source = AnalysisSourceCloudTrail(
            data_prefix=cloudtrail_prefix, organisation=cloudtrail_organisation
        )

    bill_source = None
    if cur_prefix is not None:
        bill_source = AnalysisSourceCur(data_prefix=cur_prefix)

    ctx.obj = AppContext(
        analysis=ac, log=log_source, bill=bill_source, geolite2_path=geolite2_city_path
    )


if __name__ == "__main__":
    pl.Config.set_tbl_width_chars(500)
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_tbl_rows(-1)

    app()
