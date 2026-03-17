"""
Click helpers for downstream CLI tools.

Single-command tools (most common)
-----------------------------------
Use ``pass_curtrail`` to get a ready-built ``CliContextObj`` injected as the
first argument — no need to list the standard options in your signature:

    from curtrail.cli import pass_curtrail
    from curtrail.cli_context_obj import CliContextObj

    @click.command()
    @pass_curtrail
    @click.option("--threshold", type=float, default=0.01)
    def app(curtrail: CliContextObj, threshold: float) -> None:
        ...

    app()

Multi-command tools
--------------------
Use ``make_basic_curtrail_app()`` for a Click group that shares options across
subcommands and stores a ``CliContextObj`` in ``ctx.obj``:

    from curtrail.cli import make_basic_curtrail_app
    from curtrail.cli_context_obj import CliContextObj

    app = make_basic_curtrail_app()
    pass_obj = click.make_pass_decorator(CliContextObj, ensure=True)

    @app.command()
    @pass_obj
    @click.option("--threshold", type=float, default=0.01)
    def my_command(obj: CliContextObj, threshold: float) -> None:
        ...

    app()
"""

import functools
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import click

from curtrail.base_config import BaseConfig, load_base_config
from curtrail.cli_context_obj import CliContextObj
from curtrail.common.aws_config_profiles import expand_accounts
from curtrail.common.aws_region_shortnames import expand_regions
from curtrail.common.date_ranging import parse_date_range
from curtrail.source_filter import SourceFilter


def pass_curtrail(func):
    """Decorator for single-command tools: adds standard curtrail options and injects
    a ready-built ``CliContextObj`` as the first argument (named ``curtrail``).

    The standard options (--config, --days, --account, --region, --timezone) are
    added to the command automatically and are not visible in the function signature.

        @click.command()
        @pass_curtrail
        @click.option("--my-flag", ...)
        def app(curtrail: CliContextObj, my_flag: str) -> None:
            # curtrail.base_config and curtrail.source_filter are ready to use
            ...

        app()
    """
    @functools.wraps(func)
    def wrapper(config: Path, days: str, accounts: tuple, regions: tuple, tz_name: str, **kwargs):
        obj = _build_cli_context(config, days, accounts, regions, tz_name)
        return func(obj, **kwargs)

    wrapper = click.option("--timezone", "tz_name", default="UTC", show_default=True, help="Timezone for interpreting --days, e.g. Australia/Sydney.")(wrapper)
    wrapper = click.option("--region",   "regions",  multiple=True, default=["*"], show_default=True, help="Region(s) to include. Accepts shorthand e.g. APSE2. Repeat to add more.")(wrapper)
    wrapper = click.option("--account",  "accounts", multiple=True, default=["*"], show_default=True, help="Account ID(s) to include. Accepts account names as found in local AWS config. Repeat to add more.")(wrapper)
    wrapper = click.option("--days", required=True, type=click.STRING, help="Date range e.g. 'last 7 days', 'this month', '2026-01-01'.")(wrapper)
    wrapper = click.option("--config", default="curtrail.toml", show_default=True, type=click.Path(path_type=Path), help="Path to the curtrail TOML config file.")(wrapper)
    return wrapper


def _build_cli_context(
    config: Path,
    days: str,
    accounts: tuple,
    regions: tuple,
    tz_name: str,
    base_config: Optional[BaseConfig] = None,
) -> CliContextObj:
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        raise click.BadParameter(f"Unknown timezone '{tz_name}'", param_hint="--timezone")

    obj = CliContextObj()
    obj.base_config = base_config if base_config is not None else load_base_config(config)

    source_filter = SourceFilter(days_inclusive=parse_date_range(days, tz=tz), timezone=tz)
    source_filter.accounts = "*" if list(accounts) == ["*"] else expand_accounts(list(accounts))
    source_filter.regions  = "*" if list(regions)  == ["*"] else expand_regions(list(regions))

    obj.source_filter = source_filter
    return obj


def make_basic_curtrail_app(
    config_default: str = "curtrail.toml",
    base_config: Optional[BaseConfig] = None,
) -> click.Group:
    """Return a Click group pre-wired with standard curtrail options.

    The group builds a CliContextObj and stores it in ctx.obj before any
    subcommand runs.  Subcommands retrieve it via make_pass_decorator or
    @click.pass_context.

    Args:
        config_default: Default path for the TOML config file.  Ignored when
            base_config is provided.
        base_config: Supply a pre-built BaseConfig to bypass the TOML file
            entirely (useful for testing).
    """

    @click.group()
    @click.option("--config", default=config_default, show_default=True, type=click.Path(path_type=Path), help="Path to the curtrail TOML config file.")
    @click.option("--days", required=True, type=click.STRING, help="Date range e.g. 'last 7 days', 'this month', '2026-01-01'.")
    @click.option("--account", "accounts", multiple=True, default=["*"], show_default=True, help="Account ID(s) to include. Accepts account names as found in local AWS config. Repeat to add more.")
    @click.option("--region",  "regions",  multiple=True, default=["*"], show_default=True, help="Region(s) to include. Accepts shorthand e.g. APSE2. Repeat to add more.")
    @click.option("--timezone", "tz_name", default="UTC", show_default=True, help="Timezone for interpreting --days, e.g. Australia/Sydney.")
    @click.pass_context
    def group(ctx: click.Context, config: Path, days: str, accounts: tuple, regions: tuple, tz_name: str) -> None:
        obj = ctx.ensure_object(CliContextObj)
        built = _build_cli_context(config, days, accounts, regions, tz_name, base_config)
        obj.base_config    = built.base_config
        obj.source_filter  = built.source_filter

    return group