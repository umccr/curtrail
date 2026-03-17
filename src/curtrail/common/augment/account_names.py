from pathlib import Path

import polars as pl

from curtrail.common.aws_config_profiles import account_id_to_profile


def account_id_convert(account_id: str) -> str:
    d = account_id_to_profile(Path.home() / ".aws" / "config")

    if account_id in d:
        return d[account_id]
    else:
        return account_id


def account_id_name(account_id_array):
    """
    Batch process arrays account ids and tries to return an account name

    :param account_id_array: array of account ids
    :return: series of names for the corresponding account ids
    """
    return pl.Series([account_id_convert(value) for value in account_id_array])
