from pathlib import Path

import polars as pl

from curtrail.common.aws_config_profiles import account_id_to_profile


def augment_with_account_name(
    df: pl.DataFrame, new_column_name="accountName"
) -> pl.DataFrame:
    """
    Takes the account column (with account ids) and opportunistically converts them
    to friendly names based on naming we can find in local AWS config files.
    """
    return df.with_columns(
        pl.col("account")
        .map_batches(
            lambda combined: _account_id_name_batch(combined),
            return_dtype=pl.String,
        )
        .alias(new_column_name)
    )


def _account_id_name_batch(account_id_array):
    """
    Batch process arrays account ids and tries to return an account name

    :param account_id_array: array of account ids
    :return: series of names for the corresponding account ids
    """
    return pl.Series([_account_id_convert(value) for value in account_id_array])


def _account_id_convert(account_id: str) -> str:
    d = account_id_to_profile(Path.home() / ".aws" / "config")

    if account_id in d:
        return d[account_id]
    else:
        return account_id
