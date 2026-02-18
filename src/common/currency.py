import json
from functools import lru_cache

import polars as pl
import urllib3

# Create a PoolManager instance
http = urllib3.PoolManager()

@lru_cache(maxsize=None)
def rate(d: str, orig_currency: str, target_currency: str) -> float:
    try:
        u = f"https://api.frankfurter.dev/v1/{d}?base={orig_currency}&symbols={target_currency}"
        contents_response = http.request('GET', u)
        contents = json.loads(contents_response.data.decode('utf-8'))
        r = contents["rates"][target_currency]
        return r
    except Exception:
        return 0


def currency_convert(
    when: str, val: float, orig_currency: str, target_currency: str
) -> float:
    return val * rate(when, orig_currency, target_currency)


def cost_in_aud(date_array, cost_array):
    """
    Batch process arrays of date/cost and return a series with the costs adjusted from USD to AUD

    :param date_array: array of date strings in ISO format (YYYY-MM-DD)
    :param cost_array: array of costs in USD
    :return: series of costs in AUD as per the currency conversion on the corresponding date
    """
    return pl.Series(
        [
            currency_convert(value[0], value[1], "USD", "AUD")
            for value in zip(date_array, cost_array)
        ]
    )
