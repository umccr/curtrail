from functools import lru_cache
from tempfile import NamedTemporaryFile
from time import perf_counter
from typing import Optional

import boto3
import geoip2.database
import polars as pl
from geoip2.errors import AddressNotFoundError
from maxminddb import MODE_MEMORY


def ip_as_iso_country_code(bucket: str, ip_array: pl.Series):
    """
    Batch process arrays of IPs and return a series with ISO country codes

    :param bucket: bucket name holding the geoip2 database (at root)
    :param ip_array: array of IP addresses
    :return: series of ISO country codes of the IP addresses
    """
    return pl.Series([geo_country_iso_code_lookup(bucket, value) for value in ip_array])


def ip_as_city_name(bucket: str, ip_array: pl.Series):
    """
    Batch process arrays of IPs and return a series with city names

    :param bucket: bucket name holding the geoip2 database (at root)
    :param ip_array: array of IP addresses
    :return: series of city names of the IP addresses
    """
    return pl.Series([geo_city_name_lookup(bucket, value) for value in ip_array])


@lru_cache(maxsize=1024)
def geo_city_name_lookup(s3_bucket: str, ip_address: str) -> Optional[str]:
    # creates on first use for this bucket and then caches
    geo_reader = create_geo_reader(s3_bucket)

    try:
        return geo_reader.city(ip_address).city.name
    except AddressNotFoundError:
        return None
    except ValueError:
        return "Amazon"


@lru_cache(maxsize=1024)
def geo_country_iso_code_lookup(s3_bucket: str, ip_address: str) -> Optional[str]:
    # creates on first use for this bucket and then caches
    geo_reader = create_geo_reader(s3_bucket)

    try:
        return geo_reader.city(ip_address).country.iso_code
    except AddressNotFoundError:
        return None
    except ValueError:
        return "AMZ"


@lru_cache(maxsize=None)
def create_geo_reader(bucket_name: str) -> geoip2.database.Reader:

    object_key = "GeoLite2-City.mmdb"

    start_time = perf_counter()

    s3 = boto3.client("s3")

    with NamedTemporaryFile(suffix=".mmdb", delete=True) as tmp:
        s3.download_file(bucket_name, object_key, tmp.name)

        r = geoip2.database.Reader(tmp.name, None, MODE_MEMORY)

        end_time = perf_counter()
        elapsed_time = end_time - start_time

        # print(f"Initialising GeoMind database from S3 took: {elapsed_time} seconds")

        return r
