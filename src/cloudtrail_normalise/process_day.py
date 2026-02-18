import gzip
from glob import glob
from typing import Any, List, Generator
from datetime import datetime, date, timedelta
from urllib.parse import urlparse

import boto3
import orjson
import polars as pl

from common.aws_cloudtrail_schema import cloudtrail_schema


"""
Generate a sequence of converted cloudtrail entries that have been
transformed suitable for our parquet schema
"""
def convert_cloudtrail_entries(entries: List[Any]) -> Generator:
    for entry in entries:
        # skip high volume but low importance KMS events

        new_entry = {}

        for k, v in entry.items():
            if k == 'managementEvent':
                new_entry['managementEvent'] = v == 'true'
            elif k == 'readOnly':
                new_entry['readOnly'] = v == 'true'
            elif k == "eventTime":
                new_entry['eventTime'] = datetime.fromisoformat(v)
            elif k == "requestParameters":
                new_entry['requestParameters'] = orjson.dumps(v).decode()
            elif k == "responseElements":
                new_entry['responseElements'] = orjson.dumps(v).decode()
            elif k == "additionalEventData":
                new_entry['additionalEventData'] = orjson.dumps(v).decode()
            elif k == "serviceEventDetails":
                new_entry['serviceEventDetails'] = orjson.dumps(v).decode()
            elif k == "edgeDeviceDetails":
                new_entry['edgeDeviceDetails'] = orjson.dumps(v).decode()
            else:
                new_entry[k] = v

        yield new_entry

"""
Read a single JSON cloudtrail log file and return a polars dataframe with a fixed schema
and performing any transforms
"""
def read_cloudtrail_json(log_json_path: str):
    if log_json_path.startswith("s3://"):
        parsed = urlparse(log_json_path)

        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip('/'))

        # The 'Body' is a StreamingBody, which can be treated as a file-like object
        s3_stream = response['Body']

        # Wrap the S3 stream with gzip.GzipFile for automatic decompression
        if log_json_path.endswith(".gz"):
            with gzip.GzipFile(fileobj=s3_stream, mode='rb') as gz_file:
                b = gz_file.read()
        else:
            b = s3_stream.read()
    else:
        # work with either gzipped or not, though normally they will be gzipped
        if log_json_path.endswith(".gz"):
            with gzip.open(log_json_path, "r") as f:
                b = f.read()
        else:
            with open(log_json_path, "r") as f:
                b = f.read()

    logs = orjson.loads(b)["Records"]

    df = pl.DataFrame(list(convert_cloudtrail_entries(logs)), schema=cloudtrail_schema)

    return df

def s3_glob(s3_path: str) -> List[str]:
    parsed = urlparse(s3_path)
    s3_client = boto3.client('s3')

    response = s3_client.list_objects_v2(Bucket=parsed.netloc, Prefix=parsed.path.lstrip('/'))

    matching_keys = []
    if 'Contents' in response:
        for obj in response['Contents']:
            matching_keys.append(f"s3://{parsed.netloc}/{obj['Key']}")

    return matching_keys


def process_day(aws_logs_base_location, org: str, account: str, region: str, day: date) -> pl.DataFrame:
    df = pl.DataFrame(schema=cloudtrail_schema)

    json_logs_prefix = aws_logs_base_location + f"/{org}/{account}/CloudTrail/{region}/{day.year:04}/{day.month:02}/{day.day:02}/"

    print(f"Processing logs from prefix {json_logs_prefix}")

    if aws_logs_base_location.startswith("s3://"):
        for s3_json in s3_glob(json_logs_prefix):
            df = df.vstack(read_cloudtrail_json(s3_json))
    else:
        for s3_json in glob(json_logs_prefix + "*"):
            df = df.vstack(read_cloudtrail_json(s3_json))

    # we now also want to make sure that events that occurred on our designated day BUT WHICH ARE RECORDED THE DAY AFTER
    # are also found
    day_after = day + timedelta(days=1)

    # we go for up to an hour into the next day
    day_after_json_wildcard = aws_logs_base_location + f"/{org}/{account}/CloudTrail/{region}/{day_after.year:04}/{day_after.month:02}/{day_after.day:02}/{account}_CloudTrail_{region}_{day_after.strftime('%Y%m%d')}T00"

    if aws_logs_base_location.startswith("s3://"):
        for day_after_s3_json in s3_glob(day_after_json_wildcard):
            df = df.vstack(read_cloudtrail_json(day_after_s3_json))
    else:
        for day_after_local_json in glob(day_after_json_wildcard + "*"):
            df = df.vstack(read_cloudtrail_json(day_after_local_json))

    # filter so we *only* get this day's events (we may have got some from the "day after" that are indeed actually
    # from the day after)
    df = df.filter(pl.col("eventTime").dt.date().eq(day))

    if aws_logs_base_location.startswith("s3://"):
        df.write_parquet(aws_logs_base_location + f"/{org}/{account}/CloudTrail-Analytics/{region}/{day.year:04}/{day.month:02}/{day.day:02}/cloudtrail.parquet", compression="zstd", compression_level=22)

    return df
