# curtrail

A CLI tool and Python library for analyzing billing (AWS cost usage reports = *cur*) and
logs (AWS CloudTrail entries = *trail*).

## Setup

Make a local script that pre-configures your data locations.

```sh
uv run src/curtrail.py --cur-data-location s3://abucket/cur \
                      --cloudtrail-data-location s3://anotherbucket/AWSLogsParquet \
                      --geolite2-city-data-location s3://anotherbucket/GeoLite2-City.mmdb \
                      $*
```
