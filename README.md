# curtrail

A CLI tool and Python library for analyzing billing and logs:
* AWS cost usage reports = *cur*
* AWS CloudTrail entries = *trail*
* ICA bills

## Tests

```
uv run pytest
```

## Example

See `examples/sample-consumer` for an example of how to use the library
by creating your own CLI tool(s) specific to a single use case.

## Adhoc Coding

If you just need to play with some data to see how the library works - you can edit
`tests/adhoc.py`. It can be run with `uv run tests/adhoc.py`. It initialises
various sources pointing to our local test data. Add your own
polars queries and have fun.

## Setup a production tool

Make a local script that pre-configures your data locations.

```sh
uv run src/curtrail.py --cur-data-location "s3://abucket/cur" \
                      --cloudtrail-data-location "s3://anotherbucket/AWSLogsParquet" \
                      --geolite2-city-data-location "s3://anotherbucket/GeoLite2-City.mmdb" \
                      $*
```

Or create a `curtrail.toml` file that specifies your data locations.

```toml
[[log]]
data_prefix  = "s3://anotherbucket/AWSLogsParquet"
organisation = "o-bbbexample"

[[bill_aws]]
data_prefix = "s3://org-b-cur-bucket"

[[bill_ica]]
data_prefix = "s3://org-b-ica-bucket"
```
