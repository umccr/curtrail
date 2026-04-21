import polars as pl

#
# Polars schema definitions for ICA (Illumina Connected Analytics) detailed usage CSV exports
#

usage_id_name = "USAGE_ID"
usage_id_dtype = pl.Int64()

uc_name_name = "UC_NAME"
uc_name_dtype = pl.String()

billable_account_id_name = "BILLABLE_ACCOUNT_ID"
billable_account_id_dtype = pl.String()

account_name_name = "ACCOUNT_NAME"
account_name_dtype = pl.String()

account_type_name = "ACCOUNT_TYPE"
account_type_dtype = pl.String()

usage_context_name = "USAGE_CONTEXT"
usage_context_dtype = pl.String()

usage_context_type_name = "USAGE_CONTEXT_TYPE"
usage_context_type_dtype = pl.String()

user_name_name = "USER_NAME"
user_name_dtype = pl.String()

product_name = "PRODUCT"
product_dtype = pl.String()

usage_type_description_name = "USAGE_TYPE_DESCRIPTION"
usage_type_description_dtype = pl.String()

quantity_name = "QUANTITY"
quantity_dtype = pl.Float64()

usage_unit_name = "USAGE_UNIT"
usage_unit_dtype = pl.String()

price_per_unit_name = "PRICE_PER_UNIT"
price_per_unit_dtype = pl.Float64()

cost_name = "COST"
cost_dtype = pl.Float64()

cost_unit_name = "COST_UNIT"
cost_unit_dtype = pl.String()

category_name = "CATEGORY"
category_dtype = pl.String()

usage_timestamp_name = "USAGE_TIMESTAMP"
usage_timestamp_dtype = pl.Datetime("ms")
USAGE_TIMESTAMP_FORMAT = "%m/%d/%Y %H:%M:%S"

region_name = "REGION"
region_dtype = pl.String()

metadata_name = "METADATA"
# for type see below - this field is turned into a struct

billing_date_name = "BILLING_DATE"
billing_date_dtype = pl.Date()
BILLING_DATE_FORMAT = "%m/%d/%Y %H:%M:%S"

#
# Metadata subfields — parsed from the pipe-delimited METADATA column into JSON,
# then decoded into these columns.
#

metadata_id_name = "id"
metadata_id_dtype = pl.String()

metadata_status_name = "status"
metadata_status_dtype = pl.String()

metadata_reference_name = "reference"
metadata_reference_dtype = pl.String()

metadata_payment_scheme_name = "paymentScheme"
metadata_payment_scheme_dtype = pl.String()

metadata_start_time_name = "startTime"
metadata_start_time_dtype = pl.Datetime("ms", "UTC")

metadata_completion_time_name = "completionTime"
metadata_completion_time_dtype = pl.Datetime("ms", "UTC")

metadata_dragen_version_name = "dragenVersion"
metadata_dragen_version_dtype = pl.String()

metadata_pipeline_uuid_name = "pipelineUuid"
metadata_pipeline_uuid_dtype = pl.String()

metadata_license_name = "license"
metadata_license_dtype = pl.String()

metadata_price_discount_percent_name = "priceDiscountPercent"
metadata_price_discount_percent_dtype = pl.String()

# Struct schema passed to json_decode — keys must match the JSON produced by _metadata_to_json.
# All-string schema used with json_decode — JSON has no native datetime type.
metadata_struct_schema = pl.Struct({
    metadata_completion_time_name: pl.String(),
    metadata_dragen_version_name: pl.String(),
    metadata_id_name: pl.String(),
    metadata_license_name: pl.String(),
    metadata_payment_scheme_name: pl.String(),
    metadata_pipeline_uuid_name: pl.String(),
    metadata_price_discount_percent_name: pl.String(),
    metadata_reference_name: pl.String(),
    metadata_start_time_name: pl.String(),
    metadata_status_name: pl.String(),
})

# Final struct schema after datetime fields have been parsed via struct.with_fields().
metadata_final_struct_schema = pl.Struct({
    metadata_completion_time_name: metadata_completion_time_dtype,
    metadata_dragen_version_name: metadata_dragen_version_dtype,
    metadata_id_name: metadata_id_dtype,
    metadata_license_name: metadata_license_dtype,
    metadata_payment_scheme_name: metadata_payment_scheme_dtype,
    metadata_pipeline_uuid_name: metadata_pipeline_uuid_dtype,
    metadata_price_discount_percent_name: metadata_price_discount_percent_dtype,
    metadata_reference_name: metadata_reference_dtype,
    metadata_start_time_name: metadata_start_time_dtype,
    metadata_status_name: metadata_status_dtype,
})

# Final schema after all transformations (datetimes parsed, METADATA decoded to struct).
# This is the shape returned by SourceBillIca.fetch_data().
all_fields: dict[str, pl.PolarsDataType] = {
    usage_id_name: usage_id_dtype,
    uc_name_name: uc_name_dtype,
    billable_account_id_name: billable_account_id_dtype,
    account_name_name: account_name_dtype,
    account_type_name: account_type_dtype,
    usage_context_name: usage_context_dtype,
    usage_context_type_name: usage_context_type_dtype,
    user_name_name: user_name_dtype,
    product_name: product_dtype,
    usage_type_description_name: usage_type_description_dtype,
    quantity_name: quantity_dtype,
    usage_unit_name: usage_unit_dtype,
    price_per_unit_name: price_per_unit_dtype,
    cost_name: cost_dtype,
    cost_unit_name: cost_unit_dtype,
    category_name: category_dtype,
    usage_timestamp_name: usage_timestamp_dtype,
    region_name: region_dtype,
    metadata_name: metadata_final_struct_schema,
    billing_date_name: billing_date_dtype,
}

# Schema used when scanning the CSV — datetime columns are read as strings
# and cast to Datetime after loading (the format MM/DD/YYYY HH:MM:SS is not
# auto-detected by Polars).
csv_schema: dict[str, pl.PolarsDataType] = {
    usage_id_name: pl.Int64(),
    uc_name_name: pl.String(),
    billable_account_id_name: pl.String(),
    account_name_name: pl.String(),
    account_type_name: pl.String(),
    usage_context_name: pl.String(),
    usage_context_type_name: pl.String(),
    user_name_name: pl.String(),
    product_name: pl.String(),
    usage_type_description_name: pl.String(),
    quantity_name: pl.Float64(),
    usage_unit_name: pl.String(),
    price_per_unit_name: pl.Float64(),
    cost_name: pl.Float64(),
    cost_unit_name: pl.String(),
    category_name: pl.String(),
    usage_timestamp_name: pl.String(),
    region_name: pl.String(),
    metadata_name: pl.String(),
    billing_date_name: pl.String(),
}
