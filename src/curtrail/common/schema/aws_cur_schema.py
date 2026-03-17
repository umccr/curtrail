import polars as pl

#
# a set of Polars schema definitions for AWS CUR 2 data
#

# best info is
# https://docs.aws.amazon.com/cur/latest/userguide/data-dictionary.html

#
# cur
# https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2-bill.html
#

bill_bill_type_name = "bill_bill_type"
bill_bill_type_dtype = pl.String()
bill_billing_entity_name = "bill_billing_entity"
bill_billing_entity_dtype = pl.String()
bill_billing_period_end_date_name = "bill_billing_period_end_date"
bill_billing_period_end_date_dtype = pl.Datetime("ns")
bill_billing_period_start_date_name = "bill_billing_period_start_date"
bill_billing_period_start_date_dtype = pl.Datetime("ns")
bill_invoice_id_name = "bill_invoice_id"
bill_invoice_id_dtype = pl.String()
bill_invoicing_entity_name = "bill_invoicing_entity"
bill_invoicing_entity_dtype = pl.String()
bill_payer_account_id_name = "bill_payer_account_id"
bill_payer_account_id_dtype = pl.String()
bill_payer_account_name_name = "bill_payer_account_name"
bill_payer_account_name_dtype = pl.String()

bill_columns = [
    bill_bill_type_name,
    bill_billing_entity_name,
    bill_billing_period_end_date_name,
    bill_billing_period_start_date_name,
    bill_invoice_id_name,
    bill_invoicing_entity_name,
    bill_payer_account_id_name,
    bill_payer_account_name_name,
]

bill_fields = {
    bill_bill_type_name: bill_bill_type_dtype,
    bill_billing_entity_name: bill_billing_entity_dtype,
    bill_billing_period_end_date_name: bill_billing_period_end_date_dtype,
    bill_billing_period_start_date_name: bill_billing_period_start_date_dtype,
    bill_invoice_id_name: bill_invoice_id_dtype,
    bill_invoicing_entity_name: bill_invoicing_entity_dtype,
    bill_payer_account_id_name: bill_payer_account_id_dtype,
    bill_payer_account_name_name: bill_payer_account_name_dtype,
}

#
# cost category
# https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2-cost-category.html
#

cost_category_name = "cost_category"
cost_category_dtype = pl.List(pl.Struct({"key": pl.String(), "value": pl.String()}))

cost_category_fields = {cost_category_name: cost_category_dtype}

#
# discount
# https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2-discount.html
#

discount_name = "discount"
discount_dtype = pl.List(pl.Struct({"key": pl.String(), "value": pl.String()}))
discount_bundled_discount_name = "discount_bundled_discount"
discount_bundled_discount_dtype = pl.Float64
discount_total_discount_name = "discount_total_discount"
discount_total_discount_dtype = pl.Float64

discount_fields = {
    discount_name: discount_dtype,
    discount_bundled_discount_name: discount_bundled_discount_dtype,
    discount_total_discount_name: discount_total_discount_dtype,
}

#
# identity
# https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2-identity.html
#

identity_line_item_id_name = "identity_line_item_id"
identity_line_item_id_dtype = pl.String
identity_time_interval_name = "identity_time_interval"
identity_time_interval_dtype = pl.String

identity_fields = {
    identity_line_item_id_name: identity_line_item_id_dtype,
    identity_time_interval_name: identity_time_interval_dtype,
}

#
# line item
# https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2-line-item.html
#

line_item_availability_zone_name = "line_item_availability_zone"
line_item_availability_zone_dtype = pl.String()
line_item_blended_cost_name = "line_item_blended_cost"
line_item_blended_cost_dtype = pl.Float64()
line_item_blended_rate_name = "line_item_blended_rate"
line_item_blended_rate_dtype = pl.String()
line_item_currency_code_name = "line_item_currency_code"
line_item_currency_code_dtype = pl.String()
line_item_legal_entity_name = "line_item_legal_entity"
line_item_legal_entity_dtype = pl.String()
line_item_line_item_description_name = "line_item_line_item_description"
line_item_line_item_description_dtype = pl.String()
line_item_line_item_type_name = "line_item_line_item_type"
line_item_line_item_type_dtype = pl.String()
line_item_net_unblended_cost_name = "line_item_net_unblended_cost"
line_item_net_unblended_cost_dtype = pl.Float64()
line_item_net_unblended_rate_name = "line_item_net_unblended_rate"
line_item_net_unblended_rate_dtype = pl.String()
line_item_normalization_factor_name = "line_item_normalization_factor"
line_item_normalization_factor_dtype = pl.Float64()
line_item_normalized_usage_amount_name = "line_item_normalized_usage_amount"
line_item_normalized_usage_amount_dtype = pl.Float64()
line_item_operation_name = "line_item_operation"
line_item_operation_dtype = pl.String()
line_item_product_code_name = "line_item_product_code"
line_item_product_code_dtype = pl.String()
line_item_resource_id_name = "line_item_resource_id"
line_item_resource_id_dtype = pl.String()
line_item_tax_type_name = "line_item_tax_type"
line_item_tax_type_dtype = pl.String()
line_item_unblended_cost_name = "line_item_unblended_cost"
line_item_unblended_cost_dtype = pl.Float64()
line_item_unblended_rate_name = "line_item_unblended_rate"
line_item_unblended_rate_dtype = pl.String()
line_item_usage_account_id_name = "line_item_usage_account_id"
line_item_usage_account_id_dtype = pl.String()
line_item_usage_account_name_name = "line_item_usage_account_name"
line_item_usage_account_name_dtype = pl.String()
line_item_usage_amount_name = "line_item_usage_amount"
line_item_usage_amount_dtype = pl.Float64()
line_item_usage_end_date_name = "line_item_usage_end_date"
line_item_usage_end_date_dtype = pl.Datetime("ns")
line_item_usage_start_date_name = "line_item_usage_start_date"
line_item_usage_start_date_dtype = pl.Datetime("ns")
line_item_usage_type_name = "line_item_usage_type"
line_item_usage_type_dtype = pl.String()
line_item_user_identifier_name = "line_item_user_identifier"
line_item_user_identifier_dtype = pl.String()

line_item_fields = {
    line_item_availability_zone_name: line_item_availability_zone_dtype,
    line_item_blended_cost_name: line_item_blended_cost_dtype,
    line_item_blended_rate_name: line_item_blended_rate_dtype,
    line_item_currency_code_name: line_item_currency_code_dtype,
    line_item_legal_entity_name: line_item_legal_entity_dtype,
    line_item_line_item_description_name: line_item_line_item_description_dtype,
    line_item_line_item_type_name: line_item_line_item_type_dtype,
    line_item_net_unblended_cost_name: line_item_net_unblended_cost_dtype,
    line_item_net_unblended_rate_name: line_item_net_unblended_rate_dtype,
    line_item_normalization_factor_name: line_item_normalization_factor_dtype,
    line_item_normalized_usage_amount_name: line_item_normalized_usage_amount_dtype,
    line_item_operation_name: line_item_operation_dtype,
    line_item_product_code_name: line_item_product_code_dtype,
    line_item_resource_id_name: line_item_resource_id_dtype,
    line_item_tax_type_name: line_item_tax_type_dtype,
    line_item_unblended_cost_name: line_item_unblended_cost_dtype,
    line_item_unblended_rate_name: line_item_unblended_rate_dtype,
    line_item_usage_account_id_name: line_item_usage_account_id_dtype,
    line_item_usage_account_name_name: line_item_usage_account_name_dtype,
    line_item_usage_amount_name: line_item_usage_amount_dtype,
    line_item_usage_end_date_name: line_item_usage_end_date_dtype,
    line_item_usage_start_date_name: line_item_usage_start_date_dtype,
    line_item_usage_type_name: line_item_usage_type_dtype,
    line_item_user_identifier_name: line_item_user_identifier_dtype,
}

#
# pricing
# https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2-pricing.html
#

pricing_currency_name = "pricing_currency"
pricing_currency_dtype = pl.String()
pricing_lease_contract_length_name = "pricing_lease_contract_length"
pricing_lease_contract_length_dtype = pl.String()
pricing_offering_class_name = "pricing_offering_class"
pricing_offering_class_dtype = pl.String()
pricing_public_on_demand_cost_name = "pricing_public_on_demand_cost"
pricing_public_on_demand_cost_dtype = pl.Float64()
pricing_public_on_demand_rate_name = "pricing_public_on_demand_rate"
pricing_public_on_demand_rate_dtype = pl.String()
pricing_purchase_option_name = "pricing_purchase_option"
pricing_purchase_option_dtype = pl.String()
pricing_rate_code_name = "pricing_rate_code"
pricing_rate_code_dtype = pl.String()
pricing_rate_id_name = "pricing_rate_id"
pricing_rate_id_dtype = pl.String()
pricing_term_name = "pricing_term"
pricing_term_dtype = pl.String()
pricing_unit_name = "pricing_unit"
pricing_unit_dtype = pl.String()

pricing_fields = {
    pricing_currency_name: pricing_currency_dtype,
    pricing_lease_contract_length_name: pricing_lease_contract_length_dtype,
    pricing_offering_class_name: pricing_offering_class_dtype,
    pricing_public_on_demand_cost_name: pricing_public_on_demand_cost_dtype,
    pricing_public_on_demand_rate_name: pricing_public_on_demand_rate_dtype,
    pricing_purchase_option_name: pricing_purchase_option_dtype,
    pricing_rate_code_name: pricing_rate_code_dtype,
    pricing_rate_id_name: pricing_rate_id_dtype,
    pricing_term_name: pricing_term_dtype,
    pricing_unit_name: pricing_unit_dtype,
}

#
# product
# https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2-product.html
#

product_name = "product"
product_dtype = pl.List(pl.Struct({"key": pl.String(), "value": pl.String()}))
product_comment_name = "product_comment"
product_comment_dtype = pl.String()
product_fee_code_name = "product_fee_code"
product_fee_code_dtype = pl.String()
product_fee_description_name = "product_fee_description"
product_fee_description_dtype = pl.String()
product_from_location_name = "product_from_location"
product_from_location_dtype = pl.String()
product_from_location_type_name = "product_from_location_type"
product_from_location_type_dtype = pl.String()
product_from_region_code_name = "product_from_region_code"
product_from_region_code_dtype = pl.String()
product_instance_family_name = "product_instance_family"
product_instance_family_dtype = pl.String()
product_instance_type_name = "product_instance_type"
product_instance_type_dtype = pl.String()
product_instancesku_name = "product_instancesku"
product_instancesku_dtype = pl.String()
product_location_name = "product_location"
product_location_dtype = pl.String()
product_location_type_name = "product_location_type"
product_location_type_dtype = pl.String()
product_operation_name = "product_operation"
product_operation_dtype = pl.String()
product_pricing_unit_name = "product_pricing_unit"
product_pricing_unit_dtype = pl.String()
product_product_family_name = "product_product_family"
product_product_family_dtype = pl.String()
product_region_code_name = "product_region_code"
product_region_code_dtype = pl.String()
product_servicecode_name = "product_servicecode"
product_servicecode_dtype = pl.String()
product_sku_name = "product_sku"
product_sku_dtype = pl.String()
product_to_location_name = "product_to_location"
product_to_location_dtype = pl.String()
product_to_location_type_name = "product_to_location_type"
product_to_location_type_dtype = pl.String()
product_to_region_code_name = "product_to_region_code"
product_to_region_code_dtype = pl.String()
product_usagetype_name = "product_usagetype"
product_usagetype_dtype = pl.String()

product_fields = {
    product_name: product_dtype,
    product_comment_name: product_comment_dtype,
    product_fee_code_name: product_fee_code_dtype,
    product_fee_description_name: product_fee_description_dtype,
    product_from_location_name: product_from_location_dtype,
    product_from_location_type_name: product_from_location_type_dtype,
    product_from_region_code_name: product_from_region_code_dtype,
    product_instance_family_name: product_instance_family_dtype,
    product_instance_type_name: product_instance_type_dtype,
    product_instancesku_name: product_instancesku_dtype,
    product_location_name: product_location_dtype,
    product_location_type_name: product_location_type_dtype,
    product_operation_name: product_operation_dtype,
    product_pricing_unit_name: product_pricing_unit_dtype,
    product_product_family_name: product_product_family_dtype,
    product_region_code_name: product_region_code_dtype,
    product_servicecode_name: product_servicecode_dtype,
    product_sku_name: product_sku_dtype,
    product_to_location_name: product_to_location_dtype,
    product_to_location_type_name: product_to_location_type_dtype,
    product_to_region_code_name: product_to_region_code_dtype,
    product_usagetype_name: product_usagetype_dtype,
}

#
# split line item
#

split_line_item_actual_usage_name = "split_line_item_actual_usage"
split_line_item_actual_usage_dtype = pl.Float64()
split_line_item_net_split_cost_name = "split_line_item_net_split_cost"
split_line_item_net_split_cost_dtype = pl.Float64()
split_line_item_net_unused_cost_name = "split_line_item_net_unused_cost"
split_line_item_net_unused_cost_dtype = pl.Float64()
split_line_item_parent_resource_id_name = "split_line_item_parent_resource_id"
split_line_item_parent_resource_id_dtype = pl.String()
split_line_item_public_on_demand_split_cost_name = (
    "split_line_item_public_on_demand_split_cost"
)
split_line_item_public_on_demand_split_cost_dtype = pl.Float64()
split_line_item_public_on_demand_unused_cost_name = (
    "split_line_item_public_on_demand_unused_cost"
)
split_line_item_public_on_demand_unused_cost_dtype = pl.Float64()
split_line_item_reserved_usage_name = "split_line_item_reserved_usage"
split_line_item_reserved_usage_dtype = pl.Float64()
split_line_item_split_cost_name = "split_line_item_split_cost"
split_line_item_split_cost_dtype = pl.Float64()
split_line_item_split_usage_name = "split_line_item_split_usage"
split_line_item_split_usage_dtype = pl.Float64()
split_line_item_split_usage_ratio_name = "split_line_item_split_usage_ratio"
split_line_item_split_usage_ratio_dtype = pl.Float64()
split_line_item_unused_cost_name = "split_line_item_unused_cost"
split_line_item_unused_cost_dtype = pl.Float64()

split_line_item_fields = {
    split_line_item_actual_usage_name: split_line_item_actual_usage_dtype,
    split_line_item_net_split_cost_name: split_line_item_net_split_cost_dtype,
    split_line_item_net_unused_cost_name: split_line_item_net_unused_cost_dtype,
    split_line_item_parent_resource_id_name: split_line_item_parent_resource_id_dtype,
    split_line_item_public_on_demand_split_cost_name: split_line_item_public_on_demand_split_cost_dtype,
    split_line_item_public_on_demand_unused_cost_name: split_line_item_public_on_demand_unused_cost_dtype,
    split_line_item_reserved_usage_name: split_line_item_reserved_usage_dtype,
    split_line_item_split_cost_name: split_line_item_split_cost_dtype,
    split_line_item_split_usage_name: split_line_item_split_usage_dtype,
    split_line_item_split_usage_ratio_name: split_line_item_split_usage_ratio_dtype,
    split_line_item_unused_cost_name: split_line_item_unused_cost_dtype,
}

#
# tag
# https://docs.aws.amazon.com/cur/latest/userguide/table-dictionary-cur2-tag-columns.html
#

tags_name = "tags"
tags_dtype = pl.List(pl.Struct({"key": pl.String(), "value": pl.String()}))

tags_fields = {tags_name: tags_dtype}

#
# resource tag
#
#

resource_tags_name = "resource_tags"
resource_tags_dtype = pl.List(pl.Struct({"key": pl.String(), "value": pl.String()}))

resource_tags_fields = {resource_tags_name: resource_tags_dtype}

all_fields = bill_fields | tags_fields | resource_tags_fields


#     {
#       "name": "reservation_amortized_upfront_cost_for_usage",
#       "type": "double"
#     },
#     {
#       "name": "reservation_amortized_upfront_fee_for_billing_period",
#       "type": "double"
#     },
#     {
#       "name": "reservation_availability_zone",
#       "type": "string"
#     },
#     {
#       "name": "reservation_effective_cost",
#       "type": "double"
#     },
#     {
#       "name": "reservation_end_time",
#       "type": "string"
#     },
#     {
#       "name": "reservation_modification_status",
#       "type": "string"
#     },
#     {
#       "name": "reservation_net_amortized_upfront_cost_for_usage",
#       "type": "double"
#     },
#     {
#       "name": "reservation_net_amortized_upfront_fee_for_billing_period",
#       "type": "double"
#     },
#     {
#       "name": "reservation_net_effective_cost",
#       "type": "double"
#     },
#     {
#       "name": "reservation_net_recurring_fee_for_usage",
#       "type": "double"
#     },
#     {
#       "name": "reservation_net_unused_amortized_upfront_fee_for_billing_period",
#       "type": "double"
#     },
#     {
#       "name": "reservation_net_unused_recurring_fee",
#       "type": "double"
#     },
#     {
#       "name": "reservation_net_upfront_value",
#       "type": "double"
#     },
#     {
#       "name": "reservation_normalized_units_per_reservation",
#       "type": "string"
#     },
#     {
#       "name": "reservation_number_of_reservations",
#       "type": "string"
#     },
#     {
#       "name": "reservation_recurring_fee_for_usage",
#       "type": "double"
#     },
#     {
#       "name": "reservation_reservation_a_r_n",
#       "type": "string"
#     },
#     {
#       "name": "reservation_start_time",
#       "type": "string"
#     },
#     {
#       "name": "reservation_subscription_id",
#       "type": "string"
#     },
#     {
#       "name": "reservation_total_reserved_normalized_units",
#       "type": "string"
#     },
#     {
#       "name": "reservation_total_reserved_units",
#       "type": "string"
#     },
#     {
#       "name": "reservation_units_per_reservation",
#       "type": "string"
#     },
#     {
#       "name": "reservation_unused_amortized_upfront_fee_for_billing_period",
#       "type": "double"
#     },
#     {
#       "name": "reservation_unused_normalized_unit_quantity",
#       "type": "double"
#     },
#     {
#       "name": "reservation_unused_quantity",
#       "type": "double"
#     },
#     {
#       "name": "reservation_unused_recurring_fee",
#       "type": "double"
#     },
#     {
#       "name": "reservation_upfront_value",
#       "type": "double"
#     },
#     {
#       "name": "resource_tags",
#       "type": "map"
#     },
#     {
#       "name": "savings_plan_amortized_upfront_commitment_for_billing_period",
#       "type": "double"
#     },
#     {
#       "name": "savings_plan_end_time",
#       "type": "string"
#     },
#     {
#       "name": "savings_plan_instance_type_family",
#       "type": "string"
#     },
#     {
#       "name": "savings_plan_net_amortized_upfront_commitment_for_billing_period",
#       "type": "double"
#     },
#     {
#       "name": "savings_plan_net_recurring_commitment_for_billing_period",
#       "type": "double"
#     },
#     {
#       "name": "savings_plan_net_savings_plan_effective_cost",
#       "type": "double"
#     },
#     {
#       "name": "savings_plan_offering_type",
#       "type": "string"
#     },
#     {
#       "name": "savings_plan_payment_option",
#       "type": "string"
#     },
#     {
#       "name": "savings_plan_purchase_term",
#       "type": "string"
#     },
#     {
#       "name": "savings_plan_recurring_commitment_for_billing_period",
#       "type": "double"
#     },
#     {
#       "name": "savings_plan_region",
#       "type": "string"
#     },
#     {
#       "name": "savings_plan_savings_plan_a_r_n",
#       "type": "string"
#     },
#     {
#       "name": "savings_plan_savings_plan_effective_cost",
#       "type": "double"
#     },
#     {
#       "name": "savings_plan_savings_plan_rate",
#       "type": "double"
#     },
#     {
#       "name": "savings_plan_start_time",
#       "type": "string"
#     },
#     {
#       "name": "savings_plan_total_commitment_to_date",
#       "type": "double"
#     },
#     {
#       "name": "savings_plan_used_commitment",
#       "type": "double"
#     },
