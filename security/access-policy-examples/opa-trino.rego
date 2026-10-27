# Open Policy Agent policy for Trino's OPA access control (access-control.name=opa).
# Same model as infrastructure/trino/rules.json, expressed as code. Trino POSTs {"input": {"context": {...}, "action": {...}}}
# to opa.policy.uri (e.g. http://opa:8181/v1/data/trino/allow); column masks and row filters use the
# opa.policy.column-masking-uri / opa.policy.row-filters-uri endpoints (package trino, rules columnMask / rowFilters).
package trino

import rego.v1

default allow := false

groups := input.context.identity.groups

is_admin if "admins" in groups
is_etl if "etl" in groups
is_analyst if "analysts" in groups
is_restricted if "restricted_analysts" in groups

# --- admins and the ETL identity may do anything in catalog lakehouse ---------------------------------
allow if is_admin
allow if {
    is_etl
    input.action.resource.table.catalogName == "lakehouse"
}

# --- every authenticated user may run queries and see the system catalog read-only ----------------------
allow if input.action.operation in {"ExecuteQuery", "AccessCatalog", "FilterCatalogs", "FilterSchemas", "FilterTables", "FilterColumns", "ShowSchemas", "ShowTables", "ShowColumns"}

# --- analysts: SELECT on gold.* and the non-PII silver tables; metadata tables ($) never --------------------
non_pii_silver := {"transactions", "events", "customers", "stores", "product_categories"}

allow if {
    input.action.operation == "SelectFromColumns"
    is_analyst_or_restricted
    t := input.action.resource.table
    t.catalogName == "lakehouse"
    not contains(t.tableName, "$")
    allowed_table(t)
}

is_analyst_or_restricted if is_analyst
is_analyst_or_restricted if is_restricted

allowed_table(t) if t.schemaName == "gold"
allowed_table(t) if {
    t.schemaName == "silver"
    t.tableName in non_pii_silver
}
allowed_table(t) if {
    is_restricted
    t.schemaName == "silver"
    t.tableName == "customer_pii"
    not "street_address" in t.columns
}

# --- column masks and row filters (restricted analysts on customer_pii) -----------------------------------
columnMask := {"expression": "regexp_replace(email, '^(.)(.*)(@.*)$', '$1***$3')"} if {
    is_restricted
    input.action.resource.column.columnName == "email"
    pii_table
}
columnMask := {"expression": "'***-**-' || substr(national_id, -4)"} if {
    is_restricted
    input.action.resource.column.columnName == "national_id"
    pii_table
}
columnMask := {"expression": "'***-***-' || substr(phone, -4)"} if {
    is_restricted
    input.action.resource.column.columnName == "phone"
    pii_table
}
columnMask := {"expression": "date_trunc('year', date_of_birth)"} if {
    is_restricted
    input.action.resource.column.columnName == "date_of_birth"
    pii_table
}

rowFilters contains {"expression": "marketing_opt_in = true"} if {
    is_restricted
    pii_table
}

pii_table if {
    input.action.resource.table.catalogName == "lakehouse"
    input.action.resource.table.schemaName == "silver"
    input.action.resource.table.tableName == "customer_pii"
}
