"""Seed reproducibility and data-quality checks, run through BOTH engines (the numbers must agree with
tests/expected.json, which was recorded from a real seed run and re-verified after a full re-seed)."""

from decimal import Decimal

import pytest

from conftest import connect


@pytest.fixture(scope="module")
def admin(engine):
    c = connect(engine, "admin")
    yield c
    c.close()


def one(rows):
    assert len(rows) == 1, rows
    return rows[0]


def test_transactions_checksum(admin, validation_queries, expected):
    n, sum_amount, sum_qty, distinct_ids = one(admin.query(validation_queries["v_checksum_transactions"]))
    exp = expected["silver.transactions"]
    assert n == exp["n"]
    assert Decimal(str(sum_amount)) == Decimal(exp["sum_amount"])
    assert sum_qty == exp["sum_qty"]
    assert distinct_ids == exp["distinct_ids"]


def test_silver_has_no_duplicates_but_bronze_does(admin, validation_queries, expected):
    assert one(admin.query(validation_queries["v_no_duplicates_in_silver"]))[0] == 0
    assert (
        one(admin.query(validation_queries["v_bronze_has_duplicates"]))[0]
        == expected["bronze.transactions_raw.duplicate_rows"]
    )


def test_events_count(admin, validation_queries, expected):
    assert one(admin.query(validation_queries["v_count_events"]))[0] == expected["silver.events"]["n"]


def test_cdc_merge_result(admin, validation_queries, expected):
    n, opted_in = one(admin.query(validation_queries["v_customers_after_cdc"]))
    assert n == expected["silver.customers"]["n"]
    assert opted_in == expected["silver.customers"]["opted_in"]
    assert one(admin.query(validation_queries["v_cdc_deleted_customer_absent"]))[0] == 0
    assert one(admin.query(validation_queries["v_cdc_inserted_customer_present"]))[0] == 1
    segment, tier = one(admin.query(validation_queries["v_cdc_updated_customer"]))
    assert segment and tier  # updated row exists with the v2 attributes


def test_pii_and_reference_counts(admin, validation_queries, expected):
    assert one(admin.query(validation_queries["v_pii_count"]))[0] == expected["silver.customer_pii"]["n"]
    assert one(admin.query(validation_queries["v_stores"]))[0] == expected["silver.stores"]["n"]
    assert one(admin.query(validation_queries["v_categories"]))[0] == expected["silver.product_categories"]["n"]


def test_gold_consistent_with_silver(admin, validation_queries, expected):
    n, gold_revenue = one(admin.query(validation_queries["v_gold_daily_sales"]))
    silver_revenue = one(admin.query(validation_queries["v_gold_matches_silver"]))[0]
    assert n == expected["gold.daily_sales_by_store"]["n"]
    assert Decimal(str(gold_revenue)) == Decimal(str(silver_revenue))


@pytest.mark.parametrize(
    "table", ["gold.category_sales_daily", "gold.customer_monthly_summary", "gold.event_funnel_daily"]
)
def test_gold_row_counts(admin, expected, table):
    assert one(admin.query(f"SELECT COUNT(*) FROM lakehouse.{table}"))[0] == expected[table]["n"]
