#!/usr/bin/env python3
"""Deterministic synthetic data -> bronze tables.

Every column is a pure function of the row id and the fixed SEED (hash-derived), so the output is identical
on every run and on every partitioning. No random module, no clock reads. Two ingest batches are produced:
  batch 1 : initial load (transactions, events, customers as CDC inserts)
  batch 2 : a CDC delta for customers (updates, deletes, new inserts) + re-delivered duplicate transactions
"""
from pyspark.sql import functions as F

from lakehouse import (DAYS, N_CATEGORIES, N_CDC_DELETES, N_CDC_INSERTS, N_CDC_UPDATES, N_CUSTOMERS,
                       N_DUPLICATE_TRANSACTIONS, N_EVENTS, N_STORES, N_TRANSACTIONS, SEED, START_DATE,
                       arg_parser, session, timed)

REGIONS = ["North", "South", "East", "West", "Central"]
COUNTRIES = ["US", "CA", "GB", "DE", "FR"]
CITIES = ["Springfield", "Riverton", "Fairview", "Greenville", "Bristol", "Madison", "Georgetown", "Clinton"]
SEGMENTS = ["consumer", "small_business", "enterprise"]
TIERS = ["none", "silver", "gold", "platinum"]
PAYMENTS = ["card", "cash", "wallet", "giftcard"]
CHANNELS = ["store", "web", "app"]
DEVICES = ["desktop", "mobile", "tablet"]
EVENT_TYPES = ["page_view", "page_view", "page_view", "search", "add_to_cart", "checkout", "purchase"]
PAGES = ["/home", "/search", "/category", "/product", "/cart", "/checkout"]
DEPARTMENTS = ["grocery", "apparel", "electronics", "home", "beauty"]
FIRST = ["Ava", "Liam", "Noah", "Mia", "Zoe", "Ethan", "Ivy", "Owen", "Leo", "Nora", "Kai", "Isla"]
LAST = ["Smith", "Chen", "Patel", "Garcia", "Mueller", "Rossi", "Khan", "Silva", "Novak", "Okafor", "Tanaka", "Dubois"]

def base_ts():
    """Window start as a Spark column (built lazily: Spark functions need an active session)."""
    return F.to_timestamp(F.lit(f"{START_DATE} 00:00:00"))


def h(col, salt):
    """Deterministic non-negative 64-bit hash of (col, salt, SEED)."""
    return F.abs(F.xxhash64(col, F.lit(salt), F.lit(SEED)))


def pick(col, salt, values):
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (h(col, salt) % len(values) + 1).cast("int"))


def ts_in_window(col, salt):
    seconds = (h(col, salt) % (DAYS * 86400)).cast("long")
    return (base_ts().cast("long") + seconds).cast("timestamp")


def transactions(spark, n, id_offset=0, batch=1):
    df = spark.range(n).withColumn("transaction_id", F.col("id") + id_offset)
    idc = F.col("transaction_id")
    qty = (h(idc, "qty") % 5 + 1).cast("int")
    price = (h(idc, "price") % 19000 / 100 + 1.0).cast("decimal(10,2)")
    return (df.select(
        idc,
        ts_in_window(idc, "ts").alias("transaction_ts"),
        (h(idc, "store") % N_STORES + 1).cast("int").alias("store_id"),
        # skewed customers: 20% of ids map into the first 5% of customers (realistic hot keys)
        F.when(h(idc, "hot") % 5 == 0, (h(idc, "cust") % (N_CUSTOMERS // 20) + 1))
         .otherwise(h(idc, "cust") % N_CUSTOMERS + 1).cast("bigint").alias("customer_id"),
        (h(idc, "cat") % N_CATEGORIES + 1).cast("int").alias("product_category_id"),
        qty.alias("quantity"),
        price.alias("unit_price"),
        (qty * price).cast("decimal(12,2)").alias("amount"),
        pick(idc, "pay", PAYMENTS).alias("payment_method"),
        pick(idc, "chan", CHANNELS).alias("channel"),
        F.when(h(idc, "status") % 50 == 0, "refunded").otherwise("completed").alias("status"),
        F.lit(batch).alias("ingest_batch_id"),
        (base_ts().cast("long") + DAYS * 86400 + batch * 3600).cast("timestamp").alias("ingested_at"),
    ))


def events(spark, n):
    df = spark.range(n)
    idc = F.col("id")
    return df.select(
        idc.alias("event_id"),
        ts_in_window(idc, "ets").alias("event_ts"),
        (h(idc, "ecust") % N_CUSTOMERS + 1).cast("bigint").alias("customer_id"),
        F.concat(F.lit("s-"), F.conv(F.hex(h(idc, "sess") % 200_000), 16, 36)).alias("session_id"),
        pick(idc, "etype", EVENT_TYPES).alias("event_type"),
        pick(idc, "echan", ["web", "app"]).alias("channel"),
        pick(idc, "dev", DEVICES).alias("device"),
        pick(idc, "page", PAGES).alias("page"),
        (h(idc, "ecat") % N_CATEGORIES + 1).cast("int").alias("product_category_id"),
        F.lit(1).alias("ingest_batch_id"),
    )


def customer_rows(spark, ids_df, op, batch, variant="", ts_offset=0):
    """Full customer rows (dimension + PII in one CDC record, as a source system would emit).
    ts_offset orders operations inside a batch deterministically (a delete after an update wins)."""
    idc = F.col("customer_id")
    v = variant  # salt tweak so 'updated' rows differ deterministically from the originals
    return ids_df.select(
        F.lit(op).alias("op"),
        (base_ts().cast("long") + DAYS * 86400 + batch * 3600 + ts_offset).cast("timestamp").alias("op_ts"),
        idc,
        pick(idc, "fn", FIRST).alias("first_name"),
        pick(idc, "ln", LAST).alias("last_name"),
        pick(idc, "seg" + v, SEGMENTS).alias("segment"),
        pick(idc, "ctry", COUNTRIES).alias("country"),
        pick(idc, "city", CITIES).alias("city"),
        pick(idc, "tier" + v, TIERS).alias("loyalty_tier"),
        F.date_sub(F.to_date(F.lit(START_DATE)), (h(idc, "signup") % 1500).cast("int")).alias("signup_date"),
        (h(idc, "optin" + v) % 3 != 0).alias("marketing_opt_in"),
        F.concat(F.lower(pick(idc, "fn", FIRST)), F.lit("."), F.lower(pick(idc, "ln", LAST)), idc.cast("string"),
                 F.lit("@example.test")).alias("email"),
        F.concat(F.lit("+1-555-"), F.lpad((h(idc, "ph1") % 1000).cast("string"), 3, "0"), F.lit("-"),
                 F.lpad((h(idc, "ph2") % 10000).cast("string"), 4, "0")).alias("phone"),
        F.concat(F.lpad((h(idc, "n1") % 1000).cast("string"), 3, "0"), F.lit("-"),
                 F.lpad((h(idc, "n2") % 100).cast("string"), 2, "0"), F.lit("-"),
                 F.lpad((h(idc, "n3") % 10000).cast("string"), 4, "0")).alias("national_id"),
        F.date_sub(F.to_date(F.lit("2008-01-01")), (h(idc, "dob") % 20000).cast("int")).alias("date_of_birth"),
        F.concat((h(idc, "street") % 9999 + 1).cast("string"), F.lit(" "), pick(idc, "stn", LAST),
                 F.lit(" Street")).alias("street_address"),
        F.lit(batch).alias("ingest_batch_id"),
    )


def stores(spark):
    idc = F.col("store_id")
    return spark.range(1, N_STORES + 1).select(
        F.col("id").cast("int").alias("store_id")).select(
        idc,
        F.concat(F.lit("Store "), F.lpad(idc.cast("string"), 3, "0")).alias("store_name"),
        pick(idc, "reg", REGIONS).alias("region"),
        pick(idc, "sctry", COUNTRIES).alias("country"),
        pick(idc, "scity", CITIES).alias("city"),
        pick(idc, "stype", ["flagship", "standard", "express"]).alias("store_type"),
        F.date_sub(F.to_date(F.lit(START_DATE)), (h(idc, "opened") % 4000 + 30).cast("int")).alias("opened_date"),
    )


def categories(spark):
    idc = F.col("category_id")
    return spark.range(1, N_CATEGORIES + 1).select(F.col("id").cast("int").alias("category_id")).select(
        idc,
        F.concat(pick(idc, "dept", DEPARTMENTS), F.lit(" "), pick(idc, "cname", ["basics", "premium", "seasonal", "kids", "outdoor", "organic", "classic", "smart"])).alias("category_name"),
        pick(idc, "dept", DEPARTMENTS).alias("department"),
        (h(idc, "margin") % 4000 / 100 + 5).cast("decimal(5,2)").alias("margin_pct"),
    )


def main():
    a = arg_parser(__doc__).parse_args()
    spark = session("generate_data")
    c = a.catalog

    with timed("bronze.transactions_raw batch 1 (+ duplicates re-delivered in batch 2)"):
        t1 = transactions(spark, N_TRANSACTIONS, batch=1)
        t1.writeTo(f"{c}.bronze.transactions_raw").overwritePartitions()
        dup = transactions(spark, N_DUPLICATE_TRANSACTIONS, batch=2)  # ids 0..4999 again, identical business content
        dup.writeTo(f"{c}.bronze.transactions_raw").append()

    with timed("bronze.events_raw"):
        events(spark, N_EVENTS).writeTo(f"{c}.bronze.events_raw").overwritePartitions()

    with timed("bronze.customers_cdc batch 1 (initial inserts)"):
        ids = spark.range(1, N_CUSTOMERS + 1).select(F.col("id").alias("customer_id"))
        customer_rows(spark, ids, "I", 1).writeTo(f"{c}.bronze.customers_cdc").overwritePartitions()

    with timed("bronze.customers_cdc batch 2 (CDC delta: updates, deletes, inserts)"):
        upd_ids = spark.range(1, N_CDC_UPDATES + 1).select((F.col("id") * 7).alias("customer_id"))          # 7,14,...
        del_ids = spark.range(1, N_CDC_DELETES + 1).select((F.col("id") * 97).alias("customer_id"))         # 97,194,...
        new_ids = spark.range(N_CUSTOMERS + 1, N_CUSTOMERS + N_CDC_INSERTS + 1).select(F.col("id").alias("customer_id"))
        delta = (customer_rows(spark, upd_ids, "U", 2, variant="_v2")
                 .unionByName(customer_rows(spark, del_ids, "D", 2, ts_offset=60))  # 20 ids are both updated and deleted: delete is later
                 .unionByName(customer_rows(spark, new_ids, "I", 2)))
        delta.writeTo(f"{c}.bronze.customers_cdc").overwritePartitions()

    with timed("silver.stores / silver.product_categories (reference data, loaded directly)"):
        stores(spark).writeTo(f"{c}.silver.stores").overwritePartitions()
        categories(spark).writeTo(f"{c}.silver.product_categories").overwritePartitions()

    for t in ["bronze.transactions_raw", "bronze.events_raw", "bronze.customers_cdc", "silver.stores", "silver.product_categories"]:
        print(f"{t}: {spark.table(f'{c}.{t}').count()} rows")


if __name__ == "__main__":
    main()
