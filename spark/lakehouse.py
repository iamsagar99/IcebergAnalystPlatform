"""Shared helpers for the seeding / maintenance jobs. Everything derives from a fixed SEED so reruns are identical."""
import argparse
import time

from pyspark.sql import SparkSession

CATALOG = "lakehouse"
SEED = 20260701            # fixed seed: every column is a pure function of (row id, SEED)
START_DATE = "2026-07-01"  # 90-day window of synthetic activity
DAYS = 90

# Local row counts (small enough for a laptop; production counts are in docs/business-scenario.md)
N_TRANSACTIONS = 1_000_000
N_DUPLICATE_TRANSACTIONS = 5_000   # bronze contains re-delivered rows; silver de-duplicates them
N_EVENTS = 600_000
N_CUSTOMERS = 50_000
N_CDC_UPDATES = 2_000
N_CDC_DELETES = 500
N_CDC_INSERTS = 1_000
N_STORES = 200
N_CATEGORIES = 40

NAMESPACES = ("bronze", "silver", "gold")


def session(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def sql(spark, statement, show=False):
    df = spark.sql(statement)
    if show:
        df.show(truncate=False)
    return df


def timed(label):
    class _T:
        def __enter__(self):
            self.t = time.time()
            print(f"--> {label} ...", flush=True)
            return self

        def __exit__(self, *a):
            print(f"<-- {label}: {time.time() - self.t:.1f}s", flush=True)
    return _T()


def arg_parser(description):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--catalog", default=CATALOG)
    return p
