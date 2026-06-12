"""
Shell Gas Station - Synthetic Data Generator
Generates customers, fuel transactions, and in-store purchases
to analyze cross-sell conversion (pumpers who also shop in-store).
"""

import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType,
)

try:
    from faker import Faker
except ImportError:
    raise ImportError("Install faker: pip install faker")

spark = SparkSession.builder.appName("ShellGasStationSyntheticData").getOrCreate()
fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
NUM_CUSTOMERS    = 500
NUM_FUEL_TXN     = 5_000   # ~10 visits per customer
STORE_CONVERSION = 0.38    # 38% of pumpers also go into the store
START_DATE       = datetime(2025, 1, 1)
END_DATE         = datetime(2025, 12, 31)

FUEL_TYPES   = ["Regular", "Mid-Grade", "Premium", "Diesel"]
FUEL_PRICE   = {"Regular": 3.29, "Mid-Grade": 3.59, "Premium": 3.89, "Diesel": 3.69}
PUMP_COUNT   = 12
PAYMENT_MODES = ["Credit Card", "Debit Card", "Shell Gift Card", "Shell Rewards", "Cash"]

STORE_CATEGORIES = {
    "Beverages":    (0.35, 1.99, 5.49),   # (weight, min_price, max_price)
    "Snacks":       (0.25, 0.99, 4.99),
    "Hot Food":     (0.15, 2.99, 8.99),
    "Tobacco":      (0.10, 7.99, 14.99),
    "Car Care":     (0.08, 3.99, 24.99),
    "Lottery":      (0.04, 1.00, 20.00),
    "Automotive":   (0.03, 4.99, 39.99),
}


def random_timestamp(start: datetime, end: datetime) -> datetime:
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta))


# --------------------------------------------------------------------------- #
# 1. Customers
# --------------------------------------------------------------------------- #
def generate_customers(n: int) -> list[dict]:
    loyalty_tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    records = []
    for i in range(n):
        cid = f"CUST-{i + 1:05d}"
        joined = random_timestamp(datetime(2020, 1, 1), START_DATE)
        records.append({
            "customer_id":    cid,
            "full_name":      fake.name(),
            "email":          fake.email(),
            "phone":          fake.phone_number(),
            "zip_code":       fake.zipcode(),
            "loyalty_member": random.random() < 0.60,
            "loyalty_tier":   random.choice(loyalty_tiers) if random.random() < 0.60 else None,
            "member_since":   joined,
            "preferred_fuel": random.choice(FUEL_TYPES),
        })
    return records


# --------------------------------------------------------------------------- #
# 2. Fuel transactions
# --------------------------------------------------------------------------- #
def generate_fuel_transactions(customers: list[dict], n: int) -> list[dict]:
    records = []
    for i in range(n):
        cust    = random.choice(customers)
        fuel    = random.choice(FUEL_TYPES)
        gallons = round(random.uniform(3.0, 20.0), 3)
        price   = FUEL_PRICE[fuel]
        ts      = random_timestamp(START_DATE, END_DATE)
        records.append({
            "fuel_txn_id":    f"FUEL-{i + 1:07d}",
            "customer_id":    cust["customer_id"],
            "transaction_ts": ts,
            "pump_number":    random.randint(1, PUMP_COUNT),
            "fuel_type":      fuel,
            "gallons":        gallons,
            "price_per_gallon": price,
            "total_fuel_cost": round(gallons * price, 2),
            "payment_method": random.choice(PAYMENT_MODES),
            "loyalty_points_earned": int(gallons * 10) if cust["loyalty_member"] else 0,
        })
    return records


# --------------------------------------------------------------------------- #
# 3. Store purchases  (subset of fuel customers who walked in)
# --------------------------------------------------------------------------- #
def generate_store_purchases(fuel_txns: list[dict]) -> list[dict]:
    categories   = list(STORE_CATEGORIES.keys())
    cat_weights  = [v[0] for v in STORE_CATEGORIES.values()]
    cat_prices   = {k: (v[1], v[2]) for k, v in STORE_CATEGORIES.items()}

    records = []
    purchase_id = 1
    item_id     = 1

    for txn in fuel_txns:
        if random.random() > STORE_CONVERSION:
            continue

        # 1-4 items per store visit
        num_items  = random.randint(1, 4)
        store_ts   = txn["transaction_ts"] + timedelta(minutes=random.randint(2, 15))
        basket_total = 0.0
        items_bought = []

        for _ in range(num_items):
            cat        = random.choices(categories, weights=cat_weights, k=1)[0]
            lo, hi     = cat_prices[cat]
            unit_price = round(random.uniform(lo, hi), 2)
            qty        = random.randint(1, 3)
            line_total = round(unit_price * qty, 2)
            basket_total += line_total
            items_bought.append({
                "store_item_id":   f"ITEM-{item_id:08d}",
                "fuel_txn_id":     txn["fuel_txn_id"],
                "customer_id":     txn["customer_id"],
                "purchase_id":     f"STORE-{purchase_id:07d}",
                "store_ts":        store_ts,
                "category":        cat,
                "product_name":    fake.bs().title()[:40],
                "unit_price":      unit_price,
                "quantity":        qty,
                "line_total":      line_total,
            })
            item_id += 1

        for item in items_bought:
            item["basket_total"] = round(basket_total, 2)
            records.append(item)

        purchase_id += 1

    return records


# --------------------------------------------------------------------------- #
# Schemas
# --------------------------------------------------------------------------- #
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",    StringType(),    False),
    StructField("full_name",      StringType(),    True),
    StructField("email",          StringType(),    True),
    StructField("phone",          StringType(),    True),
    StructField("zip_code",       StringType(),    True),
    StructField("loyalty_member", BooleanType(),   True),
    StructField("loyalty_tier",   StringType(),    True),
    StructField("member_since",   TimestampType(), True),
    StructField("preferred_fuel", StringType(),    True),
])

FUEL_TXN_SCHEMA = StructType([
    StructField("fuel_txn_id",          StringType(),    False),
    StructField("customer_id",          StringType(),    False),
    StructField("transaction_ts",       TimestampType(), False),
    StructField("pump_number",          IntegerType(),   True),
    StructField("fuel_type",            StringType(),    True),
    StructField("gallons",              DoubleType(),    True),
    StructField("price_per_gallon",     DoubleType(),    True),
    StructField("total_fuel_cost",      DoubleType(),    True),
    StructField("payment_method",       StringType(),    True),
    StructField("loyalty_points_earned", IntegerType(),  True),
])

STORE_PURCHASE_SCHEMA = StructType([
    StructField("store_item_id",  StringType(),    False),
    StructField("fuel_txn_id",    StringType(),    False),
    StructField("customer_id",    StringType(),    False),
    StructField("purchase_id",    StringType(),    False),
    StructField("store_ts",       TimestampType(), False),
    StructField("category",       StringType(),    True),
    StructField("product_name",   StringType(),    True),
    StructField("unit_price",     DoubleType(),    True),
    StructField("quantity",       IntegerType(),   True),
    StructField("line_total",     DoubleType(),    True),
    StructField("basket_total",   DoubleType(),    True),
])


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    print("Generating Shell Gas Station synthetic data...")

    customers   = generate_customers(NUM_CUSTOMERS)
    fuel_txns   = generate_fuel_transactions(customers, NUM_FUEL_TXN)
    store_items = generate_store_purchases(fuel_txns)

    # -- DataFrames --
    df_customers = spark.createDataFrame(customers, schema=CUSTOMER_SCHEMA)
    df_fuel      = spark.createDataFrame(fuel_txns,  schema=FUEL_TXN_SCHEMA)
    df_store     = spark.createDataFrame(store_items, schema=STORE_PURCHASE_SCHEMA)

    # -- Write as Delta tables (Unity Catalog or default catalog) --
    catalog = "main"
    schema  = "shell_gas"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    for df, tbl in [
        (df_customers, "customers"),
        (df_fuel,      "fuel_transactions"),
        (df_store,     "store_purchases"),
    ]:
        full_name = f"{catalog}.{schema}.{tbl}"
        df.write.format("delta").mode("overwrite").saveAsTable(full_name)
        print(f"  Wrote {df.count():,} rows -> {full_name}")

    # -- Quick conversion summary --
    pumpers       = df_fuel.select("customer_id").distinct().count()
    store_shoppers = df_store.select("customer_id").distinct().count()
    conversion_pct = store_shoppers / pumpers * 100

    print(f"\n--- Shell Gas Station Summary ---")
    print(f"  Customers          : {df_customers.count():,}")
    print(f"  Fuel transactions  : {df_fuel.count():,}")
    print(f"  Store line items   : {df_store.count():,}")
    print(f"  Unique pumpers     : {pumpers:,}")
    print(f"  Store shoppers     : {store_shoppers:,}")
    print(f"  Conversion rate    : {conversion_pct:.1f}%")
    print("---------------------------------")


if __name__ == "__main__":
    main()
