from pathlib import Path
from uuid import uuid4

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE_FILE = (
    PROJECT_ROOT
    / "data"
    / "source"
    / "orders"
    / "orders.csv"
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "source"
    / "orders"
    # / "orders_2026_09_25.csv"
    / "orders_2026_10_16.csv"
)


def main():
    orders = pd.read_csv(SOURCE_FILE)

    # Take two existing orders to simulate duplicates.
    duplicate_orders = orders.head(2).copy()

    # Take three existing orders as templates for new orders.
    new_orders = orders.iloc[2:4].copy()

    # Generate new order IDs while keeping the rest of the
    # structure realistic.
    new_orders["order_id"] = [
        uuid4().hex for _ in range(len(new_orders))
    ]

    # Make the new orders appear later than the original delivery.
    new_orders["order_purchase_timestamp"] = (
        "2026-10-16 10:00:00"
    )

    # Combine duplicate + new records.
    incremental_orders = pd.concat(
        [duplicate_orders, new_orders],
        ignore_index=True,
    )

    incremental_orders.to_csv(
        OUTPUT_FILE,
        index=False,
    )

    print(f"Created: {OUTPUT_FILE}")
    print(f"Records: {len(incremental_orders)}")

    # new addition in second file creation

    print(f"Existing orders: {len(duplicate_orders)}")
    print(f"New orders: {len(new_orders)}")

    # ----------------------------- 

    print("\nDelivery contents:")
    print(incremental_orders[[
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
    ]])


if __name__ == "__main__":
    main()