"""Join two Hologres tables and aggregate entirely inside the engine."""

from __future__ import annotations

import os

from _common import connect

import hologres.dataframe as hg


def run() -> None:
    orders_table = os.environ.get("HOLO_ORDERS_TABLE", "public.orders")
    users_table = os.environ.get("HOLO_USERS_TABLE", "public.users")
    with connect() as session:
        orders = session.table(orders_table).select("user_id", "amount")
        users = session.table(users_table).select("user_id", "region")
        summary = (
            orders.join(users, on="user_id", how="left")
            .group_by("region")
            .agg(
                hg.sum("amount").alias("total_amount"),
                hg.count("*").alias("order_count"),
            )
            .order_by(hg.col("total_amount").desc())
        )
        summary.show()


if __name__ == "__main__":
    run()
