"""Filter, derive, project, sort, and preview a Hologres table."""

from __future__ import annotations

import os

from _common import connect

import hologres.dataframe as hg


def run() -> None:
    orders_table = os.environ.get("HOLO_ORDERS_TABLE", "public.orders")
    with connect() as session:
        result = (
            session.table(orders_table)
            .filter(hg.col("amount") > 100)
            .with_column("tax", hg.col("amount") * 0.06)
            .select("order_id", "region", "amount", "tax")
            .order_by(hg.col("amount").desc())
            .limit(10)
        )
        result.show()


if __name__ == "__main__":
    run()
