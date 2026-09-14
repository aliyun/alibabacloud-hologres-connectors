"""Create and optionally refresh a Hologres Dynamic Table."""

from __future__ import annotations

import os

from _common import connect, required


def run() -> None:
    source_table = os.environ.get("HOLO_ORDERS_TABLE", "public.orders")
    target_table = required("HOLO_DYNAMIC_TABLE")
    with connect() as session:
        source = session.table(source_table).select(
            "order_id", "user_id", "region", "amount"
        )
        source.as_dynamic_table(
            target_table,
            freshness=os.environ.get("HOLO_FRESHNESS", "10 minutes"),
            refresh_mode=os.environ.get("HOLO_REFRESH_MODE", "auto"),
            auto_refresh=os.environ.get("HOLO_AUTO_REFRESH", "1") == "1",
            mode=os.environ.get("HOLO_DYNAMIC_MODE", "ignore"),
        )
        if os.environ.get("HOLO_REFRESH_NOW") == "1":
            session.refresh_table(target_table)
        session.table(target_table).limit(10).show()


if __name__ == "__main__":
    run()
