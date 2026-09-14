"""Append or upsert small in-memory records into an existing Hologres table."""

from __future__ import annotations

import os

from _common import connect, required

ROWS = [
    (1, "east", 120),
    (2, "west", 80),
]


def run() -> None:
    target = required("HOLO_WRITE_TABLE")
    with connect() as session:
        frame = session.from_records(
            ROWS,
            columns=["id", "region", "amount"],
        )
        if os.environ.get("HOLO_WRITE_UPSERT") == "1":
            frame.write(target, on_conflict=("update", ["id"]))
        else:
            frame.write(target)


if __name__ == "__main__":
    run()
