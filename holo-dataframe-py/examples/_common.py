"""Shared connection helpers for the runnable Hologres examples."""

from __future__ import annotations

import os

import hologres.dataframe as hg


def connect():
    """Create a Session from ``HOLO_*`` environment variables."""
    return hg.connect(
        host=os.environ["HOLO_HOST"],
        port=int(os.environ.get("HOLO_PORT", "80")),
        dbname=os.environ.get("HOLO_DATABASE", "postgres"),
        user=os.environ["HOLO_USER"],
        password=os.environ["HOLO_PASSWORD"],
    )


def required(name: str) -> str:
    """Return a required non-empty environment variable."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"set {name} before running this example")
    return value
