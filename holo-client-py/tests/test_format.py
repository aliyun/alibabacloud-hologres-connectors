"""Test that all Python code is formatted with ruff."""

import subprocess
import sys

import pytest


def test_ruff_format():
    """Fail if any file is not formatted according to ruff."""
    try:
        subprocess.run(
            [sys.executable, "-m", "ruff", "--version"],
            capture_output=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("ruff not installed")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "format",
            "--check",
            "hologres/",
            "tests/",
            "perf/",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        msg = (
            "Code is not formatted. Run 'ruff format hologres/ tests/ perf/' to fix.\n"
        )
        msg += result.stdout + result.stderr
        raise AssertionError(msg)
