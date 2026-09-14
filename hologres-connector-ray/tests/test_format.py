"""Test that all Python code is formatted with ruff."""

import subprocess
import sys
from pathlib import Path

import pytest

_TARGETS = ["hologres_ray/", "tests/"]
# Relative targets are resolved against the package root, not the shell's cwd.
_ROOT = Path(__file__).resolve().parent.parent


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
        [sys.executable, "-m", "ruff", "format", "--check", *_TARGETS],
        capture_output=True,
        text=True,
        cwd=_ROOT,
    )
    if result.returncode != 0:
        msg = f"Code is not formatted. Run 'ruff format {' '.join(_TARGETS)}' to fix.\n"
        msg += result.stdout + result.stderr
        raise AssertionError(msg)
