"""Syntax checks for runnable Hologres examples."""

from pathlib import Path

import pytest

EXAMPLES = sorted((Path(__file__).parents[1] / "examples").glob("*.py"))


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda path: path.stem)
def test_example_is_valid_python(path):
    source = path.read_text(encoding="utf-8")
    compile(source, str(path), "exec")
