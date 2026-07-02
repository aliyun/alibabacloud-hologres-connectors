"""PREPARE_GET_DATA mode: create and populate a row-oriented table for GET tests.

Mirrors Java PrepareGetData. Extends FixedCopyTest with overrides:
  - orientation = "row"
  - testByTime = false (write exactly rowNumber rows)
  - hasPk = true
  - deleteTableAfterDone = false

Config keys (prefix "prepareGetData."):
  - rowNumber: number of rows to insert (default 1,000,000)
  - orientation: table orientation (default "row")
  - columnCount: number of data columns (default from put.columnCount)

All other put.* config keys are inherited from PutTestConf.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .conf_loader import load_conf
from .config import PutTestConf
from .fixed_copy_test import FixedCopyTest

logger = logging.getLogger(__name__)


class PrepareGetData(FixedCopyTest):
    """Create and populate a table for GET benchmarks."""

    def init(self) -> None:
        # Load prepareGetData.* overrides
        prep_conf = _PrepareGetDataConf()
        load_conf(self.conf_name, "prepareGetData.", prep_conf)

        # Apply overrides matching Java PrepareGetData.init()
        self.conf.test_by_time = False
        self.conf.row_number = prep_conf.row_number
        self.conf.orientation = prep_conf.orientation
        self.conf.has_pk = True
        self.conf.delete_table_after_done = False

        if prep_conf.column_count > 0:
            self.conf.column_count = prep_conf.column_count

        logger.info(
            "PrepareGetData: %d rows, orientation=%s, columns=%d",
            self.conf.row_number,
            self.conf.orientation,
            self.conf.column_count,
        )


@dataclass
class _PrepareGetDataConf:
    """Config holder for prepareGetData.* keys."""

    row_number: int = 1_000_000
    orientation: str = "row"
    column_count: int = -1
