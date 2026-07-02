"""PREPARE_SCAN_DATA mode: create and populate a table for SCAN tests.

Mirrors Java PrepareScanData. Extends FixedCopyTest with overrides:
  - prefix_pk = true (creates composite PK (id, id1) with distribution_key='id')
  - orientation = "row"
  - testByTime = false (write exactly rowNumber rows)
  - hasPk = true
  - deleteTableAfterDone = false

Config keys (prefix "prepareScanData."):
  - rowNumber: number of rows to insert (default 1,000,000)
  - orientation: table orientation (default "row")
  - recordCountPerPrefix: rows per prefix value (default 100)

All other put.* config keys are inherited from PutTestConf.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .conf_loader import load_conf
from .fixed_copy_test import FixedCopyTest

logger = logging.getLogger(__name__)


class PrepareScanData(FixedCopyTest):
    """Create and populate a table for SCAN benchmarks."""

    def init(self) -> None:
        # Load prepareScanData.* overrides
        prep_conf = _PrepareScanDataConf()
        load_conf(self.conf_name, "prepareScanData.", prep_conf)

        # Apply overrides matching Java PrepareScanData.init()
        self.conf.test_by_time = False
        self.conf.row_number = prep_conf.row_number
        self.conf.orientation = prep_conf.orientation
        self.conf.has_pk = True
        self.conf.prefix_pk = True
        self.conf.record_count_per_prefix = prep_conf.record_count_per_prefix
        self.conf.delete_table_after_done = False

        logger.info(
            "PrepareScanData: %d rows, orientation=%s, recordCountPerPrefix=%d",
            self.conf.row_number,
            self.conf.orientation,
            self.conf.record_count_per_prefix,
        )


@dataclass
class _PrepareScanDataConf:
    """Config holder for prepareScanData.* keys."""

    row_number: int = 1_000_000
    orientation: str = "row"
    record_count_per_prefix: int = 100
