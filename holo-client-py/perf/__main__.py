"""Entry point: python -m perf <conf_file> <MODE>

Modes:
  INSERT             - Batched writes via HoloClient.put()
  ASYNC_INSERT       - Batched writes via AsyncHoloClient.put()
  FIXED_COPY         - Streaming writes via COPY binary protocol
  UPDATE             - Data update via HoloClient.put() (table must already have data)
  FIXED_COPY_UPDATE  - Data update via COPY protocol (table must already have data)
  GET                - Point query benchmark
  SCAN               - Prefix scan benchmark (SELECT * WHERE id = ?)
  PREPARE_GET_DATA   - Create and populate table for GET tests
  PREPARE_SCAN_DATA  - Create and populate table for SCAN tests
"""

from __future__ import annotations

import logging
import sys


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(threadName)s] %(levelname)s %(name)s - %(message)s",
    )

    if len(sys.argv) != 3:
        print("Usage: python -m perf <conf_file> <MODE>")
        print("  MODE: INSERT | UPDATE | FIXED_COPY | FIXED_COPY_UPDATE | GET")
        sys.exit(1)

    conf_name = sys.argv[1]
    method = sys.argv[2].upper()

    if method == "INSERT":
        from .insert_test import InsertTest

        InsertTest().run(conf_name)
    elif method == "UPDATE":
        from .insert_test import InsertTest

        InsertTest().run(conf_name, update_mode=True)
    elif method == "FIXED_COPY":
        from .fixed_copy_test import FixedCopyTest

        FixedCopyTest().run(conf_name)
    elif method == "FIXED_COPY_UPDATE":
        from .fixed_copy_test import FixedCopyTest

        FixedCopyTest().run(conf_name, update_mode=True)
    elif method == "GET":
        from .get_test import GetTest

        GetTest().run(conf_name)
    elif method == "ASYNC_INSERT":
        from .async_insert_test import AsyncInsertTest

        AsyncInsertTest().run(conf_name)
    elif method == "ASYNC_UPDATE":
        from .async_insert_test import AsyncInsertTest

        AsyncInsertTest().run(conf_name, update_mode=True)
    elif method == "SCAN":
        from .scan_test import ScanTest

        ScanTest().run(conf_name)
    elif method == "PREPARE_GET_DATA":
        from .prepare_get_data import PrepareGetData

        PrepareGetData().run(conf_name)
    elif method == "PREPARE_SCAN_DATA":
        from .prepare_scan_data import PrepareScanData

        PrepareScanData().run(conf_name)
    else:
        print(f"Unknown mode: {method}")
        print(
            "Supported modes: INSERT, ASYNC_INSERT, UPDATE, FIXED_COPY, FIXED_COPY_UPDATE, GET, SCAN, PREPARE_GET_DATA, PREPARE_SCAN_DATA"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
