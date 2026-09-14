"""Read from or export to OSS through Hologres ``EXTERNAL_FILES``."""

from __future__ import annotations

import os

from _common import connect, required


def read_oss() -> None:
    path = required("HOLO_OSS_SOURCE")
    file_format = os.environ.get("HOLO_OSS_FORMAT", "csv")
    endpoint = os.environ.get("HOLO_OSS_ENDPOINT")
    role_arn = os.environ.get("HOLO_OSS_ROLE_ARN")
    options = {"csv_skip_header": True} if file_format == "csv" else {}
    with connect() as session:
        frame = session.read_files(
            path,
            format=file_format,
            oss_endpoint=endpoint,
            role_arn=role_arn,
            **options,
        )
        frame.limit(10).show()


def export_csv() -> None:
    source_table = required("HOLO_EXPORT_TABLE")
    destination = required("HOLO_OSS_DESTINATION")
    with connect() as session:
        session.table(source_table).write_files(
            destination,
            oss_endpoint=os.environ.get("HOLO_OSS_ENDPOINT"),
            role_arn=os.environ.get("HOLO_OSS_ROLE_ARN"),
            target_file_size_mb=100,
        )


if __name__ == "__main__":
    if os.environ.get("HOLO_OSS_EXPORT") == "1":
        export_csv()
    else:
        read_oss()
