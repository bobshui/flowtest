"""Append the current business time to a file once per flow run."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from prefect import flow, get_run_logger

OUTPUT_FILE_ENV = "FLOWGATE_HOURLY_OUTPUT_FILE"
TIMEZONE_ENV = "FLOWGATE_HOURLY_TIMEZONE"
DEFAULT_OUTPUT_FILE = "/tmp/flowgate-hourly-time.log"
DEFAULT_TIMEZONE = "Asia/Shanghai"


@flow(name="hourly-file-time", log_prints=True)
def hourly_file_time() -> str:
    """Append one ISO-8601 timestamp and return the written line."""
    logger = get_run_logger()
    output_file = Path(os.getenv(OUTPUT_FILE_ENV, DEFAULT_OUTPUT_FILE))
    timezone_name = os.getenv(TIMEZONE_ENV, DEFAULT_TIMEZONE)

    try:
        business_timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Invalid timezone: {timezone_name}") from exc

    timestamp = datetime.now(business_timezone).isoformat(timespec="seconds")
    line = f"haha{timestamp}\n"

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("a", encoding="utf-8") as file_handle:
        file_handle.write(line)
        file_handle.flush()
        os.fsync(file_handle.fileno())

    logger.info("Appended %s to %s", timestamp, output_file)
    return timestamp


if __name__ == "__main__":
    hourly_file_time()
