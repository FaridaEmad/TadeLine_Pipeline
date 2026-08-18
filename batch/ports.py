"""Download the current NGA World Port Index CSV."""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

import pandas as pd


PORTS_URL = (
    "https://msi.nga.mil/api/publications/download"
    "?key=16920959/SFH00000/UpdatedPub150.csv&type=view"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
LOGGER = logging.getLogger("ports")


def download_ports(output_directory: Path) -> Path:
    output_directory.mkdir(parents=True, exist_ok=True)
    today = date.today()
    first_day_of_month = today.replace(day=1)
    output_file = output_directory / f"ports_{first_day_of_month.isoformat()}.csv"
    temporary_file = output_file.with_suffix(".csv.tmp")

    LOGGER.info("Downloading World Port Index data")
    ports = pd.read_csv(PORTS_URL)
    if ports.empty:
        raise RuntimeError("The ports source returned no rows")

    ports.to_csv(temporary_file, index=False)
    temporary_file.replace(output_file)
    LOGGER.info("Saved %s ports to %s", len(ports), output_file)
    return output_file


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=Path(__file__).resolve().parent / "data" / "ports",
    )
    arguments = parser.parse_args()
    download_ports(arguments.output_directory)


if __name__ == "__main__":
    main()
