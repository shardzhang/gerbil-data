#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple

import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print one Parquet part file using pyarrow."
    )
    parser.add_argument(
        "path",
        help="A specific Parquet part file, for example ./parquet/part-00000.snappy.parquet",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of rows to print.",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation width.",
    )
    return parser.parse_args()


def validate_parquet_file(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"Expected a specific Parquet part file, got: {path}")
    if path.name.startswith("."):
        raise ValueError(f"Expected a Parquet data file, got metadata file: {path}")
    if not path.name.startswith("part-"):
        raise ValueError(
            f"Expected a Parquet part file like part-00000.parquet, got: {path.name}"
        )
    return path


def convert_value(value: Any) -> Any:
    if isinstance(value, bytes):
        try:
            text = value.decode("utf-8")
        except UnicodeDecodeError:
            return {"hex": value.hex()}
        if any(ord(ch) < 32 and ch not in "\t\n\r" for ch in text):
            return {"hex": value.hex()}
        return text
    return value


def iter_rows(path: Path) -> Iterator[Tuple[int, Dict[str, Any]]]:
    table = pq.read_table(str(path))
    col_names = table.column_names
    num_rows = table.num_rows
    for row_index in range(num_rows):
        row: Dict[str, Any] = {}
        for col_name in col_names:
            val = table.column(col_name)[row_index].as_py()
            row[col_name] = convert_value(val)
        yield row_index + 1, row


def main() -> None:
    args = parse_args()
    file_path = validate_parquet_file(Path(args.path).expanduser().resolve())

    printed = 0
    for record_index, row in iter_rows(file_path):
        doc = {
            "file": str(file_path),
            "row_index": record_index,
            "row": row,
        }
        print(json.dumps(doc, ensure_ascii=False, indent=args.indent))
        printed += 1
        if args.limit > 0 and printed >= args.limit:
            return


if __name__ == "__main__":
    main()
