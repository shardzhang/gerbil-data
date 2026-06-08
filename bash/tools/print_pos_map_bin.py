#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import struct
from pathlib import Path
from typing import BinaryIO, Dict, List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print all contents from an nn_pos_map.bin file."
    )
    parser.add_argument(
        "path",
        default=str("./nn_pos_map.bin"),
        help=f"Path to nn_pos_map.bin",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation width.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Print pos_map and target_map entries in compact one-line JSON format.",
    )
    parser.add_argument(
        "--f-index",
        type=int,
        help="Only print pos_map entries for the specified f_index.",
    )
    parser.add_argument(
        "--target_map",
        action="store_true",
        help="Only print target_map entries, sorted by index.",
    )
    return parser.parse_args()


def read_exact(reader: BinaryIO, size: int) -> bytes:
    data = reader.read(size)
    if len(data) != size:
        raise EOFError(f"Unexpected EOF: expected {size} bytes, got {len(data)} bytes")
    return data


def read_int_le(reader: BinaryIO) -> int:
    return struct.unpack("<i", read_exact(reader, 4))[0]


def read_long_le(reader: BinaryIO) -> int:
    return struct.unpack("<q", read_exact(reader, 8))[0]


def read_double_le(reader: BinaryIO) -> float:
    return struct.unpack("<d", read_exact(reader, 8))[0]


def parse_nn_pos_map(path: Path) -> Dict[str, object]:
    with path.open("rb") as reader:
        timestamp = read_long_le(reader)

        pos_map_size = read_int_le(reader)
        pos_map: List[Dict[str, object]] = []
        for _ in range(pos_map_size):
            pos_map.append(
                {
                    "f_index": read_int_le(reader),
                    "pos": read_long_le(reader),
                    "index": read_int_le(reader),
                    "mean": read_double_le(reader),
                    "std": read_double_le(reader),
                }
            )

        target_map_size = read_int_le(reader)
        target_map: List[Dict[str, int]] = []
        for _ in range(target_map_size):
            target_map.append(
                {
                    "target": read_int_le(reader),
                    "index": read_int_le(reader),
                }
            )

        trailing = reader.read()

    return {
        "file": str(path),
        "timestamp": timestamp,
        "pos_map_size": pos_map_size,
        "pos_map": pos_map,
        "target_map_size": target_map_size,
        "target_map": target_map,
        "trailing_bytes": len(trailing),
    }


def sort_document(doc: Dict[str, object]) -> Dict[str, object]:
    doc["pos_map"] = sorted(
        doc["pos_map"],
        key=lambda item: (int(item["f_index"]), int(item["index"]), int(item["pos"])),
    )
    doc["target_map"] = sorted(
        doc["target_map"],
        key=lambda item: (int(item["index"]), int(item["target"])),
    )
    return doc


def filter_document(doc: Dict[str, object], selected_f_index: int | None) -> Dict[str, object]:
    if selected_f_index is None:
        return doc

    doc["selected_f_index"] = selected_f_index
    doc["pos_map"] = [
        item for item in doc["pos_map"] if int(item["f_index"]) == selected_f_index
    ]
    doc["pos_map_size"] = len(doc["pos_map"])
    doc["target_map"] = []
    del doc["target_map_size"]
    return doc


def select_target_map_only(doc: Dict[str, object], target_map_only: bool) -> Dict[str, object]:
    if not target_map_only:
        return doc

    source_target_map_size = int(doc["target_map_size"])
    doc["selected_target_map"] = True
    doc["pos_map"] = []
    del doc["pos_map_size"]
    return doc


def dumps_compact_entries(doc: Dict[str, object], indent: int) -> str:
    outer_indent = " " * indent
    inner_indent = " " * (indent * 2)
    items = list(doc.items())
    lines = ["{"]

    for item_index, (key, value) in enumerate(items):
        suffix = "," if item_index < len(items) - 1 else ""
        key_text = json.dumps(key, ensure_ascii=False)

        if isinstance(value, list):
            if not value:
                lines.append(f"{outer_indent}{key_text}: []{suffix}")
                continue

            lines.append(f"{outer_indent}{key_text}: [")
            for entry_index, entry in enumerate(value):
                entry_suffix = "," if entry_index < len(value) - 1 else ""
                entry_text = json.dumps(entry, ensure_ascii=False, separators=(", ", ": "))
                lines.append(f"{inner_indent}{entry_text}{entry_suffix}")
            lines.append(f"{outer_indent}]{suffix}")
            continue

        value_text = json.dumps(value, ensure_ascii=False, separators=(", ", ": "))
        lines.append(f"{outer_indent}{key_text}: {value_text}{suffix}")

    lines.append("}")
    return "\n".join(lines)

def resolve_file_path(path_text: str) -> Path:
    path = Path(path_text).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"Expected a file, got: {path}")
    return path

def main() -> None:
    args = parse_args()
    if args.target_map and args.f_index is not None:
        raise ValueError("--target_map and --f-index cannot be used together")

    path = resolve_file_path(args.path)
    doc = parse_nn_pos_map(path)
    doc = select_target_map_only(doc, args.target_map)
    doc = filter_document(doc, args.f_index)
    doc = sort_document(doc)
    if args.compact:
        print(dumps_compact_entries(doc, args.indent))
        return
    print(json.dumps(doc, ensure_ascii=False, indent=args.indent))


if __name__ == "__main__":
    main()

# python3 print_pos_map_bin.py /Users/dazhang/PycharmProject/data/ml-1m-output/nn_pos_map.bin --compact --f-index 301
# python3 print_pos_map_bin.py /Users/dazhang/PycharmProject/data/ml-1m-output/nn_pos_map.bin --compact --target_map
