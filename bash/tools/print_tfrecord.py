#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from tfrecord import example_pb2
from tfrecord.reader import tfrecord_iterator

FeatureValue = Dict[str, object]
FeatureMap = Dict[str, FeatureValue]

FEATURE_SUFFIXES = ("_raw", "_index", "_value")
FEATURE_MAP_FILE_NAMES = ("nn_pos_map.txt", "pos_map.txt")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print one TFRecord part file using the tfrecord library."
    )
    parser.add_argument(
        "path",
        help="A specific TFRecord part file, for example ./tfrecord/part-r-00000",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of records to print.",
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
        help="Print features in compact one-line JSON format.",
    )
    parser.add_argument(
        "--f-name",
        help="Only print the specified feature_name.",
    )
    parser.add_argument(
        "--f-index",
        type=int,
        help="Only print the feature resolved from the specified f_index.",
    )
    return parser.parse_args()


def validate_tfrecord_file(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")

    if not path.is_file():
        raise ValueError(f"Expected a specific TFRecord part file, got: {path}")

    if path.name.startswith(".") or path.name.endswith(".crc"):
        raise ValueError(f"Expected a TFRecord data file, got metadata file: {path}")

    if not path.name.startswith("part-"):
        raise ValueError(f"Expected a TFRecord part file like part-r-00000, got: {path.name}")

    return path


def render_bytes(value: bytes):
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError:
        return {"hex": value.hex()}

    if any(ord(ch) < 32 and ch not in "\t\n\r" for ch in text):
        return {"hex": value.hex()}
    return text


def parse_feature(feature: example_pb2.Feature) -> FeatureValue:
    kind = feature.WhichOneof("kind")
    if kind == "bytes_list":
        values = [render_bytes(value) for value in feature.bytes_list.value]
        return {"type": "bytes_list", "value": values}

    if kind == "float_list":
        values = [float(value) for value in feature.float_list.value]
        return {"type": "float_list", "value": values}

    if kind == "int64_list":
        values = [int(value) for value in feature.int64_list.value]
        return {"type": "int64_list", "value": values}

    return {"type": "empty", "value": []}


def parse_example(raw_record: bytes) -> FeatureMap:
    example = example_pb2.Example()
    example.ParseFromString(raw_record)

    features: FeatureMap = {}
    for feature_name in sorted(example.features.feature):
        features[feature_name] = parse_feature(example.features.feature[feature_name])
    return features


def iter_examples(path: Path) -> Iterator[Tuple[int, FeatureMap]]:
    iterator = tfrecord_iterator(data_path=str(path), index_path=None)
    for record_index, raw_record in enumerate(iterator, start=1):
        yield record_index, parse_example(raw_record)


def find_feature_map_file(file_path: Path) -> Path:
    current = file_path.parent.resolve()
    while True:
        for file_name in FEATURE_MAP_FILE_NAMES:
            candidate = current / file_name
            if candidate.exists() and candidate.is_file():
                return candidate.resolve()

        if current.parent == current:
            break
        current = current.parent

    names = ", ".join(FEATURE_MAP_FILE_NAMES)
    raise FileNotFoundError(
        f"Could not find any of [{names}] in parent directories of {file_path}"
    )


def parse_feature_name_index_map(path: Path) -> Dict[int, str]:
    index_to_name: Dict[int, str] = {}
    with path.open("r", encoding="utf-8") as reader:
        for line_number, raw_line in enumerate(reader, start=1):
            line = raw_line.rstrip("\n")
            if not line:
                continue

            parts = line.split(",")
            if parts[0] == "target":
                continue
            if len(parts) < 2:
                raise ValueError(f"Invalid feature map line at {path}:{line_number}")

            feature_name = parts[0]
            feature_index = int(parts[1])
            index_to_name[feature_index] = feature_name

    return index_to_name


def resolve_selected_feature(
    file_path: Path, feature_name: Optional[str], feature_index: Optional[int]
) -> Tuple[Optional[str], Optional[int]]:
    if feature_name is not None and feature_index is not None:
        raise ValueError("--f-name and --f-index cannot be used together")

    if feature_name is not None:
        return feature_name, None

    if feature_index is None:
        return None, None

    index_to_name = parse_feature_name_index_map(find_feature_map_file(file_path))
    if feature_index not in index_to_name:
        raise KeyError(f"f_index {feature_index} not found in feature map")

    return index_to_name[feature_index], feature_index


def filter_features(features: FeatureMap, selected_feature_name: Optional[str]) -> FeatureMap:
    if selected_feature_name is None:
        return features

    filtered: FeatureMap = {}
    prefixes = {selected_feature_name}
    if not selected_feature_name.endswith(FEATURE_SUFFIXES):
        prefixes.update(f"{selected_feature_name}{suffix}" for suffix in FEATURE_SUFFIXES)

    for feature_name, feature_value in features.items():
        if feature_name in prefixes:
            filtered[feature_name] = feature_value

    return filtered


def dumps_compact_feature_document(doc: Dict[str, object], indent: int) -> str:
    outer_indent = " " * indent
    inner_indent = " " * (indent * 2)
    items = list(doc.items())
    lines = ["{"]

    for item_index, (key, value) in enumerate(items):
        suffix = "," if item_index < len(items) - 1 else ""
        key_text = json.dumps(key, ensure_ascii=False)

        if key == "features":
            if not value:
                lines.append(f"{outer_indent}{key_text}: {{}}{suffix}")
                continue

            lines.append(f"{outer_indent}{key_text}: {{")
            feature_items = list(value.items())
            for feature_index, (feature_name, feature_value) in enumerate(feature_items):
                feature_suffix = "," if feature_index < len(feature_items) - 1 else ""
                feature_key = json.dumps(feature_name, ensure_ascii=False)
                feature_text = json.dumps(
                    feature_value,
                    ensure_ascii=False,
                    separators=(", ", ": "),
                )
                lines.append(f"{inner_indent}{feature_key}: {feature_text}{feature_suffix}")
            lines.append(f"{outer_indent}}}{suffix}")
            continue

        value_text = json.dumps(value, ensure_ascii=False, separators=(", ", ": "))
        lines.append(f"{outer_indent}{key_text}: {value_text}{suffix}")

    lines.append("}")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    file_path = validate_tfrecord_file(Path(args.path).expanduser().resolve())
    selected_feature_name, selected_feature_index = resolve_selected_feature(
        file_path,
        args.f_name,
        args.f_index,
    )

    printed = 0
    for record_index, features in iter_examples(file_path):
        filtered_features = filter_features(features, selected_feature_name)
        doc = {
            "file": str(file_path),
            "record_index": record_index,
            "features": filtered_features,
        }
        if selected_feature_name is not None:
            doc["selected_f_name"] = selected_feature_name
        if selected_feature_index is not None:
            doc["selected_f_index"] = selected_feature_index

        if args.compact:
            print(dumps_compact_feature_document(doc, args.indent))
        else:
            print(json.dumps(doc, ensure_ascii=False, indent=args.indent))
        printed += 1

        if args.limit > 0 and printed >= args.limit:
            return


if __name__ == "__main__":
    main()

# python3 print_tfrecord.py /Users/dazhang/PycharmProject/data/ml-1m-output/20260601/tfrecord/part-r-00000 --limit 1
# python3 print_tfrecord.py /Users/dazhang/PycharmProject/data/ml-1m-output/20260601/tfrecord/part-r-00000 --limit 1 --compact --f-name user_avg_rate
# python3 print_tfrecord.py /Users/dazhang/PycharmProject/data/ml-1m-output/20260601/tfrecord/part-r-00000 --limit 1 --compact --f-index 14


# python3 print_tfrecord.py /Users/dazhang/PycharmProject/data/ml-1m-output/20260601/tfrecord/part-r-00000 --limit 1 --compact --f-index 302
# python3 print_tfrecord.py /Users/dazhang/PycharmProject/data/ml-1m-output/20260601/tfrecord/part-r-00000 --limit 1 --compact --f-name user_movie_rate