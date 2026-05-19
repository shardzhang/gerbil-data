#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from print_tfrecord import FEATURE_SUFFIXES
from print_tfrecord import iter_examples
from print_tfrecord import resolve_selected_feature
from print_tfrecord import validate_tfrecord_file


FeatureValue = Dict[str, object]
FeatureMap = Dict[str, FeatureValue]

SUFFIX_TO_KEY = {
    "_raw": "raw",
    "_index": "index",
    "_value": "value",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print one TFRecord part file and group *_raw/*_index/*_value fields together."
    )
    parser.add_argument(
        "path",
        help="A specific TFRecord part file, for example /Users/dazhang/PycharmProject/data/ml-1m-output/20260601/tfrecord/part-r-00000",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of records to print. Use 0 for no limit.",
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
        help="Print grouped features in compact one-line JSON format.",
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


def group_features(features: FeatureMap) -> Tuple[List[Dict[str, object]], Dict[str, FeatureValue]]:
    grouped: Dict[str, Dict[str, object]] = {}
    others: Dict[str, FeatureValue] = {}

    for feature_name, feature in features.items():
        matched = False
        for suffix, field_name in SUFFIX_TO_KEY.items():
            if feature_name.endswith(suffix):
                base_name = feature_name[:-len(suffix)]
                item = grouped.setdefault(
                    base_name,
                    {
                        "name": base_name,
                        "raw": None,
                        "index": None,
                        "value": None,
                    },
                )
                item[field_name] = feature.get("value")
                matched = True
                break

        if not matched:
            others[feature_name] = feature

    grouped_items = [grouped[name] for name in sorted(grouped)]
    return grouped_items, dict(sorted(others.items()))


def filter_grouped_output(
    grouped_features: List[Dict[str, object]],
    other_features: Dict[str, FeatureValue],
    selected_feature_name: Optional[str],
) -> Tuple[List[Dict[str, object]], Dict[str, FeatureValue]]:
    if selected_feature_name is None:
        return grouped_features, other_features

    filtered_groups = [
        group for group in grouped_features if str(group.get("name")) == selected_feature_name
    ]

    selected_other_names = {selected_feature_name}
    if not selected_feature_name.endswith(FEATURE_SUFFIXES):
        selected_other_names.update(
            f"{selected_feature_name}{suffix}" for suffix in FEATURE_SUFFIXES
        )

    filtered_others = {
        name: value for name, value in other_features.items() if name in selected_other_names
    }
    return filtered_groups, filtered_others


def dumps_compact_group_document(doc: Dict[str, object], indent: int) -> str:
    outer_indent = " " * indent
    inner_indent = " " * (indent * 2)
    items = list(doc.items())
    lines = ["{"]

    for item_index, (key, value) in enumerate(items):
        suffix = "," if item_index < len(items) - 1 else ""
        key_text = json.dumps(key, ensure_ascii=False)

        if key == "feature_groups":
            if not value:
                lines.append(f"{outer_indent}{key_text}: []{suffix}")
                continue

            lines.append(f"{outer_indent}{key_text}: [")
            for group_index, group in enumerate(value):
                group_suffix = "," if group_index < len(value) - 1 else ""
                group_text = json.dumps(group, ensure_ascii=False, separators=(", ", ": "))
                lines.append(f"{inner_indent}{group_text}{group_suffix}")
            lines.append(f"{outer_indent}]{suffix}")
            continue

        if key == "other_features":
            if not value:
                lines.append(f"{outer_indent}{key_text}: {{}}{suffix}")
                continue

            lines.append(f"{outer_indent}{key_text}: {{")
            other_items = list(value.items())
            for other_index, (other_name, other_value) in enumerate(other_items):
                other_suffix = "," if other_index < len(other_items) - 1 else ""
                other_key = json.dumps(other_name, ensure_ascii=False)
                other_text = json.dumps(
                    other_value,
                    ensure_ascii=False,
                    separators=(", ", ": "),
                )
                lines.append(f"{inner_indent}{other_key}: {other_text}{other_suffix}")
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
        grouped_features, other_features = group_features(features)
        grouped_features, other_features = filter_grouped_output(
            grouped_features,
            other_features,
            selected_feature_name,
        )
        doc = {
            "file": str(file_path),
            "record_index": record_index,
            "feature_groups": grouped_features,
        }
        if selected_feature_name is not None:
            doc["selected_f_name"] = selected_feature_name
        if selected_feature_index is not None:
            doc["selected_f_index"] = selected_feature_index
        if other_features:
            doc["other_features"] = other_features

        if args.compact:
            print(dumps_compact_group_document(doc, args.indent))
        else:
            print(json.dumps(doc, ensure_ascii=False, indent=args.indent))
        printed += 1

        if args.limit > 0 and printed >= args.limit:
            return


if __name__ == "__main__":
    main()

# python3 print_tfrecord_group.py /Users/dazhang/PycharmProject/data/ml-1m-output/20260601/tfrecord/part-r-00000 --limit 1
# python3 print_tfrecord_group.py /Users/dazhang/PycharmProject/data/ml-1m-output/20260601/tfrecord/part-r-00000 --limit 1 --compact --f-name user_avg_rate
# python3 print_tfrecord_group.py /Users/dazhang/PycharmProject/data/ml-1m-output/20260601/tfrecord/part-r-00000 --limit 1 --compact --f-index 14
