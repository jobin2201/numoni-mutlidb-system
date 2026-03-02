#!/usr/bin/env python
"""Export authentication DB collections to JSON and build metadata.
This script only touches databases/authentication and authentication metadata.
"""
import os
import json
from pathlib import Path

try:
    from pymongo import MongoClient
    from bson import json_util
except Exception as exc:
    print("Missing dependencies. Install with: pip install pymongo bson")
    print(f"Error: {exc}")
    raise SystemExit(1)

from build_collection_metadata import extract_collection_metadata

BASE_DIR = Path(__file__).parent.parent
DATABASES_PATH = BASE_DIR / "databases" / "authentication"
OUTPUT_METADATA = Path(__file__).parent / "authentication_collections_metadata.json"
MAX_TOKEN_BUDGET = 2400

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = "authentication"


def export_collection(db, collection_name, output_dir):
    output_path = output_dir / f"{collection_name}.json"
    records = list(db[collection_name].find({}))
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, default=json_util.default, indent=2)
    return len(records)


def build_auth_metadata(output_dir):
    collections_metadata = {}

    for json_file in sorted(output_dir.glob("*.json")):
        collection_name = json_file.stem
        metadata = extract_collection_metadata(json_file, max_records=50, max_unique_values=2)
        if metadata:
            collections_metadata[collection_name] = metadata

    def estimate_tokens(data):
        metadata_str = json.dumps(data)
        return int(len(metadata_str.split()) * 1.3)

    def trim_metadata(data, max_fields, max_values):
        trimmed = {}
        for collection_name, metadata in data.items():
            fields = metadata.get("fields", [])[:max_fields]
            sample_values = metadata.get("sample_values", {})

            if max_values <= 0:
                new_sample_values = {}
            else:
                new_sample_values = {
                    key: list(values)[:max_values]
                    for key, values in sample_values.items()
                    if key in fields
                }

            trimmed[collection_name] = {
                **metadata,
                "fields": fields,
                "sample_values": new_sample_values
            }
        return trimmed

    trimmed_metadata = collections_metadata
    if estimate_tokens(trimmed_metadata) > MAX_TOKEN_BUDGET:
        trim_plan = [
            (10, 2),
            (8, 2),
            (8, 1),
            (6, 1),
            (5, 1),
            (4, 1),
            (4, 0)
        ]
        for max_fields, max_values in trim_plan:
            trimmed_metadata = trim_metadata(collections_metadata, max_fields, max_values)
            if estimate_tokens(trimmed_metadata) <= MAX_TOKEN_BUDGET:
                break

    with open(OUTPUT_METADATA, "w", encoding="utf-8") as f:
        json.dump(trimmed_metadata, f, indent=2)

    print(f"Saved metadata: {OUTPUT_METADATA}")
    print(f"Collections: {len(trimmed_metadata)}")
    print(f"Estimated tokens: {estimate_tokens(trimmed_metadata)}")


def main():
    DATABASES_PATH.mkdir(parents=True, exist_ok=True)

    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]

    collections = db.list_collection_names()
    if not collections:
        print("No collections found in authentication DB.")
        return

    print(f"Exporting {len(collections)} collections to {DATABASES_PATH}...")
    for name in collections:
        count = export_collection(db, name, DATABASES_PATH)
        print(f"  - {name}: {count} records")

    build_auth_metadata(DATABASES_PATH)


if __name__ == "__main__":
    main()
