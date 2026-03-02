#!/usr/bin/env python
"""Build a compact cross-DB metadata linker JSON for query routing.

Scans JSON collections under numoni_final/databases and writes a summarized
metadata file with:
- database -> collection -> columns
- small possible values per column (distinct samples)
- keyword linker map for quick prompt-to-collection matching

Usage:
    python summarised/summarised_metadata.py
    python summarised/summarised_metadata.py --max-tokens 1000
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


DEFAULT_MAX_TOKENS = 1000
DEFAULT_COLUMN_SAMPLE_VALUES = 4
DEFAULT_COLLECTION_ROW_SCAN = 300


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False)[:120]
        except Exception:
            return str(value)[:120]
    return str(value).strip()


def _tokenize(text: str) -> List[str]:
    base = re.sub(r"[^a-z0-9_\s]", " ", (text or "").lower())
    raw = [w for w in base.split() if len(w) >= 3]
    return [w for w in raw if w not in {"json", "data", "table", "record", "records", "details"}]


def _flatten_row(row: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for key, value in row.items():
        flat[str(key)] = value
    return flat


def _load_collection_rows(path: Path, row_limit: int) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    if isinstance(payload, list):
        rows = [r for r in payload if isinstance(r, dict)]
        return rows[:row_limit]
    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            rows = [r for r in payload["data"] if isinstance(r, dict)]
            return rows[:row_limit]
        return [payload]
    return []


def _collect_schema_and_samples(rows: List[Dict[str, Any]], value_limit: int) -> Tuple[List[str], Dict[str, List[str]]]:
    columns: List[str] = []
    seen_cols = set()
    value_bank: Dict[str, List[str]] = defaultdict(list)
    value_seen: Dict[str, set] = defaultdict(set)

    for row in rows:
        flat = _flatten_row(row)
        for col, value in flat.items():
            if col not in seen_cols:
                seen_cols.add(col)
                columns.append(col)

            sample = _safe_text(value)
            if not sample:
                continue
            if sample in value_seen[col]:
                continue
            value_seen[col].add(sample)
            if len(value_bank[col]) < value_limit:
                value_bank[col].append(sample)

    return sorted(columns), dict(value_bank)


def _build_keyword_index(summary: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    links: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    collection_aliases = {
        "authuser": ["auth", "authentication", "user", "account"],
        "login_activities": ["login", "signin", "digital", "activity"],
        "user_sessions": ["session", "active session", "user session", "device", "same device"],
        "audit_trail": ["audit", "audit trail", "logs"],
        "customerdetails": ["customer", "client", "buyer", "profile"],
        "merchantdetails": ["merchant", "business", "vendor", "store", "profile", "merchantdetails"],
        "customerdetails": ["customer", "client", "buyer", "profile", "customerdetails"],
        "wallet": ["wallet", "balance"],
        "transaction_history": ["transaction", "payment", "financial", "revenue"],
        "pay_on_us_notifications": ["notification", "alert"],
    }

    databases = summary.get("databases", {})
    for db_name, db_meta in databases.items():
        collections = (db_meta or {}).get("collections", {})
        for col_name, col_meta in collections.items():
            tokens = set()
            tokens.update(_tokenize(db_name))
            tokens.update(_tokenize(col_name))
            alias_key = col_name.lower().replace("_", "")
            for alias in collection_aliases.get(alias_key, []):
                tokens.update(_tokenize(alias))

            for token in tokens:
                links[token].append(
                    {
                        "database": db_name,
                        "collection": col_name,
                    }
                )

    # Keep index compact and deterministic
    output: Dict[str, List[Dict[str, str]]] = {}
    for token in sorted(links):
        unique = []
        seen = set()
        for item in links[token]:
            key = (item["database"], item["collection"])
            if key in seen:
                continue
            seen.add(key)
            unique.append(item)
        output[token] = unique[:6]

    return output


def _estimate_tokens(obj: Any) -> int:
    text = json.dumps(obj, ensure_ascii=False)
    return max(1, len(text) // 4)


def _is_key_column(column_name: str) -> bool:
    col = (column_name or "").lower()
    key_parts = [
        "id",
        "user",
        "customer",
        "merchant",
        "name",
        "email",
        "phone",
        "status",
        "type",
        "activity",
        "amount",
        "balance",
        "wallet",
        "date",
        "time",
        "reference",
        "transaction",
        "notification",
        "audit",
        "session",
    ]
    return any(part in col for part in key_parts)


def _shrink_to_token_budget(summary: Dict[str, Any], max_tokens: int) -> Dict[str, Any]:
    """Reduce sample values/index sizes until summary fits token budget."""
    result = json.loads(json.dumps(summary))
    if _estimate_tokens(result) <= max_tokens:
        return result

    # Step 1: trim column possible values
    for db_meta in result.get("databases", {}).values():
        for col_meta in db_meta.get("collections", {}).values():
            key_values = col_meta.get("possible_values", {})
            for field in list(key_values.keys()):
                key_values[field] = key_values[field][:2]
    if _estimate_tokens(result) <= max_tokens:
        return result

    # Step 2: keep key values only for first 8 columns per collection
    for db_meta in result.get("databases", {}).values():
        for col_meta in db_meta.get("collections", {}).values():
            key_values = col_meta.get("possible_values", {})
            kept = {}
            for idx, field in enumerate(sorted(key_values.keys())):
                if idx >= 8:
                    break
                kept[field] = key_values[field]
            col_meta["possible_values"] = kept
    if _estimate_tokens(result) <= max_tokens:
        return result

    # Step 3: trim keyword index breadth
    index = result.get("keyword_linker", {})
    trimmed = {}
    for idx, token in enumerate(sorted(index.keys())):
        if idx >= 80:
            break
        trimmed[token] = index[token][:3]
    result["keyword_linker"] = trimmed
    if _estimate_tokens(result) <= max_tokens:
        return result

    # Step 4: if still too large, remove possible values, keep columns + linker
    if _estimate_tokens(result) > max_tokens:
        for db_meta in result.get("databases", {}).values():
            for col_meta in db_meta.get("collections", {}).values():
                col_meta["possible_values"] = {}
                col_meta.pop("row_sampled", None)

    if _estimate_tokens(result) <= max_tokens:
        return result

    # Step 5: keep only high-value linker collections for routing hints
    prioritized = {
        "authentication": {"authuser", "login_activities", "user_sessions", "audit_trail"},
        "numoni_customer": {"customerDetails", "pay_on_us_notifications", "wallet", "transaction_history"},
        "numoni_merchant": {"merchantDetails", "wallet", "transaction_history", "notifications"},
    }
    compact_databases = {}
    for db_name, db_meta in result.get("databases", {}).items():
        allow = prioritized.get(db_name, set())
        new_cols = {}
        for col_name, col_meta in db_meta.get("collections", {}).items():
            if col_name in allow:
                new_cols[col_name] = {
                    "columns": col_meta.get("columns", []),
                    "possible_values": col_meta.get("possible_values", {}),
                }
        compact_databases[db_name] = {"collections": new_cols}
    result["databases"] = compact_databases

    if _estimate_tokens(result) <= max_tokens:
        return result

    # Step 6: final safety - keep only columns in prioritized collections
    for db_meta in result.get("databases", {}).values():
        for col_meta in db_meta.get("collections", {}).values():
            col_meta["possible_values"] = {}
            key_cols = [c for c in col_meta.get("columns", []) if _is_key_column(c)]
            col_meta["columns"] = (key_cols or col_meta.get("columns", []))[:12]

    if _estimate_tokens(result) <= max_tokens:
        return result

    # Step 7: minimal linker-only structure
    minimal = {
        "databases": {},
        "keyword_linker": {k: v[:2] for i, (k, v) in enumerate(sorted(result.get("keyword_linker", {}).items())) if i < 20},
    }
    for db_name, db_meta in result.get("databases", {}).items():
        col_map = {}
        for col_name, col_meta in db_meta.get("collections", {}).items():
            col_map[col_name] = {"columns": col_meta.get("columns", [])[:8]}
        minimal["databases"][db_name] = {"collections": col_map}
    return minimal


def build_summary(databases_dir: Path, max_tokens: int) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": str(databases_dir).replace("\\", "/"),
        "note": "Compact linker metadata for collection selection",
        "databases": {},
    }

    db_dirs = [d for d in sorted(databases_dir.iterdir()) if d.is_dir()]
    for db_dir in db_dirs:
        db_name = db_dir.name
        db_meta = {"collections": {}}

        for collection_file in sorted(db_dir.glob("*.json")):
            collection_name = collection_file.stem
            rows = _load_collection_rows(collection_file, DEFAULT_COLLECTION_ROW_SCAN)
            columns, possible_values = _collect_schema_and_samples(rows, DEFAULT_COLUMN_SAMPLE_VALUES)

            db_meta["collections"][collection_name] = {
                "columns": columns,
                "possible_values": possible_values,
                "row_sampled": len(rows),
            }

        summary["databases"][db_name] = db_meta

    summary["keyword_linker"] = _build_keyword_index(summary)

    # Optional helper for fast route checks
    summary["query_examples"] = {
        "customers_notifications_login_no_wallet": {
            "query_like": "Which customers have notifications and login activity but no wallet record?",
            "likely_collections": [
                {"database": "numoni_customer", "collection": "customerDetails"},
                {"database": "numoni_customer", "collection": "pay_on_us_notifications"},
                {"database": "authentication", "collection": "login_activities"},
                {"database": "numoni_customer", "collection": "wallet"},
            ],
        },
        "shared_device_sessions": {
            "query_like": "Which merchants and customers share the same device in user_sessions?",
            "likely_collections": [
                {"database": "authentication", "collection": "user_sessions"},
                {"database": "numoni_customer", "collection": "customerDetails"},
                {"database": "numoni_merchant", "collection": "merchantDetails"},
            ],
        },
        "users_both_customer_merchant": {
            "query_like": "Which users appear in both customerDetails and merchantDetails?",
            "likely_collections": [
                {"database": "numoni_customer", "collection": "customerDetails"},
                {"database": "numoni_merchant", "collection": "merchantDetails"},
            ],
        },
    }

    compact = _shrink_to_token_budget(summary, max_tokens=max_tokens)
    if _estimate_tokens(compact) > max_tokens:
        compact.pop("query_examples", None)
        compact["keyword_linker"] = {k: v for i, (k, v) in enumerate(sorted(compact.get("keyword_linker", {}).items())) if i < 25}
    compact["estimated_tokens"] = _estimate_tokens(compact)
    compact["max_tokens_target"] = max_tokens
    return compact


def main() -> None:
    script_path = Path(__file__).resolve()
    part4_root = script_path.parents[1]
    numoni_root = part4_root.parents[0]
    default_databases = numoni_root / "databases"
    default_output = script_path.with_name("summarised_metadata.json")

    parser = argparse.ArgumentParser(description="Generate compact cross-DB metadata linker JSON.")
    parser.add_argument("--databases-dir", type=Path, default=default_databases)
    parser.add_argument("--output", type=Path, default=default_output)
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    args = parser.parse_args()

    if not args.databases_dir.exists():
        raise FileNotFoundError(f"Databases folder not found: {args.databases_dir}")

    summary = build_summary(args.databases_dir, args.max_tokens)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Created: {args.output}")
    print(f"Estimated tokens: {summary.get('estimated_tokens')}")


if __name__ == "__main__":
    main()
