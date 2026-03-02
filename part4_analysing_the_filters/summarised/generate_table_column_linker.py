#!/usr/bin/env python
"""Generate a flat table->columns linker JSON across all 3 DBs.

Output format:
{
  "table_name_1": ["col1", "col2", ...],
  "table_name_2": ["col1", "col2", ...]
}

- No database nesting.
- If a table name appears in multiple DBs, columns are merged (union).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Set

ROW_SCAN_LIMIT = 400
TOKEN_BUDGET = 1000


def _estimate_tokens(obj: Any) -> int:
    text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return max(1, len(text) // 4)


def _column_priority(col_name: str) -> int:
    col = col_name.lower()
    ordered_keys = [
        "_id", "id", "userid", "customerid", "merchantid", "status", "type", "name",
        "email", "phone", "amount", "balance", "date", "time", "transaction", "session", "device",
    ]
    for idx, key in enumerate(ordered_keys):
        if key in col:
            return idx
    return 999


def _load_rows(file_path: Path) -> List[Dict[str, Any]]:
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    if isinstance(payload, list):
        return [r for r in payload[:ROW_SCAN_LIMIT] if isinstance(r, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return [r for r in payload["data"][:ROW_SCAN_LIMIT] if isinstance(r, dict)]
        return [payload]
    return []


def build_table_column_linker(databases_root: Path) -> Dict[str, List[str]]:
    linker: Dict[str, Set[str]] = {}

    for db_dir in sorted([d for d in databases_root.iterdir() if d.is_dir()]):
        for json_file in sorted(db_dir.glob("*.json")):
            table_name = json_file.stem
            linker.setdefault(table_name, set())

            rows = _load_rows(json_file)
            for row in rows:
                for col_name in row.keys():
                    linker[table_name].add(str(col_name))

    return {
        table: sorted(cols, key=lambda c: (_column_priority(c), c.lower()))
        for table, cols in sorted(linker.items(), key=lambda x: x[0].lower())
    }


def _compact_linker_to_budget(full_linker: Dict[str, List[str]], token_budget: int) -> Dict[str, List[str]]:
    # Keep all table names; shrink column list length globally until budget fits.
    tables = sorted(full_linker.keys(), key=lambda t: t.lower())

    for max_cols in [4, 3, 2, 1, 0]:
        compact = {table: full_linker.get(table, [])[:max_cols] for table in tables}
        if _estimate_tokens(compact) <= token_budget:
            return compact

    return {table: [] for table in tables}


def _format_inline_arrays(linker: Dict[str, List[str]]) -> str:
    lines = ["{"]
    items = sorted(linker.items(), key=lambda kv: kv[0].lower())
    for index, (table, cols) in enumerate(items):
        key_txt = json.dumps(table, ensure_ascii=False)
        cols_txt = "[" + ", ".join(json.dumps(col, ensure_ascii=False) for col in cols) + "]"
        suffix = "," if index < len(items) - 1 else ""
        lines.append(f"  {key_txt}: {cols_txt}{suffix}")
    lines.append("}")
    return "\n".join(lines) + "\n"


def main() -> None:
    script_path = Path(__file__).resolve()
    part4_root = script_path.parents[1]
    numoni_root = part4_root.parent
    databases_root = numoni_root / "databases"

    if not databases_root.exists():
        raise FileNotFoundError(f"Databases folder not found: {databases_root}")

    output_path = script_path.with_name("table_column_linker.json")
    full_linker = build_table_column_linker(databases_root)
    linker = _compact_linker_to_budget(full_linker, TOKEN_BUDGET)
    output_path.write_text(_format_inline_arrays(linker), encoding="utf-8")

    print(f"Created: {output_path}")
    print(f"Total tables: {len(linker)}")
    print(f"Estimated tokens: {_estimate_tokens(linker)}")


if __name__ == "__main__":
    main()
