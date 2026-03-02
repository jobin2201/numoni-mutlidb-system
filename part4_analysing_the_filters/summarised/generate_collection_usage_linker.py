#!/usr/bin/env python
"""Generate collection usage linker from Part-2 metadata files.

Output format:
{
  "collection_name": [
    "one line explanation",
    ["keyword1", "keyword2", ...]
  ],
  ...
}

Reads:
- part2_analysing_the_collection/authentication_collections_metadata.json
- part2_analysing_the_collection/numoni_customer_collections_metadata.json
- part2_analysing_the_collection/numoni_merchant_collections_metadata.json
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

MAX_TOKENS = 2500


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _split_name_tokens(text: str) -> List[str]:
    if not text:
        return []
    spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    spaced = spaced.replace("_", " ")
    tokens = [t.lower() for t in re.split(r"\W+", spaced) if t]
    return [t for t in tokens if len(t) >= 2]


def _field_keywords(fields: List[str], limit: int = 12) -> List[str]:
    blocked = {
        "id", "_id", "class", "createddt", "updateddt", "createdat", "updatedat",
        "date", "time", "created", "updated",
    }
    ordered: List[str] = []
    for f in fields:
        for token in _split_name_tokens(f):
            compact = token.replace("_", "")
            if compact in blocked:
                continue
            if token not in ordered:
                ordered.append(token)
            if len(ordered) >= limit:
                return ordered
    return ordered


def _sample_keywords(sample_values: Dict[str, Any], limit: int = 6) -> List[str]:
    out: List[str] = []
    for field, vals in (sample_values or {}).items():
        if not isinstance(vals, list):
            continue
        for v in vals[:4]:
            text = _safe_text(v)
            if not text:
                continue
            up = text.upper()
            if up in {"ACTIVE", "INACTIVE", "SUCCESSFUL", "FAILED", "PENDING", "COMPLETED", "LOGIN", "MERCHANT", "CUSTOMER"}:
                low = text.lower()
                if low not in out:
                    out.append(low)
            if len(out) >= limit:
                return out
    return out


def _db_collection_purpose(db_name: str, collection: str) -> str:
    c = collection.lower()
    if c == "transaction_history":
        if db_name == "numoni_customer":
            return "customer transactions, spends, credits/debits, and customer payment history"
        if db_name == "numoni_merchant":
            return "merchant sales, revenue, POS/business transactions, and merchant payment history"
    if c == "wallet":
        if db_name == "numoni_customer":
            return "customer wallet balances and customer wallet state"
        if db_name == "numoni_merchant":
            return "merchant wallet balances and merchant wallet state"
    if c == "tokens":
        if db_name == "numoni_customer":
            return "customer-service token records"
        if db_name == "numoni_merchant":
            return "merchant-service token records"
    if c == "shedlock":
        if db_name == "numoni_customer":
            return "customer-service scheduler lock records"
        if db_name == "numoni_merchant":
            return "merchant-service scheduler lock records"
    if db_name == "authentication":
        return f"authentication/identity activity for {collection}"
    if db_name == "numoni_customer":
        return f"customer-domain data for {collection}"
    if db_name == "numoni_merchant":
        return f"merchant-domain data for {collection}"
    return f"data for {collection}"


def _collection_one_liner(collection: str, dbs: Set[str], fields: List[str]) -> str:
    name_phrase = " ".join(_split_name_tokens(collection)) or collection
    if not dbs:
        return f"Use `{collection}` for {name_phrase} data."

    if len(dbs) == 1:
        db_name = sorted(dbs)[0]
        useful_fields = [f for f in fields if f not in {"_id", "_class"}]
        if useful_fields:
            field_preview = ", ".join(useful_fields[:3])
            return f"Use `{collection}` in {db_name} for {_db_collection_purpose(db_name, collection)} (key fields: {field_preview})."
        return f"Use `{collection}` in {db_name} for {_db_collection_purpose(db_name, collection)}."

    parts = []
    for db_name in sorted(dbs):
        parts.append(f"in {db_name} for {_db_collection_purpose(db_name, collection)}")
    return f"Use `{collection}` {', and '.join(parts)}."


def _load_metadata_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _estimate_tokens(obj: Any) -> int:
    text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return max(1, len(text) // 4)


def build_collection_usage_linker(part2_dir: Path) -> Dict[str, List[Any]]:
    metadata_files: List[Tuple[str, Path]] = [
        ("authentication", part2_dir / "authentication_collections_metadata.json"),
        ("numoni_customer", part2_dir / "numoni_customer_collections_metadata.json"),
        ("numoni_merchant", part2_dir / "numoni_merchant_collections_metadata.json"),
    ]

    combined: Dict[str, Dict[str, Any]] = {}

    for db_name, file_path in metadata_files:
        meta = _load_metadata_file(file_path)
        for collection_name, info in meta.items():
            if not isinstance(info, dict):
                continue

            entry = combined.setdefault(collection_name, {
                "dbs": set(),
                "fields": set(),
                "sample_values": {},
                "fields_by_db": {},
                "total_records": 0,
            })

            entry["dbs"].add(db_name)
            entry["fields_by_db"].setdefault(db_name, set())
            for f in info.get("fields", []) or []:
                entry["fields"].add(str(f))
                entry["fields_by_db"][db_name].add(str(f))

            samples = info.get("sample_values", {}) or {}
            if isinstance(samples, dict):
                for k, vals in samples.items():
                    if k not in entry["sample_values"] and isinstance(vals, list):
                        entry["sample_values"][k] = vals

            tr = info.get("total_records")
            if isinstance(tr, int):
                entry["total_records"] = max(entry["total_records"], tr)

    out: Dict[str, List[Any]] = {}
    priority_frontload = {
        "login_activities": ["login_activities", "login", "activity", "login activity", "record", "records", "users", "authentication"],
        "transaction_history": ["transaction", "history", "customer", "merchant", "either", "both"],
        "customer_wallet_ledger": ["customer", "wallet", "ledger", "credits", "debits", "movement"],
        "wallet": ["wallet", "balance", "customer", "merchant", "entries"],
        "invoice": ["invoice", "invoices", "customer", "billing"],
        "favourite_deal": ["favourite", "deal", "customer", "saved"],
        "customerlocation": ["customer", "location", "multiple", "city"],
        "pay_on_us_notifications": ["customer", "notifications", "notification", "alerts"],
        "merchantDetails": ["merchant", "details", "profile", "business"],
        "merchant_payout": ["merchant", "payout", "amount", "records"],
        "merchant_wallet_ledger": ["merchant", "wallet", "ledger", "debit", "credit"],
        "deals": ["deals", "deal", "offers", "merchant", "merchants", "promo"],
        "businessimage": ["business", "image", "merchant"],
        "merchantlocation": ["merchant", "location", "city", "multiple"],
        "notifications": ["notifications", "notification", "alerts", "merchant", "merchants", "received"],
        "dealimage": ["deal", "image", "offer"],
        "authuser": ["authuser", "users", "user", "auth", "authentication", "active", "account", "identity"],
        "user_sessions": ["user_sessions", "sessions", "session", "active sessions", "active", "users", "device"],
        "audit_trail": ["audit", "trail", "entries", "login"],
        "roles": ["roles", "role", "assigned", "auth"],
        "signin_records": ["signin_records", "signin", "sign in", "records", "login", "activity", "auth", "users"],
        "customerDetails": ["customer", "details", "profile", "users"],
    }

    for collection in sorted(combined.keys(), key=lambda x: x.lower()):
        info = combined[collection]
        dbs: Set[str] = info["dbs"]
        fields = sorted(info["fields"])
        sample_values = info["sample_values"]
        fields_by_db = info.get("fields_by_db", {})

        keywords: List[str] = []

        for t in priority_frontload.get(collection, []):
            tt = _safe_text(t).lower()
            if tt and tt not in keywords:
                keywords.append(tt)

        for t in _split_name_tokens(collection):
            if t not in keywords:
                keywords.append(t)

        for t in _field_keywords(fields):
            if t not in keywords:
                keywords.append(t)

        for t in _sample_keywords(sample_values):
            if t not in keywords:
                keywords.append(t)

        # compact aliases for common intent matching
        aliases = {
            "merchantdetails": ["merchant", "business", "store", "vendor"],
            "customerdetails": ["customer", "buyer", "client", "profile"],
            "authuser": ["authuser", "auth user", "users", "authentication", "auth", "user account"],
            "transaction_history": ["transaction", "payment", "revenue"],
            "login_activities": ["login", "login activity", "record", "signin", "sign in", "digital activity"],
            "user_sessions": ["sessions", "session", "active sessions", "active session", "device"],
            "wallet": ["wallet", "balance"],
            "audit_trail": ["audit", "logs", "activity trail"],
            "notifications": ["notification", "alert"],
            "pay_on_us_notifications": ["notification", "pay on us", "alert"],
        }
        for t in aliases.get(collection.lower(), []):
            for piece in _split_name_tokens(t):
                if piece not in keywords:
                    keywords.append(piece)

        keywords = keywords[:18]
        # DB disambiguation keywords for same table names across DBs
        if len(dbs) > 1:
            for db_name in sorted(dbs):
                db_hint = db_name.replace("numoni_", "")
                if db_hint not in keywords:
                    keywords.append(db_hint)
                pair_token = f"{collection}_{db_hint}".lower()
                if pair_token not in keywords:
                    keywords.append(pair_token)
                for f in sorted(fields_by_db.get(db_name, set()))[:6]:
                    for t in _split_name_tokens(f):
                        if t not in keywords:
                            keywords.append(t)

        # Query-intent enrichments from your cross-db patterns
        intent_boost = {
            "merchantdetails": ["merchant", "not in auth", "without auth", "merchant profile"],
            "customerdetails": ["customer", "not in auth", "customer profile", "exists in both"],
            "authuser": ["auth", "authentication", "active auth", "zero presence", "not exist"],
            "signin_records": ["signin_records", "signin", "sign in", "signin records", "login records", "auth", "users", "without authuser", "not in authuser"],
            "transaction_history": ["transactions", "no transaction", "customer transaction", "merchant transaction"],
            "wallet": ["wallet", "wallet balance", "wallet entries", "no wallet record"],
            "login_activities": ["login activity", "digitally active", "no login"],
            "user_sessions": ["user sessions", "same device", "shared device", "no session"],
            "audit_trail": ["audit trail", "audit entries", "no audit"],
            "merchant_payout": ["payout", "payout records", "merchant payout"],
            "pay_on_us_notifications": ["notifications", "customer notifications", "alerts"],
            "customer_wallet_ledger": ["ledger", "credits", "debits", "ledger movement"],
            "invoice": ["invoice", "invoices", "billing"],
            "favourite_deal": ["favourite deal", "saved deal", "bookmarked deal"],
            "customerlocation": ["location", "multiple locations", "city"],
            "merchant_wallet_ledger": ["merchant ledger", "debit entries", "credit entries"],
            "businessimage": ["business image", "merchant image"],
            "merchantlocation": ["merchant location", "store city", "multiple cities"],
            "dealimage": ["deal image", "offer image"],
            "deals": ["deals", "offers", "promotions"],
            "notifications": ["notifications", "notification", "received notifications", "received", "alerts"],
            "deals": ["deals", "deal", "offers", "promotions", "no deals"],
        }
        for phrase in intent_boost.get(collection.lower(), []):
            for t in _split_name_tokens(phrase):
                if t not in keywords:
                    keywords.append(t)

        keywords = keywords[:26]
        one_liner = _collection_one_liner(collection, dbs, fields)
        out[collection] = [one_liner, keywords]

        # Extra explicit entries for same table names across DBs
        if len(dbs) > 1:
            for db_name in sorted(dbs):
                scoped_key = f"{collection}__{db_name}"
                scoped_fields = sorted(fields_by_db.get(db_name, set()))
                scoped_keywords = []
                scoped_keywords.extend(_split_name_tokens(collection))
                scoped_keywords.extend(_split_name_tokens(db_name.replace("numoni_", "")))
                scoped_keywords.extend(_field_keywords(scoped_fields, limit=10))
                dedup = []
                for k in scoped_keywords:
                    if k not in dedup:
                        dedup.append(k)
                out[scoped_key] = [
                    f"Use `{collection}` in {db_name} specifically for {_db_collection_purpose(db_name, collection)}.",
                    dedup[:18],
                ]

    return out


def compact_usage_linker(data: Dict[str, List[Any]], token_budget: int) -> Dict[str, List[Any]]:
    # Keep one-liners unchanged; shrink keywords to important minimal set.
    def slim_keywords(entry: List[Any]) -> List[Any]:
        one_liner = entry[0] if isinstance(entry, list) and entry else ""
        kws = entry[1] if isinstance(entry, list) and len(entry) > 1 and isinstance(entry[1], list) else []
        filtered = []
        important_kw = {
            "merchant", "customer", "auth", "authentication", "transaction", "wallet", "login", "session",
            "audit", "payout", "notification", "device", "details", "history", "balance", "user",
            "role", "roles", "signin", "sign", "record", "records", "active", "presence", "both", "either",
            "authuser", "signin_records", "without", "missing", "not",
            "sessions", "users", "deals", "notifications", "received", "no",
        }
        for k in kws:
            kk = str(k).lower()
            if kk in important_kw and kk not in filtered:
                filtered.append(kk)
        if not filtered:
            # fallback: keep up to 2 raw keywords
            filtered = [str(k).lower() for k in kws[:2]]
        return [one_liner, filtered[:3]]

    slim = {k: slim_keywords(v) for k, v in data.items()}
    if _estimate_tokens(slim) <= token_budget:
        return slim

    # Always include all base collection keys (no '__').
    base_keys = sorted([k for k in slim.keys() if "__" not in k], key=lambda x: x.lower())
    compact: Dict[str, List[Any]] = {k: [slim[k][0], list(slim[k][1])] for k in base_keys}

    high_priority = {
        "authuser",
        "login_activities",
        "user_sessions",
        "audit_trail",
        "roles",
        "signin_records",
        "customerDetails",
        "customer_wallet_ledger",
        "wallet",
        "invoice",
        "transaction_history",
        "favourite_deal",
        "deals",
        "customerlocation",
        "pay_on_us_notifications",
        "merchantDetails",
        "merchant_payout",
        "merchant_wallet_ledger",
        "businessimage",
        "merchantlocation",
        "notifications",
        "dealimage",
    }

    # Reduce keyword lengths globally while preserving all base tables.
    for non_priority_limit in [1, 0]:
        test = {}
        for k, v in compact.items():
            if k in high_priority:
                test[k] = [v[0], v[1][:3]]
            else:
                test[k] = [v[0], v[1][:non_priority_limit]]
        if _estimate_tokens(test) <= token_budget:
            compact = test
            break

    # If still high, reduce priority keywords too, but keep at least one.
    if _estimate_tokens(compact) > token_budget:
        for priority_limit in [2, 1]:
            test = {}
            for k, v in compact.items():
                if k in high_priority:
                    test[k] = [v[0], v[1][:priority_limit]]
                else:
                    test[k] = [v[0], []]
            if _estimate_tokens(test) <= token_budget:
                compact = test
                break

    # Add disambiguation scoped keys only if budget allows.
    scoped_priority = [
        "transaction_history__numoni_customer",
        "transaction_history__numoni_merchant",
        "wallet__numoni_customer",
        "wallet__numoni_merchant",
        "tokens__numoni_customer",
        "tokens__numoni_merchant",
        "shedLock__numoni_customer",
        "shedLock__numoni_merchant",
    ]

    for scoped_key in scoped_priority:
        if scoped_key not in slim or scoped_key in compact:
            continue
        compact[scoped_key] = [slim[scoped_key][0], slim[scoped_key][1][:1]]
        if _estimate_tokens(compact) > token_budget:
            compact.pop(scoped_key, None)

    return compact


def main() -> None:
    script_path = Path(__file__).resolve()
    part4_root = script_path.parents[1]
    numoni_root = part4_root.parent
    part2_dir = numoni_root / "part2_analysing_the_collection"

    output_path = script_path.with_name("collection_usage_linker.json")
    data = build_collection_usage_linker(part2_dir)
    compact_data = compact_usage_linker(data, MAX_TOKENS)
    output_path.write_text(json.dumps(compact_data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")

    print(f"Created: {output_path}")
    print(f"Collections mapped: {len(compact_data)}")
    print(f"Estimated tokens: {_estimate_tokens(compact_data)}")


if __name__ == "__main__":
    main()
