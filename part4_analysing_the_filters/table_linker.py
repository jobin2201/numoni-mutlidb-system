#!/usr/bin/env python
"""Table linker for same-database field enrichment (lightweight join behavior)."""
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


BASE_DIR = Path(__file__).parent.parent
DATABASES_PATH = BASE_DIR / "databases"


JOIN_KEY_PRIORITY = [
    "customerId", "sentCustomerId", "receiveCustomerId", "userId", "sentUserId", "receiveUserId", "merchantId", "phoneNumber", "email",
    "accountNumber", "sessionId", "deviceId", "reference", "_id", "id"
]

JOIN_KEY_EQUIVALENTS = [
    ("customerId", "_id"),
    ("sentCustomerId", "customerId"),
    ("receiveCustomerId", "customerId"),
    ("sentCustomerId", "_id"),
    ("receiveCustomerId", "_id"),
    ("merchantId", "_id"),
    ("userId", "_id"),
    ("sentUserId", "userId"),
    ("receiveUserId", "userId"),
    ("sentUserId", "_id"),
    ("receiveUserId", "_id"),
    ("customerId", "customerUserId"),
]


REQUESTED_FIELD_ALIASES = {
    "customer id": ["customerId", "userId", "_id"],
    "customer name": ["name", "customerName", "accountName"],
    "name": ["name", "customerName", "accountName"],
    "sender name": ["name", "customerName", "accountName", "senderName"],
    "receiver name": ["name", "customerName", "accountName", "receiverName", "recipientName"],
    "sent customer name": ["name", "customerName", "accountName", "senderName"],
    "received customer name": ["name", "customerName", "accountName", "receiverName", "recipientName"],
    "total amount": ["totalAmount", "totalAmountPaid", "transactionAmount", "amount"],
    "amount": ["amount", "transactionAmount", "totalAmount", "totalAmountPaid"],
        "merchant id": ["merchantId", "merchantUserId", "userId", "id"],
        "pos id": ["posId", "id"],
        "pos name": ["posName", "name"],
        "commission deducted": ["merchantFee", "commission", "fee", "charges", "transactionFee"],
        "payout status": ["payoutStatus", "status"],
        "transaction reference id": ["transactionReferenceId", "reference", "sourceTransactionId"],
    }


SALES_FAST_COLLECTION_HINTS = {
    "customer": ["customerDetails", "customer_wallet_ledger", "transaction_history", "customer_load_money"],
    "merchant": ["merchantDetails", "transaction_history", "merchant_wallet_ledger"],
    "pos": ["pos", "transaction_history", "merchant_wallet_ledger"],
    "order": ["transaction_history", "merchant_wallet_ledger", "deals"],
    "transaction": ["transaction_history", "merchant_wallet_ledger"],
    "reference": ["transaction_history", "merchant_wallet_ledger", "customer_load_money"],
    "date": ["transaction_history", "merchant_wallet_ledger", "customer_load_money", "customerError"],
}


def _to_key(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_requested_field(field_name: str) -> str:
    return " ".join(field_name.lower().strip().split())


def _candidate_target_fields(requested_field: str, candidate_keys: List[str]) -> List[str]:
    requested_norm = _normalize_requested_field(requested_field)
    aliases = REQUESTED_FIELD_ALIASES.get(requested_norm, [])

    if requested_norm == "customer name":
        strict = []
        for key in candidate_keys:
            if key.lower() in ["name", "customername", "accountname"]:
                strict.append(key)
        return strict

    preferred: List[str] = []
    for alias in aliases:
        for key in candidate_keys:
            if key.lower() == alias.lower() and key not in preferred:
                preferred.append(key)

    if preferred:
        return preferred

    token_hits: List[str] = []
    requested_tokens = requested_norm.split()
    requires_name_field = "name" in requested_tokens
    requires_id_field = "id" in requested_tokens and "name" not in requested_tokens

    for key in candidate_keys:
        key_low = key.lower()
        if requires_name_field and "name" not in key_low:
            continue
        if requires_id_field and "id" not in key_low:
            continue
        if any(token in key_low for token in requested_tokens):
            token_hits.append(key)

    return token_hits


def _field_relevance_score(requested_field: str, target_field: str) -> int:
    requested_norm = _normalize_requested_field(requested_field)
    target_low = target_field.lower()
    aliases = [alias.lower() for alias in REQUESTED_FIELD_ALIASES.get(requested_norm, [])]

    if target_low in aliases:
        return 100
    if "name" in requested_norm and "name" in target_low:
        return 60
    if "id" in requested_norm and "id" in target_low:
        return 50

    score = 0
    for token in requested_norm.split():
        if token in target_low:
            score += 10
    return score


def _collection_preference_score(requested_field: str, collection_name: str) -> int:
    requested_norm = _normalize_requested_field(requested_field)
    collection_low = collection_name.lower()

    if requested_norm == "customer name":
        if collection_low == "customerdetails":
            return 80
        if "customer" in collection_low:
            return 20
    if requested_norm in {"sender name", "receiver name", "sent customer name", "received customer name"}:
        if collection_low == "customerdetails":
            return 60
        if "customer" in collection_low:
            return 20
    return 0


def _join_key_priority_bonus(base_join_key: str, other_join_key: str) -> int:
    base_low = base_join_key.lower()
    other_low = other_join_key.lower()

    customer_keys = {"customerid", "sentcustomerid", "receivecustomerid"}
    user_keys = {"userid", "sentuserid", "receiveuserid"}

    if base_low in customer_keys and (other_low in customer_keys or other_low == "_id"):
        return 120
    if base_low in user_keys and (other_low in user_keys or other_low == "_id"):
        return 80
    if base_low == "merchantid" and other_low in {"merchantid", "_id"}:
        return 60
    return 0


def _find_common_join_keys(base_keys: List[str], other_keys: List[str]) -> List[Tuple[str, str]]:
    common: List[Tuple[str, str]] = []
    base_map = {k.lower(): k for k in base_keys}
    other_map = {k.lower(): k for k in other_keys}

    for key in JOIN_KEY_PRIORITY:
        k_low = key.lower()
        if k_low in base_map and k_low in other_map:
            common.append((base_map[k_low], other_map[k_low]))

    for left_key, right_key in JOIN_KEY_EQUIVALENTS:
        left_low = left_key.lower()
        right_low = right_key.lower()
        if left_low in base_map and right_low in other_map:
            pair = (base_map[left_low], other_map[right_low])
            if pair not in common:
                common.append(pair)
        if right_low in base_map and left_low in other_map:
            pair = (base_map[right_low], other_map[left_low])
            if pair not in common:
                common.append(pair)

    return common


def _join_strength(
    base_data: List[Dict],
    other_data: List[Dict],
    base_join_key: str,
    other_join_key: str,
    fast_mode: bool = False,
) -> int:
    base_values = set()
    base_limit = 250 if fast_mode else 500
    other_limit = 800 if fast_mode else 2000

    for row in base_data[:base_limit]:
        val = _to_key(row.get(base_join_key))
        if val:
            base_values.add(val)

    if not base_values:
        return 0

    score = 0
    for row in other_data[:other_limit]:
        val = _to_key(row.get(other_join_key))
        if val and val in base_values:
            score += 1
    return score


@lru_cache(maxsize=8)
def _load_database_collections_cached(database_name: str) -> Dict[str, List[Dict]]:
    result: Dict[str, List[Dict]] = {}
    db_path = DATABASES_PATH / database_name
    if not db_path.exists():
        return result

    for file_path in db_path.glob("*.json"):
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
            if isinstance(data, list) and data:
                result[file_path.stem] = data
        except Exception:
            continue

    return result


def _load_database_collections(database_name: str) -> Dict[str, List[Dict]]:
    cached = _load_database_collections_cached(database_name)
    return {collection_name: rows for collection_name, rows in cached.items()}


def _load_selected_collections(database_name: str, selected_collections: List[str]) -> Dict[str, List[Dict]]:
    result: Dict[str, List[Dict]] = {}
    db_path = DATABASES_PATH / database_name
    if not db_path.exists():
        return result

    for collection_name in selected_collections:
        file_path = db_path / f"{collection_name}.json"
        if not file_path.exists():
            continue
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
            if isinstance(data, list) and data:
                result[collection_name] = data
        except Exception:
            continue

    return result


def _pick_sales_fast_collections(unresolved_fields: List[str], base_collection: str, query_text: str = "") -> List[str]:
    selected = {base_collection}

    normalized_fields = [f.lower() for f in unresolved_fields]
    joined = " ".join(normalized_fields + [query_text.lower()])

    for token, collections in SALES_FAST_COLLECTION_HINTS.items():
        if token in joined:
            selected.update(collections)

    if "customer" in joined:
        selected.add("customerDetails")
    if "merchant" in joined:
        selected.add("merchantDetails")
    if "pos" in joined:
        selected.add("pos")
    if "reference" in joined or "transaction" in joined:
        selected.add("transaction_history")

    return list(selected)


def enrich_requested_fields_with_links(
    database_name: str,
    base_collection: str,
    base_data: List[Dict],
    requested_fields: List[str],
    direct_field_mapping: Dict[str, str],
    query_text: str = "",
    fast_mode: bool = False,
) -> Tuple[List[Dict], Dict[str, str], List[str], List[str]]:
    """
    Resolve missing requested fields by linking with collections in same DB.

    Returns:
        display_rows, final_field_mapping, unresolved_fields, join_notes
    """
    if not base_data:
        return [], direct_field_mapping, requested_fields, []

    unresolved = [field for field in requested_fields if field not in direct_field_mapping]
    if not unresolved:
        display_rows = []
        for row in base_data:
            out = {}
            for display_name, actual_field in direct_field_mapping.items():
                out[display_name] = row.get(actual_field)
            display_rows.append(out)
        return display_rows, direct_field_mapping, [], []

    if fast_mode:
        selected = _pick_sales_fast_collections(unresolved, base_collection, query_text)
        all_collections = _load_selected_collections(database_name, selected)
    else:
        all_collections = _load_database_collections(database_name)
    all_collections.pop(base_collection, None)

    base_keys = list(base_data[0].keys())
    final_mapping = dict(direct_field_mapping)
    join_plans: Dict[str, Tuple[str, str, str]] = {}
    join_notes: List[str] = []
    strength_cache: Dict[Tuple[str, str, str], int] = {}

    for requested in unresolved:
        best_plan: Optional[Tuple[str, str, str, str, int]] = None

        for other_collection, other_data in all_collections.items():
            if not other_data or not isinstance(other_data[0], dict):
                continue

            other_keys = list(other_data[0].keys())
            target_fields = _candidate_target_fields(requested, other_keys)
            if not target_fields:
                continue

            join_key_pairs = _find_common_join_keys(base_keys, other_keys)
            if not join_key_pairs:
                continue

            for target_field in target_fields:
                for base_join_key, other_join_key in join_key_pairs:
                    cache_key = (other_collection, base_join_key, other_join_key)
                    if cache_key not in strength_cache:
                        strength_cache[cache_key] = _join_strength(
                            base_data,
                            other_data,
                            base_join_key,
                            other_join_key,
                            fast_mode=fast_mode,
                        )
                    strength = strength_cache[cache_key]
                    if strength <= 0:
                        continue
                    relevance = _field_relevance_score(requested, target_field)
                    collection_bias = _collection_preference_score(requested, other_collection)
                    join_bonus = _join_key_priority_bonus(base_join_key, other_join_key)
                    total_score = (strength * 10) + relevance + collection_bias + join_bonus
                    current = (other_collection, target_field, base_join_key, other_join_key, total_score)
                    if best_plan is None or current[4] > best_plan[4]:
                        best_plan = current

        if best_plan:
            selected_collection, selected_target_field, base_join_key, other_join_key, _ = best_plan
            other_data = all_collections[selected_collection]
            join_plans[requested] = (selected_collection, base_join_key, other_join_key)
            final_mapping[requested] = f"{selected_collection}.{selected_target_field}"
            join_notes.append(
                f"{requested} ← {selected_collection}.{selected_target_field} (join: {base_join_key} = {other_join_key})"
            )

    display_rows: List[Dict] = []
    join_indexes: Dict[str, Dict[str, Dict]] = {}

    for requested_field, (collection_name, _, other_join_key) in join_plans.items():
        other_data = all_collections.get(collection_name, [])
        index: Dict[str, Dict] = {}
        for item in other_data:
            join_value = _to_key(item.get(other_join_key))
            if join_value and join_value not in index:
                index[join_value] = item
        join_indexes[requested_field] = index

    for base_row in base_data:
        out: Dict[str, Any] = {}
        for display_name in requested_fields:
            mapped = final_mapping.get(display_name)
            if not mapped:
                out[display_name] = None
                continue

            if "." not in mapped:
                out[display_name] = base_row.get(mapped)
                continue

            collection_name, target_field = mapped.split(".", 1)
            _, base_join_key, _ = join_plans.get(display_name, (collection_name, "", ""))
            join_value = _to_key(base_row.get(base_join_key)) if base_join_key else None
            matched = join_indexes.get(display_name, {}).get(join_value) if join_value else None
            out[display_name] = matched.get(target_field) if matched else None

        display_rows.append(out)

    unresolved_after = [field for field in requested_fields if field not in final_mapping]
    return display_rows, final_mapping, unresolved_after, join_notes
