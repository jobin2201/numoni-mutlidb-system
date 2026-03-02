import json
import re
import csv
import io
from pathlib import Path
from functools import lru_cache
from typing import Any, Callable, Dict, List, Tuple


STOP_KEYWORDS = {
    "the", "a", "an", "of", "for", "to", "in", "on", "at", "by", "with", "and", "or", "from",
    "compare", "comparison", "vs", "versus", "between", "number", "count", "counts", "total",
    "show", "list", "get", "me", "all", "per", "each", "please",
}

AGG_AVG_WORDS = {"average", "avg", "mean"}
AGG_SUM_WORDS = {"total", "sum", "amount", "value", "balance", "payout", "volume"}
AGG_COUNT_WORDS = {"count", "number", "frequency", "times", "records", "transactions", "deals", "sessions", "logins"}

NUMERIC_FIELD_HINTS = [
    "amount", "balance", "value", "total", "payout", "wallet", "transaction", "points", "count", "frequency",
]

VIRTUAL_COLLECTION_METADATA: Dict[str, Dict[str, Any]] = {
    "businessimage": {
        "keywords": ["business image", "business images", "image", "logo", "photo"],
        "fields": ["businessImagePath", "merchantId", "image"],
        "description": "Merchant business images",
    }
}

COLLECTION_INTENT_HINTS: Dict[str, List[str]] = {
    "authuser": ["authentication", "auth", "register", "registration", "registered", "signup", "account", "email", "active account"],
    "transaction_history": ["transaction", "transactions", "transacting", "payment", "payments", "volume", "activity"],
    "deals": ["deal", "deals", "offer", "offers", "promotion", "discount"],
    "wallet": ["wallet", "balance", "negative wallet"],
    "merchant_payout": ["payout", "payouts", "settlement", "withdrawal", "pending payout", "payout delay", "immediate payout"],
    "invoice": ["invoice", "invoices", "bill", "receipt"],
    "favourite_deal": ["favourite", "favorite", "favourited", "liked", "saved", "bookmarked"],
    "user_sessions": ["session", "sessions", "without sessions"],
    "login_activities": ["login", "logins", "signin", "sign in", "failed login", "successful login"],
    "roles": ["role", "roles", "multiple roles", "single role", "permission"],
    "refreshtoken": ["refresh token", "refresh", "token", "active refresh token"],
    "account_deletion_request": ["account deletion", "delete account", "deletion request", "deactivate"],
    "merchantlocation": ["city", "cities", "location", "state", "region"],
    "otp": ["otp", "one time password", "verification code", "otp requests"],
    "merchantDetails": ["merchant", "merchants", "business", "registered email", "business image", "business images"],
    "customerDetails": ["customer", "customers", "active customer"],
    "businessimage": ["business image", "business images", "image", "images"],
}

ERROR_CONTEXT_HINTS = {"error", "errors", "failed", "failure", "mismatch", "inconsistency", "inconsistencies", "issue", "issues"}

COMPARE_MARKERS = [" compare ", " versus ", " vs ", " difference "]
TIME_COMPARE_MARKERS = ["last year", "this year", "last month", "this month", "last week", "this week"]
COMPARE_CONNECTOR_RE = re.compile(r"\b(vs|versus|against|and|or|before|after)\b", re.IGNORECASE)
CLAUSE_BOUNDARY_MARKERS = [
    " for ", " per ", " by ", " across ", " among ", " in ", " where ", " from ",
]


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _split_identifier(text: str) -> List[str]:
    if not text:
        return []
    spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    spaced = spaced.replace("_", " ")
    return [token.lower() for token in re.split(r"\W+", spaced) if token]


def _tokenize_query(query: str) -> List[str]:
    text = re.sub(r"[^a-zA-Z0-9\s_]", " ", (query or "").lower())
    words = [word for word in text.split() if word and word not in STOP_KEYWORDS]
    words = [word for word in words if len(word) >= 2]
    bigrams = [f"{words[i]} {words[i+1]}" for i in range(len(words) - 1)]

    ordered: List[str] = []
    seen = set()
    for token in words + bigrams:
        if token not in seen:
            seen.add(token)
            ordered.append(token)
    return ordered


def _normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", _safe_text(text)).strip()


def _normalize_token(token: str) -> str:
    t = _safe_text(token).lower().strip()
    if len(t) > 4 and t.endswith("ies"):
        return t[:-3] + "y"
    if len(t) > 5 and t.endswith("ing"):
        return t[:-3]
    if len(t) > 4 and t.endswith("ed"):
        return t[:-2]
    if len(t) > 3 and t.endswith("es"):
        return t[:-2]
    if len(t) > 3 and t.endswith("s"):
        return t[:-1]
    return t


def _token_variants(tokens: List[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for token in tokens:
        for item in (token, _normalize_token(token)):
            clean = _safe_text(item).lower().strip()
            if clean and clean not in seen:
                seen.add(clean)
                ordered.append(clean)
    return ordered


@lru_cache(maxsize=1)
def load_collection_keywords() -> Dict[str, Any]:
    keywords_path = Path(__file__).resolve().parents[1] / "part2_analysing_the_collection" / "collection_keywords.json"
    if not keywords_path.exists():
        return {}
    try:
        data = json.loads(keywords_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _score_collection(collection_name: str, metadata: Dict[str, Any], query: str, tokens: List[str]) -> Tuple[int, List[str]]:
    score = 0
    hits: List[str] = []

    keyword_tokens = set()
    for keyword in metadata.get("keywords", []) or []:
        k = _safe_text(keyword).lower()
        if not k:
            continue
        keyword_tokens.add(k)
        keyword_tokens.update(_split_identifier(k))

    field_tokens = set()
    for field in metadata.get("fields", []) or []:
        field_tokens.update(_split_identifier(_safe_text(field)))

    name_tokens = set(_split_identifier(collection_name))
    query_text = f" {_safe_text(query).lower()} "

    for token in tokens:
        token_low = token.lower()
        token_hit = False

        if token_low in keyword_tokens:
            score += 6
            token_hit = True
        elif token_low in field_tokens:
            score += 4
            token_hit = True
        elif token_low in name_tokens:
            score += 5
            token_hit = True

        pattern = r"\b" + re.escape(token_low).replace(r"\ ", r"\s+") + r"\b"
        if re.search(pattern, query_text):
            if token_low in keyword_tokens:
                score += 4
                token_hit = True
            elif token_low in name_tokens or token_low in field_tokens:
                score += 2
                token_hit = True

        if token_hit and token_low not in hits:
            hits.append(token_low)

    return score, hits


def _infer_scope(query: str) -> str:
    q = _safe_text(query).lower()
    if "merchant" in q or "business" in q or "vendor" in q or "store" in q:
        return "merchant"
    if "customer" in q or "client" in q:
        return "customer"
    if any(token in q for token in ["deal", "payout", "merchantlocation"]):
        return "merchant"
    if any(token in q for token in ["invoice", "favourite", "favorite"]):
        return "customer"
    return "user"


def _enriched_metadata_map(metadata_map: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(metadata_map or {})
    for collection_name, meta in VIRTUAL_COLLECTION_METADATA.items():
        if collection_name not in out:
            out[collection_name] = meta
    return out


def _collection_intent_bonus(collection_name: str, text: str, terms: set) -> int:
    hints = COLLECTION_INTENT_HINTS.get(collection_name, [])
    if not hints:
        return 0

    text_low = _safe_text(text).lower()
    bonus = 0
    for hint in hints:
        h = _safe_text(hint).lower()
        if not h:
            continue
        if " " in h:
            if h in text_low:
                bonus += 12
            continue

        if h in terms:
            bonus += 7
    return bonus


def _scope_alignment_bonus(scope: str, collection_name: str, query_low: str) -> int:
    name = _safe_text(collection_name).lower()
    has_customer = "customer" in query_low or "client" in query_low
    has_merchant = "merchant" in query_low or "business" in query_low or "vendor" in query_low

    if scope == "merchant":
        if (name.startswith("customer") or name in {"customerdetails", "invoice", "favourite_deal"}) and not has_customer:
            return -10
        if name.startswith("merchant") or name in {"deals", "transaction_history", "wallet", "businessimage", "authuser"}:
            return 3
    elif scope == "customer":
        if (name.startswith("merchant") or name in {"deals", "merchantlocation", "businessimage"}) and not has_merchant:
            return -10
        if name.startswith("customer") or name in {"transaction_history", "wallet", "invoice", "favourite_deal", "authuser"}:
            return 3
    else:
        if name in {"authuser", "login_activities", "user_sessions", "roles", "refreshtoken", "account_deletion_request", "otp", "transaction_history"}:
            return 3

    return 0


def _count_compare_connectors(query: str) -> Dict[str, int]:
    text = _safe_text(query).lower()
    connectors = re.findall(COMPARE_CONNECTOR_RE, text)
    counts: Dict[str, int] = {}
    for conn in connectors:
        key = _safe_text(conn).lower()
        if key:
            counts[key] = counts.get(key, 0) + 1
    return counts


def _extract_compare_segment(query: str) -> str:
    q = f" {_normalize_spaces(query).lower()} "
    start = 0
    for marker in [" compare ", " comparison ", " difference between ", " between "]:
        pos = q.find(marker)
        if pos != -1:
            start = pos + len(marker)
            break

    segment = q[start:].strip()
    if not segment:
        return ""

    padded = f" {segment} "
    cut_positions = [padded.find(marker) for marker in CLAUSE_BOUNDARY_MARKERS]
    valid_positions = [pos for pos in cut_positions if pos > 0]
    if valid_positions:
        segment = padded[: min(valid_positions)].strip()

    segment = segment.replace("difference between", "vs")
    segment = segment.replace("before and after", "before vs after")
    segment = segment.replace("before/after", "before vs after")
    return segment


def _clean_metric_phrase(phrase: str) -> str:
    cleaned = _normalize_spaces(phrase.lower())
    cleaned = re.sub(r"^[^a-z0-9]+|[^a-z0-9]+$", "", cleaned)
    cleaned = re.sub(r"\b(total|number of|sum of|sum|value of)\b", "", cleaned)
    cleaned = re.sub(r"\b(each|every|all|per|for|of|the|a|an)\b", "", cleaned)
    cleaned = re.sub(r"\b(between|top|bottom|higher|lower|than|but|rarely|frequently)\b", "", cleaned)
    cleaned = re.sub(r"\b(merchant|merchants|customer|customers|user|users)\b", "", cleaned)
    cleaned = re.sub(r"\d+", "", cleaned)
    cleaned = _normalize_spaces(cleaned)
    return cleaned


def _extract_metric_phrases(query: str) -> List[str]:
    segment = _extract_compare_segment(query)
    if not segment:
        return []

    parts = re.split(r"\b(?:vs|versus|against|and|or|with|than|but)\b|,|/", segment, flags=re.IGNORECASE)
    phrases: List[str] = []
    for part in parts:
        cleaned = _clean_metric_phrase(part)
        if cleaned and cleaned not in phrases:
            phrases.append(cleaned)

    # Keep only meaningful chunks; fallback to original segment tokens if split was too aggressive.
    def _is_grouping_fragment(text: str) -> bool:
        t = _normalize_spaces(text.lower())
        if not t:
            return True
        if re.fullmatch(r"(top|bottom)\s*\d*", t):
            return True
        stripped = re.sub(
            r"\b(top|bottom|last|previous|this|current|month|week|year|quarter|merchant|merchants|customer|customers|user|users|between|and|active|actively|new|created)\b",
            "",
            t,
        )
        stripped = re.sub(r"\d+", "", stripped)
        stripped = _normalize_spaces(stripped)
        return len(stripped) < 3

    meaningful = [p for p in phrases if len(p) >= 3 and p not in {"before", "after"} and not _is_grouping_fragment(p)]
    if len(meaningful) >= 2:
        return meaningful

    compact = _clean_metric_phrase(segment)
    if compact:
        compact_parts = [
            _clean_metric_phrase(item)
            for item in re.split(r"\b(?:vs|versus|against|and|or|than|but)\b", compact, flags=re.IGNORECASE)
        ]
        compact_parts = [item for item in compact_parts if item and len(item) >= 3 and not _is_grouping_fragment(item)]
        if len(compact_parts) >= 2:
            return compact_parts

    return meaningful


def _match_metric_phrase_to_collection(
    phrase: str,
    metadata_map: Dict[str, Any],
) -> Tuple[str, int]:
    if not phrase:
        return "", 0

    tokens = _tokenize_query(phrase)
    phrase_terms = _token_variants(_split_identifier(phrase) + tokens)
    best_name = ""
    best_score = 0

    for collection_name, meta in metadata_map.items():
        if not isinstance(meta, dict):
            continue
        score, _ = _score_collection(collection_name, meta, phrase, tokens)
        collection_name_low = collection_name.lower()
        phrase_low = phrase.lower()
        collection_terms = _token_variants(_split_identifier(collection_name))
        name_overlap = len(set(phrase_terms).intersection(set(collection_terms)))
        score += name_overlap * 12

        if collection_name_low in phrase_low:
            score += 10

        for name_token in collection_terms:
            if name_token and re.search(rf"\b{re.escape(name_token)}\b", phrase_low):
                score += 4

        score += _collection_intent_bonus(collection_name, phrase, set(phrase_terms))

        if score > best_score:
            best_score = score
            best_name = collection_name

    return best_name, best_score


def _is_compare_query(query: str) -> bool:
    q = f" {_safe_text(query).lower()} "
    if any(marker in q for marker in COMPARE_MARKERS):
        return True
    if " before and after " in q:
        return True
    return bool(re.search(COMPARE_CONNECTOR_RE, q) and (" compare " in q or " difference " in q or " between " in q))


def _extract_explicit_top_n(query: str) -> int:
    q = _safe_text(query).lower()
    if " bottom " in f" {q} ":
        return 0
    patterns = [
        r"\btop\s+(\d{1,4})\b",
        r"\bfirst\s+(\d{1,4})\b",
        r"\blimit\s+(\d{1,4})\b",
        r"\bshow\s+(\d{1,4})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, q)
        if match:
            try:
                return max(1, min(500, int(match.group(1))))
            except Exception:
                return 0
    return 0


def _adaptive_top_n(total_rows: int) -> int:
    if total_rows <= 12:
        return total_rows
    if total_rows <= 25:
        return 20
    if total_rows <= 60:
        return 15
    if total_rows <= 200:
        return 12
    return 10


def _rows_to_csv_text(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    output = io.StringIO()
    fieldnames = list(rows[0].keys())
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue()


def _finalize_compare_payload(
    query: str,
    scope: str,
    preferred_db: str,
    base_collection: str,
    metric_collections: List[str],
    metric_labels: List[str],
    rows: List[Dict[str, Any]],
    source_dbs: Dict[str, str],
    ranked_collections: List[Tuple[str, int, List[str]]],
    metric_phrases: List[str],
    phrase_collection_matches: List[Dict[str, Any]],
    connector_counts: Dict[str, int],
) -> Dict[str, Any]:
    if len(metric_collections) == 2:
        rows.sort(
            key=lambda item: (item.get("Total Compared Records", 0), abs(item.get("Difference", 0))),
            reverse=True,
        )
    else:
        rows.sort(key=lambda item: item.get("Total Compared Records", 0), reverse=True)

    top_group_n, bottom_group_n = _extract_top_bottom_request(query)

    full_rows = rows
    total_row_count = len(full_rows)
    explicit_top_n = _extract_explicit_top_n(query)
    top_n = explicit_top_n or _adaptive_top_n(total_row_count)
    if top_group_n > 0 or bottom_group_n > 0:
        top_n = total_row_count
    top_n = max(1, min(top_n, total_row_count if total_row_count > 0 else 1))
    rows = full_rows[:top_n]
    is_truncated = len(rows) < total_row_count

    return {
        "handled": True,
        "scope": scope,
        "database": preferred_db,
        "base_collection": base_collection,
        "collections": [base_collection] + metric_collections,
        "metric_collections": metric_collections,
        "metric_labels": metric_labels,
        "rows": rows,
        "rows_full": full_rows,
        "top_n": top_n,
        "total_rows": total_row_count,
        "is_truncated": is_truncated,
        "row_limit_options": [5, 10, 12, 15, 20, 25, 50, 100],
        "display_order": ["chart", "table"],
        "visualization": {
            "type": "bar",
            "x_axis": f"{scope.title()} Name",
            "y_axes": metric_labels,
            "preferred_order": ["chart", "table"],
            "recommended_top_n": top_n,
        },
        "csv_export": {
            "available": total_row_count > 0,
            "filename": f"compare_{scope}_{top_n}_of_{total_row_count}.csv",
            "filename_all": f"compare_{scope}_all_{total_row_count}.csv",
            "csv_top": _rows_to_csv_text(rows),
            "csv_all": _rows_to_csv_text(full_rows) if total_row_count <= 5000 else "",
            "csv_all_truncated": total_row_count > 5000,
            "csv_all_note": "csv_all omitted because row count is large; use rows_full to stream/export externally" if total_row_count > 5000 else "",
        },
        "compare_phrases": metric_phrases,
        "compare_phrase_matches": phrase_collection_matches,
        "conjunction_counts": connector_counts,
        "ranked_collections": [
            {
                "collection": name,
                "score": score,
                "matched_keywords": hits,
            }
            for name, score, hits in ranked_collections[:10]
        ],
        "source_dbs": source_dbs,
    }


def _is_customer_vs_merchant_transaction_compare(query: str) -> bool:
    q = f" {_safe_text(query).lower()} "
    has_customer = " customer" in q
    has_merchant = " merchant" in q
    has_transaction = any(token in q for token in [" transaction", " transactions", "transacting", "frequency", "volume", "value"])
    has_compare_marker = any(marker in q for marker in [" vs ", " versus ", " between ", " compare "])
    return has_customer and has_merchant and has_transaction and has_compare_marker


def _is_city_grouped_merchant_transaction_compare(query: str) -> bool:
    q = f" {_safe_text(query).lower()} "
    has_city_signal = any(token in q for token in [" city", " cities", " location", " locations", "different cities"])
    has_merchant = " merchant" in q
    has_transaction = any(token in q for token in [" transaction", " transactions", "volume", "frequency", "value", "amount"])
    return has_city_signal and has_merchant and has_transaction


def _handle_customer_vs_merchant_transaction_compare(
    query: str,
    load_collection_data: Callable[[str, str], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    customer_rows = [row for row in (load_collection_data("numoni_customer", "transaction_history") or []) if isinstance(row, dict)]
    merchant_rows = [row for row in (load_collection_data("numoni_merchant", "transaction_history") or []) if isinstance(row, dict)]
    if not customer_rows and not merchant_rows:
        return {"handled": False}

    q = _safe_text(query).lower()
    agg_mode = "count"
    if any(token in q for token in ["volume", "amount", "value", "total"]):
        agg_mode = "sum"

    metric_label = "transaction_history Count" if agg_mode == "count" else "transaction_history Total"

    def _cohort_value(rows: List[Dict[str, Any]], cohort: str) -> Tuple[float, int]:
        if agg_mode == "count":
            value = float(len(rows))
        else:
            value = 0.0
            for row in rows:
                amount = _as_number(row.get("amount"))
                if amount == 0.0:
                    amount = _as_number(row.get("transactionAmount"))
                value += amount

        unique_entities = set()
        for row in rows:
            if cohort == "customer":
                candidate_keys = ["customerId", "userId", "createdBy", "id", "_id"]
            else:
                candidate_keys = ["merchantId", "userId", "createdBy", "id", "_id"]
            for key in candidate_keys:
                val = _safe_text(row.get(key))
                if val:
                    unique_entities.add(val)
                    break
        return round(value, 2), len(unique_entities)

    customer_value, customer_active = _cohort_value(customer_rows, "customer")
    merchant_value, merchant_active = _cohort_value(merchant_rows, "merchant")

    rows = [
        {
            "Cohort ID": "customer",
            "Cohort Name": "Customers",
            metric_label: customer_value,
            "Active Entities": customer_active,
            "Total Compared Records": customer_value,
        },
        {
            "Cohort ID": "merchant",
            "Cohort Name": "Merchants",
            metric_label: merchant_value,
            "Active Entities": merchant_active,
            "Total Compared Records": merchant_value,
        },
    ]

    return _finalize_compare_payload(
        query=query,
        scope="cohort",
        preferred_db="numoni_customer,numoni_merchant",
        base_collection="cohort",
        metric_collections=["transaction_history"],
        metric_labels=[metric_label],
        rows=rows,
        source_dbs={
            "cohort": "numoni_customer+numoni_merchant",
            "transaction_history": "numoni_customer+numoni_merchant",
        },
        ranked_collections=[("transaction_history", 100, ["transaction", "customer", "merchant"])],
        metric_phrases=["customer transactions", "merchant transactions"],
        phrase_collection_matches=[
            {"phrase": "customer transactions", "collection": "transaction_history", "score": 100},
            {"phrase": "merchant transactions", "collection": "transaction_history", "score": 100},
        ],
        connector_counts=_count_compare_connectors(query),
    )


def _handle_city_grouped_merchant_transaction_compare(
    query: str,
    load_collection_data: Callable[[str, str], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    def _normalized_id(value: Any) -> str:
        if isinstance(value, dict):
            oid = _safe_text(value.get("$oid"))
            if oid:
                return oid
        return _safe_text(value)

    location_rows = [row for row in (load_collection_data("numoni_merchant", "merchantlocation") or []) if isinstance(row, dict)]
    merchant_rows = [row for row in (load_collection_data("numoni_merchant", "merchantDetails") or []) if isinstance(row, dict)]
    tx_rows = [row for row in (load_collection_data("numoni_merchant", "transaction_history") or []) if isinstance(row, dict)]
    if not tx_rows:
        return {"handled": False}

    user_to_merchant: Dict[str, str] = {}
    for row in merchant_rows:
        user_id = _normalized_id(row.get("userId"))
        merchant_id = _normalized_id(row.get("merchantId")) or _normalized_id(row.get("_id"))
        if user_id and merchant_id:
            user_to_merchant[user_id] = merchant_id

    merchant_to_city: Dict[str, str] = {}
    for row in location_rows:
        city = _safe_text(row.get("city")) or _safe_text(row.get("state")) or _safe_text(row.get("address"))
        if not city:
            continue
        for key in ["merchantId", "userId", "createdBy", "id", "_id"]:
            alias = _normalized_id(row.get(key))
            if alias:
                merchant_to_city[alias] = city
                if key == "userId" and alias in user_to_merchant:
                    merchant_to_city[user_to_merchant[alias]] = city

    q = _safe_text(query).lower()
    agg_mode = "count"
    if any(token in q for token in ["volume", "amount", "value", "total"]):
        agg_mode = "sum"

    metric_label = "transaction_history Count" if agg_mode == "count" else "transaction_history Total"
    city_values: Dict[str, float] = {}

    for row in tx_rows:
        aliases = _extract_identity_values(row, "merchant")
        city = "Unknown"
        for alias in aliases:
            if alias in merchant_to_city:
                city = merchant_to_city[alias]
                break

        if agg_mode == "count":
            city_values[city] = city_values.get(city, 0.0) + 1.0
        else:
            amount = _as_number(row.get("amount"))
            if amount == 0.0:
                amount = _as_number(row.get("transactionAmount"))
            city_values[city] = city_values.get(city, 0.0) + amount

    rows: List[Dict[str, Any]] = []
    for city, value in city_values.items():
        rounded = round(value, 2)
        rows.append(
            {
                "City ID": _safe_text(city).lower() or "unknown",
                "City Name": city or "Unknown",
                metric_label: rounded,
                "Total Compared Records": rounded,
            }
        )

    if not rows:
        return {"handled": False}

    return _finalize_compare_payload(
        query=query,
        scope="city",
        preferred_db="numoni_merchant",
        base_collection="merchantlocation",
        metric_collections=["transaction_history"],
        metric_labels=[metric_label],
        rows=rows,
        source_dbs={
            "merchantlocation": "numoni_merchant",
            "transaction_history": "numoni_merchant",
        },
        ranked_collections=[
            ("merchantlocation", 100, ["city", "location"]),
            ("transaction_history", 100, ["transaction"]),
        ],
        metric_phrases=["transactions by city"],
        phrase_collection_matches=[{"phrase": "transactions by city", "collection": "transaction_history", "score": 100}],
        connector_counts=_count_compare_connectors(query),
    )


def _as_number(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = _safe_text(value)
    if not text:
        return 0.0
    text = text.replace(",", "")
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", text):
        try:
            return float(text)
        except Exception:
            return 0.0
    return 0.0


def _infer_agg_mode(phrase: str) -> str:
    tokens = _token_variants(_split_identifier(phrase))
    if any(token in AGG_AVG_WORDS for token in tokens):
        return "avg"
    if any(token in AGG_SUM_WORDS for token in tokens):
        return "sum"
    if any(token in AGG_COUNT_WORDS for token in tokens):
        return "count"
    return "count"


def _infer_metric_mode(collection_name: str, phrase: str, query: str) -> str:
    direct_mode = _infer_agg_mode(phrase)
    if direct_mode != "count":
        return direct_mode

    phrase_tokens = set(_token_variants(_split_identifier(phrase)))
    if phrase_tokens.intersection({"count", "frequency", "number", "times", "records"}):
        return "count"

    collection_tokens = set(_token_variants(_split_identifier(collection_name)))
    query_tokens = set(_token_variants(_split_identifier(query)))

    if query_tokens.intersection(AGG_AVG_WORDS):
        if {"transaction", "history"}.intersection(collection_tokens):
            return "avg"

    if query_tokens.intersection(AGG_SUM_WORDS):
        if {"wallet", "payout", "transaction", "ledger", "deal"}.intersection(collection_tokens):
            return "sum"

    if {"wallet", "payout"}.intersection(collection_tokens) and {"balance", "amount", "value", "total"}.intersection(query_tokens):
        return "sum"

    return "count"


def _metric_label(collection_name: str, agg_mode: str) -> str:
    prefix = "Count"
    if agg_mode == "sum":
        prefix = "Total"
    elif agg_mode == "avg":
        prefix = "Average"
    return f"{collection_name} {prefix}"


def _resolve_phrase_for_collection(collection_name: str, phrase_matches: List[Dict[str, Any]]) -> str:
    best_phrase = ""
    best_score = -1
    for item in phrase_matches:
        if _safe_text(item.get("collection")) != collection_name:
            continue
        score = int(item.get("score") or 0)
        if score > best_score:
            best_score = score
            best_phrase = _safe_text(item.get("phrase"))
    return best_phrase


def _candidate_numeric_fields(
    collection_name: str,
    metadata_map: Dict[str, Any],
    phrase: str,
    row: Dict[str, Any],
) -> List[str]:
    meta = metadata_map.get(collection_name, {}) if isinstance(metadata_map, dict) else {}
    meta_fields = meta.get("fields", []) if isinstance(meta, dict) else []
    phrase_tokens = set(_token_variants(_split_identifier(phrase)))

    field_scores: List[Tuple[str, int]] = []
    for key in row.keys():
        key_text = _safe_text(key)
        if not key_text:
            continue
        key_low = key_text.lower()
        key_tokens = set(_token_variants(_split_identifier(key_text)))
        score = 0

        if key_text in meta_fields:
            score += 5
        if any(hint in key_low for hint in NUMERIC_FIELD_HINTS):
            score += 6
        score += len(phrase_tokens.intersection(key_tokens)) * 4
        if score > 0:
            field_scores.append((key_text, score))

    field_scores.sort(key=lambda item: (-item[1], item[0]))
    return [name for name, _ in field_scores]


def _metric_value_for_row(
    row: Dict[str, Any],
    collection_name: str,
    metadata_map: Dict[str, Any],
    phrase: str,
    agg_mode: str,
) -> Tuple[float, bool]:
    if agg_mode == "count":
        return 1.0, True

    for field_name in _candidate_numeric_fields(collection_name, metadata_map, phrase, row):
        number_value = _as_number(row.get(field_name))
        if number_value != 0.0:
            return number_value, True

    for fallback in ["amount", "balance", "value", "totalAmount", "payoutAmount", "walletBalance"]:
        if fallback in row:
            number_value = _as_number(row.get(fallback))
            if number_value != 0.0:
                return number_value, True

    return 0.0, False


def _extract_top_bottom_request(query: str) -> Tuple[int, int]:
    q = _safe_text(query).lower()
    top_match = re.search(r"\btop\s+(\d{1,3})\b", q)
    bottom_match = re.search(r"\bbottom\s+(\d{1,3})\b", q)
    top_n = int(top_match.group(1)) if top_match else 0
    bottom_n = int(bottom_match.group(1)) if bottom_match else 0
    if top_n == 0 and bottom_n == 0 and "top" in q and "bottom" in q:
        return 5, 5
    return max(0, top_n), max(0, bottom_n)


def _apply_compare_post_pattern_filters(query: str, rows: List[Dict[str, Any]], metric_labels: List[str]) -> List[Dict[str, Any]]:
    if not rows or not metric_labels:
        return rows

    q = _safe_text(query).lower()
    filtered = rows

    top_n, bottom_n = _extract_top_bottom_request(q)
    if (top_n > 0 or bottom_n > 0) and metric_labels:
        first_metric = metric_labels[0]
        sorted_rows = sorted(filtered, key=lambda item: float(item.get(first_metric, 0) or 0), reverse=True)
        top_rows = sorted_rows[:top_n] if top_n > 0 else []
        bottom_rows = sorted_rows[-bottom_n:] if bottom_n > 0 else []
        combined: List[Dict[str, Any]] = []
        seen_ids = set()
        for item in top_rows + bottom_rows:
            key = _safe_text(item.get("Entity ID")) or _safe_text(item.get("Merchant ID")) or _safe_text(item.get("Customer ID"))
            marker = key or _safe_text(item)
            if marker in seen_ids:
                continue
            seen_ids.add(marker)
            combined.append(item)
        if combined:
            return combined

    if len(metric_labels) >= 2 and ("higher" in q and "than" in q):
        left_label = metric_labels[0]
        right_label = metric_labels[1]
        left_phrase = _normalize_spaces(q.split("compare", 1)[-1]) if "compare" in q else q
        if " than " in left_phrase:
            pivot = left_phrase.split(" than ", 1)
            if len(pivot) == 2:
                if any(token in pivot[0] for token in ["wallet", "balance"]):
                    left_label = next((label for label in metric_labels if "wallet" in label.lower() or "balance" in label.lower()), left_label)
                if any(token in pivot[1] for token in ["payout", "transaction", "deal", "session", "login"]):
                    right_label = next(
                        (label for label in metric_labels if any(t in label.lower() for t in ["payout", "transaction", "deal", "session", "login"])),
                        right_label,
                    )

        filtered = [
            row for row in filtered
            if float(row.get(left_label, 0) or 0) > float(row.get(right_label, 0) or 0)
        ]

    if len(metric_labels) >= 2 and "high" in q and "low" in q:
        left_label = metric_labels[0]
        right_label = metric_labels[1]
        sorted_high = sorted(filtered, key=lambda item: float(item.get(left_label, 0) or 0), reverse=True)
        sorted_low = sorted(filtered, key=lambda item: float(item.get(right_label, 0) or 0))
        high_cut = max(1, int(len(sorted_high) * 0.3))
        low_cut = max(1, int(len(sorted_low) * 0.3))
        high_ids = {
            _safe_text(item.get("Entity ID")) or _safe_text(item.get("Merchant ID")) or _safe_text(item.get("Customer ID"))
            for item in sorted_high[:high_cut]
        }
        low_ids = {
            _safe_text(item.get("Entity ID")) or _safe_text(item.get("Merchant ID")) or _safe_text(item.get("Customer ID"))
            for item in sorted_low[:low_cut]
        }
        target_ids = {entity_id for entity_id in high_ids if entity_id and entity_id in low_ids}
        if target_ids:
            filtered = [
                row for row in filtered
                if (_safe_text(row.get("Entity ID")) or _safe_text(row.get("Merchant ID")) or _safe_text(row.get("Customer ID"))) in target_ids
            ]

    return filtered


def _load_collection_any_db(
    collection_name: str,
    load_collection_data: Callable[[str, str], List[Dict[str, Any]]],
    preferred_db: str,
) -> Tuple[str, List[Dict[str, Any]]]:
    db_order = [preferred_db, "numoni_merchant", "numoni_customer", "authentication"]
    seen = set()
    for db_name in db_order:
        if not db_name or db_name in seen:
            continue
        seen.add(db_name)
        rows = load_collection_data(db_name, collection_name) or []
        if rows:
            return db_name, rows
    return preferred_db, []


def _extract_identity_values(row: Dict[str, Any], scope: str) -> List[str]:
    keys_by_scope = {
        "merchant": ["merchantId", "userId", "createdBy", "_id", "id"],
        "customer": ["customerId", "userId", "createdBy", "_id", "id"],
        "user": ["userId", "createdBy", "_id", "id"],
    }
    out: List[str] = []
    for key in keys_by_scope.get(scope, keys_by_scope["user"]):
        value = _safe_text(row.get(key))
        if value and value not in out:
            out.append(value)
    return out


def _entity_name(row: Dict[str, Any], scope: str) -> str:
    preferred = {
        "merchant": ["businessName", "brandName", "merchantName", "name"],
        "customer": ["customerName", "name", "userName"],
        "user": ["name", "userName", "email"],
    }
    for key in preferred.get(scope, preferred["user"]):
        value = _safe_text(row.get(key))
        if value:
            return value
    return "N/A"


def _entity_id(row: Dict[str, Any], scope: str) -> str:
    preferred = {
        "merchant": ["merchantId", "userId", "_id", "id"],
        "customer": ["customerId", "userId", "_id", "id"],
        "user": ["userId", "_id", "id"],
    }
    for key in preferred.get(scope, preferred["user"]):
        value = _safe_text(row.get(key))
        if value:
            return value
    return "N/A"


def _choose_collections_for_compare(
    query: str,
) -> Tuple[str, List[str], List[Tuple[str, int, List[str]]], List[str], List[Dict[str, Any]], Dict[str, int]]:
    metadata_map = _enriched_metadata_map(load_collection_keywords())
    if not metadata_map:
        return "", [], [], [], [], {}

    tokens = _tokenize_query(query)
    query_low = _safe_text(query).lower()
    query_terms = set(_token_variants(_split_identifier(query_low) + tokens))
    scope = _infer_scope(query)
    ranked: List[Tuple[str, int, List[str]]] = []

    for collection_name, meta in metadata_map.items():
        if not isinstance(meta, dict):
            continue
        score, hits = _score_collection(collection_name, meta, query, tokens)
        score += _collection_intent_bonus(collection_name, query, query_terms)
        score += _scope_alignment_bonus(scope, collection_name, query_low)

        if collection_name.lower().endswith("error") and not any(term in query_terms for term in ERROR_CONTEXT_HINTS):
            score -= 8

        if score > 0:
            ranked.append((collection_name, score, hits))

    ranked.sort(key=lambda item: (-item[1], item[0]))
    if not ranked:
        return "", [], [], [], [], {}

    base_by_scope = {
        "merchant": "merchantDetails",
        "customer": "customerDetails",
        "user": "authuser",
    }
    base_collection = base_by_scope.get(scope, "authuser")
    if any(token in query_low for token in ["authentication", "auth", "registered", "registrations"]) and scope in {"customer", "user"}:
        if "customer" in query_low and "customerDetails" in metadata_map:
            base_collection = "customerDetails"
        elif "authuser" in metadata_map:
            base_collection = "authuser"

    if base_collection not in metadata_map:
        base_collection = ranked[0][0]

    metric_phrases = _extract_metric_phrases(query)
    phrase_collection_matches: List[Dict[str, Any]] = []

    explicit_collections: List[str] = []
    for collection_name in metadata_map.keys():
        collection_low = collection_name.lower()
        if re.search(rf"\b{re.escape(collection_low)}\b", query_low) and collection_name != base_collection:
            explicit_collections.append(collection_name)

    selected_metrics: List[str] = []
    for collection_name in explicit_collections:
        if collection_name not in selected_metrics:
            selected_metrics.append(collection_name)

    for phrase in metric_phrases:
        matched_collection, match_score = _match_metric_phrase_to_collection(phrase, metadata_map)
        phrase_terms = set(_token_variants(_split_identifier(phrase) + _tokenize_query(phrase)))
        if matched_collection:
            match_score += _collection_intent_bonus(matched_collection, phrase, phrase_terms)
            match_score += _scope_alignment_bonus(scope, matched_collection, query_low)
        if matched_collection and matched_collection != base_collection and match_score >= 7:
            phrase_collection_matches.append(
                {
                    "phrase": phrase,
                    "collection": matched_collection,
                    "score": match_score,
                }
            )
            if matched_collection not in selected_metrics:
                selected_metrics.append(matched_collection)

    for collection_name in metadata_map.keys():
        if collection_name == base_collection or collection_name in selected_metrics:
            continue
        bonus = _collection_intent_bonus(collection_name, query, query_terms)
        if bonus >= 14:
            selected_metrics.append(collection_name)

    candidates = [name for name, _, _ in ranked if name != base_collection]
    connector_counts = _count_compare_connectors(query)
    connector_total = sum(connector_counts.values())
    phrase_count = len(metric_phrases)
    matched_metric_count = len({item.get("collection") for item in phrase_collection_matches if _safe_text(item.get("collection"))})
    has_top_bottom = " top " in f" {query_low} " and " bottom " in f" {query_low} "
    if has_top_bottom and phrase_count <= 1:
        target_metrics = 1
    elif matched_metric_count > 0:
        target_metrics = matched_metric_count
    else:
        target_metrics = max(1, phrase_count if phrase_count > 0 else (2 if connector_total > 0 else 1))

    ranked_score_map = {name: score for name, score, _ in ranked}
    for coll in candidates:
        if len(selected_metrics) >= target_metrics:
            break
        if coll == base_collection or coll in selected_metrics:
            continue
        if ranked_score_map.get(coll, 0) < 12:
            continue
        selected_metrics.append(coll)

    return base_collection, selected_metrics, ranked, metric_phrases, phrase_collection_matches, connector_counts


def handle_compare_keyword_query(
    query: str,
    load_collection_data: Callable[[str, str], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    if not _is_compare_query(query):
        return {"handled": False}

    if _is_customer_vs_merchant_transaction_compare(query):
        return _handle_customer_vs_merchant_transaction_compare(query, load_collection_data)

    if _is_city_grouped_merchant_transaction_compare(query):
        return _handle_city_grouped_merchant_transaction_compare(query, load_collection_data)

    (
        base_collection,
        metric_collections,
        ranked,
        metric_phrases,
        phrase_collection_matches,
        connector_counts,
    ) = _choose_collections_for_compare(query)
    if not base_collection or len(metric_collections) < 1:
        return {"handled": False}

    scope = _infer_scope(query)
    preferred_db = "numoni_merchant" if scope == "merchant" else "numoni_customer" if scope == "customer" else "authentication"
    metadata_map = load_collection_keywords()

    base_db, base_rows = _load_collection_any_db(base_collection, load_collection_data, preferred_db)
    base_rows = [row for row in base_rows if isinstance(row, dict)]
    if not base_rows:
        return {"handled": False}

    entity_map: Dict[str, Dict[str, Any]] = {}
    alias_to_entity: Dict[str, str] = {}

    for row in base_rows:
        entity_id = _entity_id(row, scope)
        if entity_id == "N/A":
            continue

        entity_name = _entity_name(row, scope)
        aliases = _extract_identity_values(row, scope)
        aliases.append(entity_id)

        entity_map.setdefault(entity_id, {
            "Entity ID": entity_id,
            "Entity Name": entity_name,
            "_aliases": set(),
        })

        for alias in aliases:
            if not alias:
                continue
            entity_map[entity_id]["_aliases"].add(alias)
            alias_to_entity[alias] = entity_id

    metric_modes: Dict[str, str] = {}
    metric_phrases_by_collection: Dict[str, str] = {}
    for metric_collection in metric_collections:
        phrase = _resolve_phrase_for_collection(metric_collection, phrase_collection_matches)
        metric_phrases_by_collection[metric_collection] = phrase
        metric_modes[metric_collection] = _infer_metric_mode(metric_collection, phrase, query)

    metric_values: Dict[str, Dict[str, float]] = {
        metric_collection: {eid: 0.0 for eid in entity_map.keys()}
        for metric_collection in metric_collections
    }
    metric_observations: Dict[str, Dict[str, int]] = {
        metric_collection: {eid: 0 for eid in entity_map.keys()}
        for metric_collection in metric_collections
    }

    metric_sources: Dict[str, str] = {}
    for metric_collection in metric_collections:
        metric_db, metric_rows = _load_collection_any_db(metric_collection, load_collection_data, preferred_db)
        metric_sources[metric_collection] = metric_db

        for row in metric_rows:
            if not isinstance(row, dict):
                continue
            aliases = _extract_identity_values(row, scope)
            matched_entity = ""
            for alias in aliases:
                if alias in alias_to_entity:
                    matched_entity = alias_to_entity[alias]
                    break
            if matched_entity:
                agg_mode = metric_modes.get(metric_collection, "count")
                phrase = metric_phrases_by_collection.get(metric_collection, "")
                metric_value, found_numeric = _metric_value_for_row(
                    row,
                    metric_collection,
                    metadata_map,
                    phrase,
                    agg_mode,
                )

                if agg_mode in {"sum", "avg"} and not found_numeric:
                    metric_value = 1.0

                metric_values[metric_collection][matched_entity] += metric_value
                metric_observations[metric_collection][matched_entity] += 1

    metric_labels = [_metric_label(metric, metric_modes.get(metric, "count")) for metric in metric_collections]

    rows: List[Dict[str, Any]] = []
    for entity_id, info in entity_map.items():
        row: Dict[str, Any] = {
            f"{scope.title()} ID": info["Entity ID"],
            f"{scope.title()} Name": info["Entity Name"],
        }
        total_compared = 0
        for metric, label in zip(metric_collections, metric_labels):
            value = metric_values[metric].get(entity_id, 0.0)
            if metric_modes.get(metric) == "avg":
                obs = metric_observations[metric].get(entity_id, 0)
                value = (value / obs) if obs > 0 else 0.0

            if isinstance(value, float):
                value = round(value, 2)
            row[label] = value
            total_compared += float(value)

        row["Total Compared Records"] = round(total_compared, 2)
        if len(metric_collections) == 2:
            row["Difference"] = round(float(row.get(metric_labels[0], 0) or 0) - float(row.get(metric_labels[1], 0) or 0), 2)

        rows.append(row)

    rows = _apply_compare_post_pattern_filters(query, rows, metric_labels)

    source_dbs = {base_collection: base_db}
    for metric_collection in metric_collections:
        source_dbs[metric_collection] = metric_sources.get(metric_collection, preferred_db)

    return _finalize_compare_payload(
        query=query,
        scope=scope,
        preferred_db=preferred_db,
        base_collection=base_collection,
        metric_collections=metric_collections,
        metric_labels=metric_labels,
        rows=rows,
        source_dbs=source_dbs,
        ranked_collections=ranked,
        metric_phrases=metric_phrases,
        phrase_collection_matches=phrase_collection_matches,
        connector_counts=connector_counts,
    )
