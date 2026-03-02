import json
import re
from pathlib import Path
from functools import lru_cache
from typing import Callable, Dict, List, Tuple, Any

import pandas as pd
import streamlit as st


def _normalized_compact(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (text or "").lower())


def _normalize_query_phrases(text: str) -> str:
    q = " ".join((text or "").lower().split())
    replacements = [
        (r"\blogged\s+in\b", "login"),
        (r"\blog\s+in\b", "login"),
        (r"\blogged\b", "login"),
        (r"\blogging\b", "login"),
        (r"\bsigned\s+in\b", "signin"),
        (r"\bsign\s+in\b", "signin"),
    ]
    for pattern, repl in replacements:
        q = re.sub(pattern, repl, q)
    return q


def _tokenize_query(query_text: str) -> List[str]:
    text = _normalize_query_phrases(query_text)
    cleaned = "".join(ch if (ch.isalnum() or ch in {"_", " "}) else " " for ch in text)
    words = [word for word in cleaned.split() if len(word) >= 2]
    bigrams = [f"{words[i]} {words[i+1]}" for i in range(len(words) - 1)]
    trigrams = [f"{words[i]} {words[i+1]} {words[i+2]}" for i in range(len(words) - 2)]

    ordered = []
    seen = set()
    for token in words + bigrams + trigrams:
        if token not in seen:
            seen.add(token)
            ordered.append(token)
    return ordered


@lru_cache(maxsize=1)
def load_collection_usage_linker(linker_path: str) -> Dict[str, Any]:
    file_path = Path(linker_path)
    if not file_path.exists():
        return {}
    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _extract_show_subject_terms(query_text: str) -> List[str]:
    query_lower = _normalize_query_phrases(query_text)
    match = re.search(
        r"^\s*show\s+me\s+(?:all\s+)?(.+?)(?:\s+(?:with|along\s+with|who|that|which|where|having)\b|$)",
        query_lower,
    )
    if not match:
        return []

    subject = match.group(1).strip(" .,")
    subject = re.sub(r"\b(their|the|a|an)\b", " ", subject)
    parts = [part.strip() for part in re.split(r"\s+|,", subject) if part.strip()]
    return list(dict.fromkeys(parts))


def _extract_requested_terms(query_text: str) -> List[str]:
    query_lower = _normalize_query_phrases(query_text)
    terms: List[str] = []

    with_match = re.search(r"\b(?:along\s+with|with)\s+(.+)$", query_lower)
    if with_match:
        tail = with_match.group(1).strip(" .,")
        parts = [part.strip() for part in re.split(r",|\band\b", tail) if part.strip()]
        for part in parts:
            part = re.sub(r"^(their|the|a|an)\s+", "", part)
            if len(part) >= 3:
                terms.append(part)

    who_match = re.search(r"\bwho\s+(.+)$", query_lower)
    if who_match:
        tail = who_match.group(1)
        clause_parts = [part.strip() for part in re.split(r"\bbut\b|\band\b", tail) if part.strip()]
        for part in clause_parts:
            cleaned = re.sub(r"\b(have|has|had|never|no|not|received|created|in|on|of|for|to)\b", " ", part)
            cleaned = " ".join(cleaned.split())
            if len(cleaned) >= 3:
                terms.append(cleaned)

    return list(dict.fromkeys(terms))


def _is_negative_style_query(query_text: str) -> bool:
    query_lower = _normalize_query_phrases(query_text)
    negative_markers = [
        " never ", " no ", " not ", " without ", " none ", " null ", " missing ",
        " but not ", " but no ", " have no ", " has no ", " with no ",
    ]
    padded = f" {query_lower} "
    return any(marker in padded for marker in negative_markers)


def _extract_focus_terms(query_text: str) -> List[str]:
    tokens = _tokenize_query(query_text)
    stop_words = {
        "which", "what", "show", "show me", "get", "get me", "how", "many", "count", "number",
        "total", "of", "in", "on", "for", "to", "from", "with", "along", "along with", "and",
        "or", "the", "a", "an", "is", "are", "was", "were", "be", "being", "been", "have",
        "has", "had", "who", "that", "where", "when", "it", "this", "these", "those", "all",
        "customer", "customers", "merchant", "merchants", "user", "users", "records", "record",
        "no", "not", "never", "without", "none", "null", "missing", "but",
        "more", "than", "less", "least", "most", "one", "two", "three", "four", "five",
    }
    out: List[str] = []
    for token in tokens:
        normalized = " ".join(token.split())
        if not normalized or normalized in stop_words:
            continue
        if len(normalized) < 3:
            continue
        if normalized not in out:
            out.append(normalized)
    return out[:12]


def _entity_preference_from_query(query_text: str) -> List[str]:
    query_lower = _normalize_query_phrases(query_text)
    preferred: List[str] = []
    if "customer" in query_lower:
        preferred.append("customerdetails")
    if "merchant" in query_lower:
        preferred.append("merchantdetails")
    if "user" in query_lower:
        preferred.append("authuser")
    return preferred


def _build_aliases(collection_name: str, entry: Any) -> List[str]:
    aliases = {collection_name.lower(), collection_name.lower().replace("_", " ")}
    if isinstance(entry, list) and len(entry) > 1 and isinstance(entry[1], list):
        for keyword in entry[1]:
            keyword_text = str(keyword or "").strip().lower()
            if keyword_text:
                aliases.add(keyword_text)
                aliases.add(keyword_text.replace("_", " "))
    return [alias for alias in aliases if alias]


def _resolve_ranked_collections(
    query_text: str,
    database_name: str,
    linker: Dict[str, Any],
) -> List[Tuple[str, int, List[str]]]:
    query_tokens = _tokenize_query(query_text)
    token_set = set(query_tokens)
    token_norm_set = {_normalized_compact(token) for token in query_tokens}
    query_lower = _normalize_query_phrases(query_text)
    generic_alias_terms = {
        "customer", "customers", "merchant", "merchants", "user", "users", "record", "records",
        "detail", "details", "data", "active", "status",
    }

    ranked: List[Tuple[str, int, List[str]]] = []

    for collection_name, entry in linker.items():
        if not collection_name or not isinstance(entry, list) or not entry:
            continue

        usage_line = str(entry[0] or "").lower()
        in_preferred_db = f"in {database_name.lower()}" in usage_line if database_name else False
        aliases = _build_aliases(collection_name, entry)

        score = 0
        matched_aliases: List[str] = []
        for alias in aliases:
            alias_norm = _normalized_compact(alias)
            alias_hit = False
            token_weight = 2 if alias in generic_alias_terms else 6
            text_weight = 1 if alias in generic_alias_terms else 4
            if alias in token_set:
                score += token_weight
                alias_hit = True
            if alias_norm and alias_norm in token_norm_set:
                score += token_weight
                alias_hit = True
            if re.search(r"\b" + re.escape(alias).replace(r"\ ", r"\s+") + r"\b", query_lower):
                score += text_weight
                alias_hit = True
            if alias_hit and alias not in matched_aliases:
                matched_aliases.append(alias)

        if in_preferred_db:
            score += 1

        if score > 0:
            ranked.append((collection_name, score, matched_aliases))

    ranked.sort(key=lambda item: (-item[1], item[0]))
    return ranked


def _pick_base_collection(
    base_collection: str,
    ranked: List[Tuple[str, int, List[str]]],
    subject_terms: List[str],
    entity_preferences: List[str],
) -> str:
    ranked_names = [name for name, _, _ in ranked]

    # First preference: explicit query entity preference (e.g., customers -> customerDetails)
    if entity_preferences:
        for collection_name, _, aliases in ranked:
            alias_blob = " ".join([collection_name.lower(), collection_name.lower().replace("_", " ")] + aliases)
            if any(pref in alias_blob.replace(" ", "") for pref in entity_preferences):
                return collection_name

    # Second preference: subject phrase extraction from query text
    if subject_terms:
        for collection_name, _, aliases in ranked:
            alias_blob = " ".join(aliases + [collection_name.lower(), collection_name.lower().replace("_", " ")])
            if any(term in alias_blob for term in subject_terms):
                return collection_name

    # Third preference: keep provided base if it is actually relevant
    if base_collection and base_collection in ranked_names:
        return base_collection

    # Fallback: top-ranked collection from linker scoring
    return ranked_names[0] if ranked_names else ""


def _relevant_negative_count_columns(query_text: str, columns: List[str]) -> List[str]:
    query_lower = _normalize_query_phrases(query_text)
    markers = [" but not ", " but no ", " have no ", " has no ", " with no ", " without ", " no ", " not "]
    absent_clause = ""
    padded = f" {query_lower} "
    positions = [(padded.find(marker), marker) for marker in markers if padded.find(marker) >= 0]
    if positions:
        cut, marker = min(positions, key=lambda x: x[0])
        absent_clause = padded[cut + len(marker):].strip()

    active_terms: List[str] = []
    clause_words = [w for w in re.split(r"\W+", absent_clause) if w]
    stop = {"in", "the", "a", "an", "and", "or", "record", "records", "have", "has", "with", "without", "not", "no"}
    for word in clause_words:
        if word not in stop and len(word) >= 3:
            active_terms.append(word)

    if not active_terms:
        intent_groups = {
            "login": ["login", "signin"],
            "session": ["session"],
            "deal": ["deal", "offer", "promotion"],
            "notification": ["notification", "alert"],
            "transaction": ["transaction", "payment"],
            "wallet": ["wallet", "balance", "ledger"],
        }
        for terms in intent_groups.values():
            if any(term in query_lower for term in terms):
                active_terms.extend(terms)

    count_cols = [col for col in columns if "count" in str(col).lower()]
    if not active_terms:
        return count_cols

    matched = [col for col in count_cols if any(term in str(col).lower() for term in active_terms)]
    return matched if matched else count_cols


def _apply_negative_presence_filter(df: pd.DataFrame, query_text: str) -> pd.DataFrame:
    if df.empty or not _is_negative_style_query(query_text):
        return df

    count_columns = _relevant_negative_count_columns(query_text, list(df.columns))
    if not count_columns:
        return df

    mask = pd.Series(True, index=df.index)
    for col in count_columns:
        series = pd.to_numeric(df[col], errors="coerce")
        col_mask = series.isna() | (series <= 0)
        mask = mask & col_mask

    filtered = df[mask]
    return filtered


def _load_collection_rows_across_known_dbs(
    preferred_database: str,
    collection_name: str,
    load_collection_data: Callable[[str, str], List[Dict[str, Any]]],
) -> Tuple[str, List[Dict[str, Any]]]:
    ordered_dbs = [preferred_database, "numoni_customer", "numoni_merchant", "authentication"]
    seen = set()
    for db_name in ordered_dbs:
        if not db_name or db_name in seen:
            continue
        seen.add(db_name)
        rows = load_collection_data(db_name, collection_name)
        if rows:
            return db_name, rows
    return preferred_database, []


def _find_identity_key(rows: List[Dict[str, Any]]) -> str:
    preferred = ["merchantId", "customerId", "userId", "id", "_id"]
    for key in preferred:
        if any(isinstance(row, dict) and row.get(key) is not None for row in rows):
            return key
    return ""


def _to_display_value(value: Any) -> Any:
    if isinstance(value, dict) and "$date" in value:
        return value.get("$date")
    return value


def _normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits if len(digits) >= 8 else str(value or "").strip().lower()


def _identity_tokens(row: Dict[str, Any]) -> List[str]:
    tokens: List[str] = []

    def _add(prefix: str, value: Any) -> None:
        text = str(value or "").strip()
        if not text:
            return
        token = f"{prefix}:{text.lower()}"
        if token not in tokens:
            tokens.append(token)

    def _add_email(value: Any) -> None:
        text = str(value or "").strip().lower()
        if text and "@" in text:
            _add("email", text)

    def _add_phone(value: Any) -> None:
        text = _normalize_phone(value)
        if text:
            _add("phone", text)

    for key in ["userId", "customerId", "merchantId", "id", "_id"]:
        if key in row and row.get(key) is not None:
            _add("id", row.get(key))

    for key in ["email", "userName", "createdBy"]:
        if key in row:
            _add_email(row.get(key))

    for key in ["phoneNumber", "phoneNo", "businessPhoneNo", "mobile", "phone"]:
        if key in row:
            _add_phone(row.get(key))

    return tokens


def _best_field_for_term(term: str, columns: List[str]) -> str:
    if not columns:
        return ""

    term_lower = " ".join((term or "").lower().split())
    term_tokens = set(re.split(r"\s+", term_lower))

    alias_map = {
        "last login time": ["activityTime", "lastLogin", "createdDt", "createdDate"],
        "login time": ["activityTime", "lastLogin", "createdDt", "createdDate"],
        "wallet balance": ["walletBalance", "availableBalance", "currentBalance", "balance", "amount"],
        "total transactions": ["transactionCount", "totalTransactions", "totalTransactionCount"],
        "payout": ["payoutAmount", "amount", "status", "payoutDate", "createdAt"],
        "deals": ["dealId", "dealName", "heading", "createdAt", "createdDt"],
    }

    for alias, candidates in alias_map.items():
        if alias in term_lower:
            for candidate in candidates:
                if candidate in columns:
                    return candidate

    best_col = ""
    best_score = -1
    for column in columns:
        col_text = re.sub(r"([a-z])([A-Z])", r"\1 \2", str(column)).lower().replace("_", " ")
        col_tokens = set(col_text.split())
        score = len(term_tokens & col_tokens) * 3
        if col_text in term_lower or term_lower in col_text:
            score += 4
        if _normalized_compact(col_text) == _normalized_compact(term_lower):
            score += 6
        if score > best_score:
            best_score = score
            best_col = str(column)

    return best_col if best_score > 0 else ""


def _is_count_style_query(query_text: str) -> bool:
    query_lower = " ".join((query_text or "").lower().split())
    return any(phrase in query_lower for phrase in ["how many", "number of", "count of", "total number"])


def _parse_top_n_query(query_text: str) -> int:
    query_lower = _normalize_query_phrases(query_text)
    match = re.search(r"\btop\s+(\d+)\b", query_lower)
    if not match:
        return 0
    try:
        return max(1, int(match.group(1)))
    except Exception:
        return 0


def _is_top_style_query(query_text: str) -> bool:
    return _parse_top_n_query(query_text) > 0


def _query_prefers_value_metric(query_text: str) -> bool:
    query_lower = _normalize_query_phrases(query_text)
    value_terms = ["value", "amount", "revenue", "spend", "sales", "transaction value", "total transaction"]
    return any(term in query_lower for term in value_terms)


def _extract_numeric_values(rows: List[Dict[str, Any]], candidate_fields: List[str]) -> List[float]:
    values: List[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for field in candidate_fields:
            if field not in row or row.get(field) is None:
                continue
            try:
                num = float(str(row.get(field)).replace(",", "").strip())
                values.append(num)
                break
            except Exception:
                continue
    return values


def _extract_entity_label(query_text: str) -> str:
    query_lower = " ".join((query_text or "").lower().split())
    if "merchant" in query_lower:
        return "Merchants"
    if "customer" in query_lower:
        return "Customers"
    if "user" in query_lower:
        return "Users"
    if "transaction" in query_lower:
        return "Transactions"
    return "Records"


def _best_count_column(df: pd.DataFrame) -> str:
    preferred = [
        "merchantId", "Merchant ID", "customerId", "Customer ID", "userId", "User ID", "id", "_id",
        "businessName", "merchantName", "customerName", "name", "email"
    ]
    for column in preferred:
        if column in df.columns:
            return column

    for column in df.columns:
        if column != "Source Database":
            return column
    return ""


def _parse_numeric_condition(query_text: str) -> Tuple[str, float] | None:
    query_lower = " ".join((query_text or "").lower().split())

    normalized = query_lower
    word_to_number = {
        "zero": "0",
        "one": "1",
        "two": "2",
        "three": "3",
        "four": "4",
        "five": "5",
        "six": "6",
        "seven": "7",
        "eight": "8",
        "nine": "9",
        "ten": "10",
    }
    for word, digit in word_to_number.items():
        normalized = re.sub(rf"\b{word}\b", digit, normalized)

    pattern_map = [
        (r"(?:greater\s+than\s+or\s+equal\s+to|at\s+least|not\s+less\s+than)\s*(-?\d+(?:\.\d+)?)", ">="),
        (r"(?:less\s+than\s+or\s+equal\s+to|at\s+most|not\s+more\s+than)\s*(-?\d+(?:\.\d+)?)", "<="),
        (r"(?:greater\s+than|more\s+than|above|over)\s*(-?\d+(?:\.\d+)?)", ">"),
        (r"(?:less\s+than|below|under)\s*(-?\d+(?:\.\d+)?)", "<"),
        (r"(?:equal\s+to|equals?)\s*(-?\d+(?:\.\d+)?)", "=="),
        (r">=\s*(-?\d+(?:\.\d+)?)", ">="),
        (r"<=\s*(-?\d+(?:\.\d+)?)", "<="),
        (r">\s*(-?\d+(?:\.\d+)?)", ">"),
        (r"<\s*(-?\d+(?:\.\d+)?)", "<"),
        (r"=\s*(-?\d+(?:\.\d+)?)", "=="),
    ]

    for pattern, operator in pattern_map:
        match = re.search(pattern, normalized)
        if match:
            try:
                return operator, float(match.group(1))
            except Exception:
                continue

    return None


def _find_condition_column(df: pd.DataFrame, query_text: str) -> str:
    query_lower = (query_text or "").lower()
    preferred_keywords = ["balance", "wallet", "amount", "available", "current"]

    # Prefer semantic matches first (e.g. wallet balance query -> Wallet/Balance columns)
    for column in df.columns:
        column_lower = str(column).lower()
        if any(keyword in query_lower for keyword in preferred_keywords):
            if any(keyword in column_lower for keyword in preferred_keywords):
                return str(column)

    # Fallback to first non-id numeric candidate
    for column in df.columns:
        column_lower = str(column).lower()
        if any(id_kw in column_lower for id_kw in ["id", "name", "email", "source database"]):
            continue
        numeric_series = pd.to_numeric(df[column], errors="coerce")
        if numeric_series.notna().any():
            return str(column)

    return ""


def _apply_numeric_condition(df: pd.DataFrame, column_name: str, operator: str, threshold: float) -> pd.DataFrame:
    if column_name not in df.columns:
        return df

    numeric_series = pd.to_numeric(df[column_name], errors="coerce")

    if operator == ">":
        mask = numeric_series > threshold
    elif operator == ">=":
        mask = numeric_series >= threshold
    elif operator == "<":
        mask = numeric_series < threshold
    elif operator == "<=":
        mask = numeric_series <= threshold
    elif operator == "==":
        mask = numeric_series == threshold
    else:
        return df

    filtered_df = df[mask]
    return filtered_df


def build_json_linker_secondary_view(
    query_text: str,
    database_name: str,
    base_collection: str,
    load_collection_data: Callable[[str, str], List[Dict[str, Any]]],
    linker_path: str,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    linker = load_collection_usage_linker(linker_path)
    if not linker:
        return [], []

    subject_terms = _extract_show_subject_terms(query_text)
    requested_terms = _extract_requested_terms(query_text)
    focus_terms = _extract_focus_terms(query_text)
    entity_preferences = _entity_preference_from_query(query_text)
    is_negative_query = _is_negative_style_query(query_text)
    is_top_query = _is_top_style_query(query_text)
    top_n = _parse_top_n_query(query_text)
    prefers_value_metric = _query_prefers_value_metric(query_text)
    query_lower = " ".join((query_text or "").lower().split())

    # For count-style numeric queries, auto-add the numeric intent term so display fields
    # use real numeric columns (e.g., walletBalance) instead of only collection counts.
    if _is_count_style_query(query_text):
        inferred_numeric_term = ""
        if "wallet balance" in query_lower:
            inferred_numeric_term = "wallet balance"
        elif "balance" in query_lower:
            inferred_numeric_term = "balance"
        elif "amount" in query_lower:
            inferred_numeric_term = "amount"
        elif "point" in query_lower or "points" in query_lower:
            inferred_numeric_term = "points"

        if inferred_numeric_term and not any(inferred_numeric_term in term.lower() for term in requested_terms):
            requested_terms.append(inferred_numeric_term)
    ranked = _resolve_ranked_collections(query_text, database_name, linker)
    if not ranked:
        return [], []

    ranked_names = [name for name, _, _ in ranked]
    count_style_query = _is_count_style_query(query_text)

    base_name = _pick_base_collection(
        base_collection="" if (count_style_query and entity_preferences) else base_collection,
        ranked=ranked,
        subject_terms=subject_terms,
        entity_preferences=entity_preferences,
    )
    if not base_name:
        return [], []

    selected = [base_name]
    if is_top_query:
        max_selected = 2
    elif count_style_query:
        max_selected = 3 if is_negative_query else 2
    else:
        max_selected = 6

    merged_requested_terms: List[str] = []
    for term in requested_terms + focus_terms:
        if term not in merged_requested_terms:
            merged_requested_terms.append(term)

    # Priority: collections directly tied to requested terms / query intent
    for term in merged_requested_terms:
        if len(selected) >= max_selected:
            break
        term_lower = term.lower()
        per_term_matches = 0
        per_term_limit = 3 if is_negative_query else 1
        for collection_name, _, aliases in ranked:
            if len(selected) >= max_selected:
                break
            if collection_name in selected:
                continue
            alias_blob = " ".join(aliases + [collection_name.lower(), collection_name.lower().replace("_", " ")])
            if term_lower in alias_blob or any(token in alias_blob for token in term_lower.split()):
                selected.append(collection_name)
                per_term_matches += 1
                if per_term_matches >= per_term_limit:
                    break

    # For negative queries, make sure high-signal linked collections are included early
    if is_negative_query:
        if len(selected) < max_selected:
            for collection_name, _, _ in ranked[:8]:
                if collection_name not in selected:
                    selected.append(collection_name)
                if len(selected) >= max_selected:
                    break

    # Secondary: top ranked extras
    if (not count_style_query and not is_top_query) or len(selected) < 2:
        for collection_name in ranked_names:
            if collection_name not in selected:
                selected.append(collection_name)
            if len(selected) >= max_selected:
                break

    base_db, base_rows = _load_collection_rows_across_known_dbs(database_name, selected[0], load_collection_data)
    base_rows = [row for row in base_rows if isinstance(row, dict)]
    if not base_rows:
        return [], selected

    base_key = _find_identity_key(base_rows)

    collection_indexes: Dict[str, Tuple[str, str, Dict[str, List[Dict[str, Any]]], Dict[str, List[Dict[str, Any]]]]] = {}
    for collection_name in selected:
        source_db, rows = _load_collection_rows_across_known_dbs(database_name, collection_name, load_collection_data)
        rows = [row for row in rows if isinstance(row, dict)]
        key_name = _find_identity_key(rows)
        idx: Dict[str, List[Dict[str, Any]]] = {}
        identity_idx: Dict[str, List[Dict[str, Any]]] = {}
        if key_name:
            for row in rows:
                key_value = row.get(key_name)
                if key_value is None:
                    continue
                idx.setdefault(str(key_value), []).append(row)
        for row in rows:
            for token in _identity_tokens(row):
                identity_idx.setdefault(token, []).append(row)
        collection_indexes[collection_name] = (source_db, key_name, idx, identity_idx)

    display_fields: List[Tuple[str, str, str]] = []
    base_columns = list(base_rows[0].keys()) if base_rows else []

    for candidate in ["merchantId", "customerId", "userId", "id", "_id", "businessName", "name", "customerName", "userName", "email"]:
        if candidate in base_columns:
            display_fields.append((selected[0], candidate, candidate))
        if len(display_fields) >= 3:
            break

    if is_top_query:
        metric_collection = ""
        metric_score = -1
        metric_candidates = ["transaction_history", "merchant_wallet_ledger", "customer_wallet_ledger", "wallet"]
        for collection_name in selected[1:]:
            score = 0
            cname = collection_name.lower()
            if cname in metric_candidates:
                score += 4
            if "transaction" in cname:
                score += 3
            if prefers_value_metric and any(k in cname for k in ["transaction", "wallet", "payout"]):
                score += 2
            if score > metric_score:
                metric_score = score
                metric_collection = collection_name

        if metric_collection:
            metric_field = "__sum_amount__" if prefers_value_metric else "__count__"
            metric_label = "Total Transaction Value" if prefers_value_metric else f"{metric_collection} Count"
            display_fields.append((metric_collection, metric_field, metric_label))

    elif not count_style_query:
        for term in merged_requested_terms:
            chosen = None
            for collection_name in selected:
                source_db, rows = _load_collection_rows_across_known_dbs(database_name, collection_name, load_collection_data)
                columns = list(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
                field_name = _best_field_for_term(term, columns)
                if not field_name and "transaction" in term.lower() and collection_name == "transaction_history":
                    field_name = "__count__"
                if not field_name:
                    continue
                chosen = (collection_name, field_name, term.title())
                break
            if chosen:
                display_fields.append(chosen)

    # For negative/no/never queries, include count columns so zero/missing filters can apply meaningfully
    if (is_negative_query or count_style_query) and not is_top_query:
        for collection_name in selected[1:5]:
            display_fields.append((collection_name, "__count__", f"{collection_name} Count"))

    # If no explicit requested terms, still expose linked collection counts for visibility.
    if not requested_terms:
        for collection_name in selected[1:4]:
            display_fields.append((collection_name, "__count__", f"{collection_name} Count"))

    deduped: List[Tuple[str, str, str]] = []
    seen_fields = set()
    for item in display_fields:
        dedupe_key = (item[0], item[1], item[2])
        if dedupe_key in seen_fields:
            continue
        seen_fields.add(dedupe_key)
        deduped.append(item)
    display_fields = deduped[:12]

    rows_out: List[Dict[str, Any]] = []
    for base_row in base_rows[:1000]:
        row_out: Dict[str, Any] = {"Source Database": base_db}

        base_id = str(base_row.get(base_key)) if base_key and base_row.get(base_key) is not None else ""

        for collection_name, field_name, display_name in display_fields:
            if collection_name == selected[0]:
                source_record = base_row
                source_list = [base_row]
            else:
                _, link_key, index_map, identity_map = collection_indexes.get(collection_name, (database_name, "", {}, {}))
                source_list = index_map.get(base_id, []) if base_id and index_map else []

                if not source_list and link_key and link_key in base_row and index_map:
                    alt_id = str(base_row.get(link_key))
                    source_list = index_map.get(alt_id, [])

                if not source_list and identity_map:
                    matched_rows: List[Dict[str, Any]] = []
                    for token in _identity_tokens(base_row):
                        for candidate in identity_map.get(token, []):
                            if candidate not in matched_rows:
                                matched_rows.append(candidate)
                    source_list = matched_rows

                source_record = source_list[0] if source_list else None

            if field_name == "__count__":
                value = len(source_list)
            elif field_name == "__sum_amount__":
                numeric_values = _extract_numeric_values(
                    source_list,
                    [
                        "transactionAmount",
                        "amount",
                        "amountPaid",
                        "amountByWallet",
                        "totalAmount",
                        "newPrice",
                        "initialPrice",
                    ],
                )
                value = sum(numeric_values) if numeric_values else 0
            else:
                value = source_record.get(field_name) if isinstance(source_record, dict) else None

            row_out[display_name] = _to_display_value(value) if value is not None else "N/A"

        rows_out.append(row_out)

    if is_top_query and rows_out:
        metric_col = "Total Transaction Value" if _query_prefers_value_metric(query_text) else "Count"
        if metric_col not in rows_out[0]:
            for key in rows_out[0].keys():
                if "value" in str(key).lower() or "count" in str(key).lower():
                    metric_col = key
                    break

        rows_out.sort(
            key=lambda row: float(row.get(metric_col, 0) or 0) if str(row.get(metric_col, "")).replace(".", "", 1).replace("-", "", 1).isdigit() else 0,
            reverse=True,
        )
        limit = _parse_top_n_query(query_text)
        if limit > 0:
            rows_out = rows_out[:limit]

    return rows_out, selected


def render_optional_json_linker_section(
    query: str,
    database_name: str,
    collection_name: str,
    load_collection_data: Callable[[str, str], List[Dict[str, Any]]],
    linker_path: str,
) -> None:
    st.markdown("---")
    st.subheader("🔗 Optional JSON Linker Secondary Layer")
    st.caption("Default normal path stays unchanged. Click only when you want linker-based merged output.")

    run_secondary_json_layer = st.button(
        "🔗 Generate Linked View Using collection_usage_linker.json",
        key=f"json_linker_secondary_{database_name}_{collection_name}_{hash(query)}",
    )

    if not run_secondary_json_layer:
        return

    with st.spinner("Building secondary linked output from collection_usage_linker.json..."):
        secondary_rows, used_collections = build_json_linker_secondary_view(
            query_text=query,
            database_name=database_name,
            base_collection=collection_name,
            load_collection_data=load_collection_data,
            linker_path=linker_path,
        )

    if used_collections:
        st.info(f"Linked collections used: {', '.join(used_collections[:8])}")

    if not secondary_rows:
        st.warning("No secondary linked output could be generated for this query/database.")
        return

    secondary_df = pd.DataFrame(secondary_rows)
    display_df = secondary_df

    display_df = _apply_negative_presence_filter(display_df, query)

    count_column = ""
    if _is_count_style_query(query):
        count_column = _best_count_column(display_df)
        if count_column and count_column in display_df.columns:
            valid_for_dedupe = display_df[count_column].astype(str)
            valid_mask = ~valid_for_dedupe.isin(["N/A", "", "None", "nan"])
            dedupe_df = display_df[valid_mask].drop_duplicates(subset=[count_column], keep="first")
            if len(dedupe_df) > 0:
                display_df = dedupe_df

        parsed_condition = _parse_numeric_condition(query)
        if parsed_condition:
            operator, threshold = parsed_condition
            condition_column = _find_condition_column(display_df, query)
            if condition_column and condition_column in display_df.columns:
                display_df = _apply_numeric_condition(display_df, condition_column, operator, threshold)

    st.success(f"✅ Secondary linked output generated: {len(display_df):,} rows")
    st.dataframe(display_df.head(200), use_container_width=True, hide_index=True)

    if _is_count_style_query(query):
        entity_label = _extract_entity_label(query)
        if count_column and count_column in display_df.columns:
            valid_series = display_df[count_column].astype(str)
            valid_series = valid_series[~valid_series.isin(["N/A", "", "None", "nan"])]
            total_count = int(valid_series.nunique()) if len(valid_series) > 0 else int(len(display_df))
        else:
            total_count = int(len(display_df))
        st.markdown(f"**Total {entity_label}: {total_count:,}**")

    secondary_csv = secondary_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Secondary Linked Output (CSV)",
        data=secondary_csv,
        file_name=f"{collection_name}_secondary_linked_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )
