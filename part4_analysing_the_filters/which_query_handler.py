#!/usr/bin/env python
"""Lightweight handler for natural-language 'which' questions.
Keeps scope isolated to 'which ...' queries so existing flows remain unchanged.
"""
import re
import json
from pathlib import Path
from functools import lru_cache
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Callable, Tuple


QUERY_KEYWORD_DATA_MAP: Dict[str, Dict[str, Dict[str, Any]]] = {
    "customer": {
        "auth_login": {
            "any_of": ["authentication", "auth", "login", "logged in", "login activity"],
            "all_of": ["customer"],
            "sources": [
                ("numoni_customer", "customerDetails"),
                ("authentication", "authuser"),
                ("authentication", "login_activities"),
            ],
            "columns": ["customerId", "userId", "email", "phoneNumber", "status", "activityType"],
        },
        "sessions": {
            "any_of": ["session", "sessions", "active session"],
            "all_of": ["customer"],
            "sources": [
                ("numoni_customer", "customerDetails"),
                ("authentication", "user_sessions"),
            ],
            "columns": ["userId", "isActive", "status"],
        },
        "wallet": {
            "any_of": ["wallet", "wallet balance", "ledger", "wallet activity"],
            "all_of": ["customer"],
            "sources": [
                ("numoni_customer", "customerDetails"),
                ("numoni_customer", "wallet"),
                ("numoni_customer", "customer_wallet_ledger"),
            ],
            "columns": ["customerId", "balance", "walletBalance", "amount", "status"],
        },
        "transactions": {
            "any_of": ["transaction", "transactions", "refund", "failed", "transacted"],
            "all_of": ["customer"],
            "sources": [
                ("numoni_customer", "customerDetails"),
                ("numoni_customer", "transaction_history"),
            ],
            "columns": ["customerId", "status", "transactionAmount", "transactionDate"],
        },
        "audit": {
            "any_of": ["audit", "audit trail", "audit login"],
            "all_of": ["customer"],
            "sources": [
                ("numoni_customer", "customerDetails"),
                ("authentication", "audit_trail"),
            ],
            "columns": ["userId", "action", "userType", "createdBy"],
        },
    },
    "merchant": {
        "core": {
            "any_of": ["merchant", "store", "vendor", "business"],
            "all_of": [],
            "sources": [
                ("numoni_merchant", "merchantDetails"),
            ],
            "columns": ["merchantId", "userId", "businessName", "status", "verificationStatus"],
        },
        "location": {
            "any_of": ["location", "locations", "city", "region", "outside"],
            "all_of": ["merchant"],
            "sources": [
                ("numoni_merchant", "merchantDetails"),
                ("numoni_merchant", "merchantlocation"),
            ],
            "columns": ["userId", "address", "city", "country", "postalCode"],
        },
        "auth_login": {
            "any_of": ["authentication", "auth", "login", "logged in", "verified", "blocked"],
            "all_of": ["merchant"],
            "sources": [
                ("numoni_merchant", "merchantDetails"),
                ("authentication", "authuser"),
                ("authentication", "login_activities"),
            ],
            "columns": ["userId", "email", "status", "accountLocked", "emailVerified", "phoneVerified"],
        },
        "sessions_audit": {
            "any_of": ["session", "sessions", "audit", "audit trail"],
            "all_of": ["merchant"],
            "sources": [
                ("numoni_merchant", "merchantDetails"),
                ("authentication", "user_sessions"),
                ("authentication", "audit_trail"),
            ],
            "columns": ["userId", "isActive", "action", "userType"],
        },
        "transactions": {
            "any_of": ["transaction", "transactions", "revenue"],
            "all_of": ["merchant"],
            "sources": [
                ("numoni_merchant", "merchantDetails"),
                ("numoni_merchant", "transaction_history"),
            ],
            "columns": ["merchantId", "status", "amount", "amountPaid", "branchName", "city"],
        },
        "wallet_payout": {
            "any_of": ["wallet", "ledger", "payout", "balance"],
            "all_of": ["merchant"],
            "sources": [
                ("numoni_merchant", "merchantDetails"),
                ("numoni_merchant", "wallet"),
                ("numoni_merchant", "merchant_wallet_ledger"),
                ("numoni_merchant", "merchant_payout"),
            ],
            "columns": ["merchantId", "amount", "balance", "status", "payoutDate"],
        },
        "deals_media": {
            "any_of": ["deal", "offers", "promotion", "image", "notification"],
            "all_of": ["merchant"],
            "sources": [
                ("numoni_merchant", "merchantDetails"),
                ("numoni_merchant", "deals"),
                ("numoni_merchant", "dealimage"),
                ("numoni_merchant", "businessimage"),
                ("numoni_merchant", "notifications"),
            ],
            "columns": ["userId", "dealId", "dealStatus", "isActive", "usertype"],
        },
    },
    "deals": {
        "deal_lookup": {
            "any_of": ["deal", "offer", "promotion", "expired", "active", "image"],
            "all_of": [],
            "sources": [
                ("numoni_merchant", "deals"),
                ("numoni_merchant", "dealimage"),
                ("numoni_merchant", "merchantDetails"),
                ("authentication", "authuser"),
            ],
            "columns": ["dealStatus", "isActive", "endDate", "merchantId", "userId"],
        },
    },
    "user": {
        "role_overlap": {
            "any_of": ["user", "users", "role", "roles", "across systems"],
            "all_of": ["role"],
            "sources": [
                ("authentication", "authuser"),
                ("authentication", "audit_trail"),
                ("numoni_customer", "customerDetails"),
                ("numoni_merchant", "merchantDetails"),
            ],
            "columns": ["userId", "userType", "roles", "email", "phoneNumber", "createdBy"],
        },
    },
    "authentication": {
        "auth_activity": {
            "any_of": ["auth", "authentication", "login", "session", "audit"],
            "all_of": [],
            "sources": [
                ("authentication", "login_activities"),
                ("authentication", "audit_trail"),
            ],
            "columns": ["userId", "userType", "activityType", "action", "status", "successful"],
        },
    },
}


CROSS_DB_QUERY_MAP: Dict[str, Dict[str, Any]] = {
    "login_without_transaction_either_db": {
        "any_of": ["login activity", "login", "users"],
        "all_of": ["no", "transaction", "either"],
        "sources": [
            ("authentication", "login_activities"),
            ("numoni_customer", "transaction_history"),
            ("numoni_merchant", "transaction_history"),
            ("numoni_customer", "customerDetails"),
            ("numoni_merchant", "merchantDetails"),
        ],
        "columns": ["userId", "activityType", "customerId", "merchantId", "transactionId"],
        "title": "Which Users Have Login Activity But No Transaction In Either DB",
        "target": "users",
        "database": "cross_db",
        "collections": ["login_activities", "transaction_history (customer)", "transaction_history (merchant)"],
        "action": "list",
        "filters": {"login_activity": ">0", "customer_transactions": "=0", "merchant_transactions": "=0"},
    },
    "auth_without_profiles": {
        "any_of": ["authuser", "authentication", "users", "user"],
        "all_of": ["no", "authuser", "customer", "business", "record"],
        "sources": [
            ("authentication", "authuser"),
            ("numoni_customer", "customerDetails"),
            ("numoni_merchant", "merchantDetails"),
        ],
        "columns": ["userId", "email", "phoneNumber", "name", "userName"],
        "title": "Which Users Exist In Authuser But Have Neither CustomerDetails Nor MerchantDetails Records",
        "target": "users",
        "database": "cross_db",
        "collections": ["authuser", "customerDetails", "merchantDetails"],
        "action": "list",
        "filters": {"auth_record": "exists", "customer_profile": "missing", "merchant_profile": "missing"},
    },
    "login_without_profiles": {
        "any_of": ["login activity", "login activities", "login"],
        "all_of": ["no", "business", "customer", "record"],
        "sources": [
            ("authentication", "login_activities"),
            ("numoni_customer", "customerDetails"),
            ("numoni_merchant", "merchantDetails"),
        ],
        "columns": ["userId", "activityType", "customerId", "merchantId"],
        "title": "Which Users Have Login Activity But No Business Or Customer Profile",
        "target": "users",
        "database": "cross_db",
        "collections": ["login_activities", "customerDetails", "merchantDetails"],
        "action": "list",
        "filters": {"login_activity": ">0", "customer_profile": "missing", "merchant_profile": "missing"},
    },
    "wallet_without_auth": {
        "any_of": ["wallet activity", "wallet", "ledger"],
        "all_of": ["no", "authuser", "record"],
        "sources": [
            ("numoni_customer", "wallet"),
            ("numoni_merchant", "wallet"),
            ("authentication", "authuser"),
            ("numoni_customer", "customerDetails"),
            ("numoni_merchant", "merchantDetails"),
        ],
        "columns": ["userId", "merchantId", "customerId", "amount", "balance", "walletBalance"],
        "title": "Which Users Have Wallet Activity But No Authentication Record",
        "target": "users",
        "database": "cross_db",
        "collections": ["wallet (numoni_customer)", "wallet (numoni_merchant)", "authuser"],
        "action": "list",
        "filters": {"wallet_activity": ">0", "auth_record": "missing"},
    },
    "shared_phone_in_auth": {
        "any_of": ["share", "same phone", "phone number"],
        "all_of": ["business", "customer", "authuser"],
        "sources": [
            ("authentication", "authuser"),
            ("numoni_customer", "customerDetails"),
            ("numoni_merchant", "merchantDetails"),
        ],
        "columns": ["phoneNumber", "phoneNo", "businessPhoneNo", "userId", "email"],
        "title": "Which Merchants And Customers Share The Same Phone Number In Authentication",
        "target": "users",
        "database": "cross_db",
        "collections": ["authuser", "customerDetails", "merchantDetails"],
        "action": "list",
        "filters": {"shared_phone": True, "customer_profile": "exists", "merchant_profile": "exists"},
    },
    "shared_device_in_sessions": {
        "any_of": ["same", "device", "user_sessions", "session"],
        "all_of": ["business", "customer", "device", "user_sessions"],
        "sources": [
            ("authentication", "user_sessions"),
            ("numoni_customer", "customerDetails"),
            ("numoni_merchant", "merchantDetails"),
        ],
        "columns": ["userId", "deviceId", "deviceToken", "deviceName", "token", "fcmToken"],
        "title": "Which Merchants And Customers Share The Same Device In User Sessions",
        "target": "users",
        "database": "cross_db",
        "collections": ["user_sessions", "customerDetails", "merchantDetails"],
        "action": "list",
        "filters": {"shared_device": True, "customer_profile": "exists", "merchant_profile": "exists"},
    },
    "users_in_both_customer_merchant": {
        "any_of": ["user", "appear", "both", "customerdetails", "merchantdetails"],
        "all_of": ["user", "both", "customerdetails", "merchantdetails"],
        "sources": [
            ("numoni_customer", "customerDetails"),
            ("numoni_merchant", "merchantDetails"),
        ],
        "columns": ["userId", "customerId", "merchantId", "email", "phoneNumber", "name", "businessName"],
        "title": "Which Users Appear In Both CustomerDetails And MerchantDetails",
        "target": "users",
        "database": "cross_db",
        "collections": ["customerDetails", "merchantDetails"],
        "action": "list",
        "filters": {"customer_profile": "exists", "merchant_profile": "exists"},
    },
    "transactions_both_dbs": {
        "any_of": ["transactions", "transaction history", "across both"],
        "all_of": ["customer", "business", "database"],
        "sources": [
            ("numoni_customer", "transaction_history"),
            ("numoni_merchant", "transaction_history"),
            ("authentication", "authuser"),
        ],
        "columns": ["userId", "customerId", "merchantId", "status", "transactionId"],
        "title": "Which Users Have Transactions Across Both Customer And Merchant Databases",
        "target": "users",
        "database": "cross_db",
        "collections": ["transaction_history (numoni_customer)", "transaction_history (numoni_merchant)", "authuser"],
        "action": "list",
        "filters": {"customer_transactions": ">0", "merchant_transactions": ">0"},
    },
    "auth_with_customer_not_merchant": {
        "any_of": ["authentication", "auth", "users"],
        "all_of": ["customerdetails", "not", "merchantdetails"],
        "sources": [
            ("authentication", "authuser"),
            ("numoni_customer", "customerDetails"),
            ("numoni_merchant", "merchantDetails"),
        ],
        "columns": ["userId", "email", "phoneNumber"],
        "title": "Which Users Exist In Authentication And CustomerDetails But Not MerchantDetails",
        "target": "users",
        "database": "cross_db",
        "collections": ["authuser", "customerDetails", "merchantDetails"],
        "action": "list",
        "filters": {"auth_record": "exists", "customer_profile": "exists", "merchant_profile": "missing"},
    },
    "auth_with_merchant_not_customer": {
        "any_of": ["authentication", "auth", "users"],
        "all_of": ["merchantdetails", "not", "customerdetails"],
        "sources": [
            ("authentication", "authuser"),
            ("numoni_customer", "customerDetails"),
            ("numoni_merchant", "merchantDetails"),
        ],
        "columns": ["userId", "email", "phoneNumber"],
        "title": "Which Users Exist In Authentication And MerchantDetails But Not CustomerDetails",
        "target": "users",
        "database": "cross_db",
        "collections": ["authuser", "customerDetails", "merchantDetails"],
        "action": "list",
        "filters": {"auth_record": "exists", "customer_profile": "missing", "merchant_profile": "exists"},
    },
    "merchants_without_auth": {
        "any_of": ["business", "merchantdetails", "operating"],
        "all_of": ["business", "no", "authuser"],
        "sources": [
            ("numoni_merchant", "merchantDetails"),
            ("authentication", "authuser"),
        ],
        "columns": ["merchantId", "userId", "businessName", "email", "phoneNumber"],
        "title": "Which Merchants Are Operating Without Authentication",
        "target": "merchants",
        "database": "cross_db",
        "collections": ["merchantDetails", "authuser"],
        "action": "list",
        "filters": {"merchant_profile": "exists", "auth_record": "missing"},
    },
    "merchant_financial_active_digital_inactive": {
        "any_of": ["business", "financially active", "digitally inactive", "transaction history", "login activity"],
        "all_of": ["business", "financially active", "digitally inactive"],
        "sources": [
            ("numoni_merchant", "merchantDetails"),
            ("numoni_merchant", "transaction_history"),
            ("authentication", "login_activities"),
        ],
        "columns": ["merchantId", "userId", "amount", "transactionAmount", "activityType"],
        "title": "Which Merchants Are Financially Active But Digitally Inactive",
        "target": "merchants",
        "database": "cross_db",
        "collections": ["merchantDetails", "transaction_history", "login_activities"],
        "action": "list",
        "filters": {"financial_activity": ">0", "digital_activity": 0},
    },
    "customer_digital_active_financial_inactive": {
        "any_of": ["customer", "digitally active", "financially inactive", "transaction history", "login activity"],
        "all_of": ["customer", "digitally active", "financially inactive"],
        "sources": [
            ("numoni_customer", "customerDetails"),
            ("authentication", "login_activities"),
            ("numoni_customer", "transaction_history"),
        ],
        "columns": ["customerId", "userId", "activityType", "transactionId", "status"],
        "title": "Which Customers Are Digitally Active But Financially Inactive",
        "target": "customers",
        "database": "cross_db",
        "collections": ["customerDetails", "login_activities", "transaction_history"],
        "action": "list",
        "filters": {"digital_activity": ">0", "financial_activity": 0},
    },
    "users_inconsistent_status": {
        "any_of": ["user", "inconsistent", "status", "systems"],
        "all_of": ["user", "inconsistent", "status"],
        "sources": [
            ("authentication", "authuser"),
            ("numoni_customer", "customerDetails"),
            ("numoni_merchant", "merchantDetails"),
        ],
        "columns": ["userId", "status", "verificationStatus", "isDeleted"],
        "title": "Which Users Have Inconsistent Status Across Systems",
        "target": "users",
        "database": "cross_db",
        "collections": ["authuser", "customerDetails", "merchantDetails"],
        "action": "list",
        "filters": {"status_consistency": "inconsistent"},
    },
    "merchant_revenue_no_audit": {
        "any_of": ["business", "revenue", "audit trail", "no"],
        "all_of": ["business", "revenue", "no", "audit trail"],
        "sources": [
            ("numoni_merchant", "merchantDetails"),
            ("numoni_merchant", "transaction_history"),
            ("authentication", "audit_trail"),
        ],
        "columns": ["merchantId", "userId", "amount", "status", "action", "userType"],
        "title": "Which Merchants Have Revenue But No Audit Trail",
        "target": "merchants",
        "database": "cross_db",
        "collections": ["merchantDetails", "transaction_history", "audit_trail"],
        "action": "list",
        "filters": {"revenue": ">0", "audit_trail": 0},
    },
}


CROSS_DB_SYNONYM_PACKS: Dict[str, List[str]] = {
    "user": ["user", "users", "account", "accounts", "identity", "identities"],
    "authuser": ["authuser", "auth user", "authentication", "auth", "auth record", "authentication record", "auth records", "authentication records"],
    "customerdetails": ["customerdetails", "customer details", "customer profile", "customer record", "customer account", "buyer profile", "client profile"],
    "merchantdetails": ["merchantdetails", "merchant details", "business profile", "merchant profile", "store profile", "vendor profile", "business details"],
    "login activity": ["login activity", "login activities", "login", "logged in", "sign in", "signed in", "signin", "sign-in", "login record", "login records", "sign in records", "signin records", "sign-in records", "sign in activity", "signin activity", "sign-in activity"],
    "wallet activity": ["wallet activity", "wallet", "wallet ledger", "ledger", "wallet movement", "wallet transactions"],
    "record": ["record", "records", "entry", "entries", "profile", "profiles", "account", "accounts", "detail", "details"],
    "same phone": ["same phone", "same phone number", "shared phone", "matching phone", "common phone", "phone match", "same mobile"],
    "share": ["share", "shared", "same", "matching", "common"],
    "transaction history": ["transaction history", "transactions", "transaction", "transaction records", "tx history"],
    "across both": ["across both", "in both", "both", "both databases", "across customer and merchant", "across merchant and customer"],
    "database": ["database", "databases", "db", "dbs", "systems", "platforms"],
    "either": ["either", "either db", "either database", "either dbs", "in either db", "in either database"],
    "neither": ["neither", "not either", "not in either", "none of"],
    "no": ["no", "not", "without", "missing", "absent", "lacking", "neither", "none", "does not", "do not", "dont", "doesnt", "is not", "isn't", "are not", "aren't"],
    "business": ["business", "businesses", "merchant", "merchants", "store", "stores", "vendor", "vendors", "company", "companies"],
    "customer": ["customer", "customers", "buyer", "buyers", "client", "clients"],
    "operating": ["operating", "running", "working", "active operations"],
    "financially active": ["financially active", "financial active", "has revenue", "earning", "transaction active", "active financially"],
    "financially inactive": ["financially inactive", "financial inactive", "no transactions", "zero transactions", "not transacting"],
    "digitally active": ["digitally active", "digital active", "has login", "login active", "online active", "signed in"],
    "digitally inactive": ["digitally inactive", "digital inactive", "no login", "never logged in", "offline"],
    "inconsistent": ["inconsistent", "mismatch", "different", "conflicting", "not same"],
    "status": ["status", "state", "verification status", "account status"],
    "audit trail": ["audit trail", "audit", "audit logs", "audit records"],
    "revenue": ["revenue", "earnings", "income", "sales amount", "amount earned"],
    "systems": ["systems", "across systems", "cross systems", "platforms", "across databases"],
    "user_sessions": ["user_sessions", "user sessions", "session", "sessions", "active session", "device session"],
    "device": ["device", "same device", "shared device", "device id", "device token", "phone device", "mobile device"],
    "both": ["both", "in both", "present in both", "appear in both", "exists in both"],
    "appear": ["appear", "present", "exists", "found"],
}


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "active", "enabled"}


def _get_status_filter(query_lower: str):
    if re.search(r"\bsuccessful\b|\bsuccess\b", query_lower):
        return "SUCCESSFUL"
    if re.search(r"\bfailed\b", query_lower):
        return "FAILED"
    if re.search(r"\bpending\b", query_lower):
        return "PENDING"
    if re.search(r"\bcompleted\b", query_lower):
        return "COMPLETED"
    if re.search(r"\bactive\b", query_lower):
        return "ACTIVE"
    return None


def _extract_target(query_lower: str) -> str:
    m = re.search(r"^\s*(?:which|show\s+me|get\s+me)\s+(?:some\s+|the\s+|all\s+)?([a-z_]+)", query_lower)
    if not m:
        return ""
    raw = m.group(1).strip()
    # normalize simple plural forms
    if raw.endswith("ies"):
        return raw[:-3] + "y"
    if raw.endswith("s"):
        return raw[:-1]
    return raw


def _is_highest_or_lowest(query_lower: str):
    is_high = any(k in query_lower for k in ["highest", "top", "most", "maximum", "max"])
    is_low = any(k in query_lower for k in ["lowest", "least", "minimum", "min", "bottom"])
    return is_high, is_low


def _dedupe_rows(rows: List[Dict[str, Any]], key_fields: List[str]) -> List[Dict[str, Any]]:
    seen = set()
    output = []
    for row in rows:
        key = tuple(str(row.get(k, "")).strip() for k in key_fields)
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _extract_customer_id(row: Dict[str, Any]) -> str:
    for key in ["customerId", "customerUserId", "userId", "sentCustomerId", "receiveCustomerId", "receiverId", "senderId"]:
        value = _safe_text(row.get(key))
        if value:
            return value
    return ""


def _extract_customer_name(row: Dict[str, Any]) -> str:
    for key in ["customerName", "name", "userName", "accountName", "fullName"]:
        value = _safe_text(row.get(key))
        if value:
            return value
    return ""


def _extract_user_id(row: Dict[str, Any]) -> str:
    for key in ["userId", "customerUserId", "sentUserId", "receiveUserId", "createdBy"]:
        value = _safe_text(row.get(key))
        if value:
            return value
    return ""


def _extract_phone(row: Dict[str, Any]) -> str:
    for key in ["phoneNumber", "phoneNo", "businessPhoneNo", "mobile", "phone"]:
        value = _safe_text(row.get(key))
        if value:
            return value
    return ""


def _normalize_phone(value: Any) -> str:
    text = _safe_text(value)
    if not text:
        return ""
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 10:
        return digits
    return text.strip().lower()


def _extract_email(row: Dict[str, Any]) -> str:
    for key in ["email", "userName", "createdBy"]:
        value = _safe_text(row.get(key))
        if value and "@" in value:
            return value
    return ""


def _normalize_email(value: Any) -> str:
    text = _safe_text(value).lower()
    if text.endswith("_customer") or text.endswith("_merchant"):
        text = text.rsplit("_", 1)[0]
    return text


def _normalize_email_key(value: Any) -> str:
    text = _normalize_email(value)
    return text if "@" in text else ""


def _contains_any(text: str, terms: List[str]) -> bool:
    query_text = re.sub(r"\s+", " ", _safe_text(text).lower()).strip()
    compact_text = _normalized_compact_text(query_text)
    return any(_query_has_generic_term(query_text, compact_text, term) for term in terms)


def _contains_all(text: str, terms: List[str]) -> bool:
    query_text = re.sub(r"\s+", " ", _safe_text(text).lower()).strip()
    compact_text = _normalized_compact_text(query_text)
    return all(_query_has_generic_term(query_text, compact_text, term) for term in terms)


def _expand_generic_term_variants(term: str) -> List[str]:
    base = re.sub(r"\s+", " ", _safe_text(term).lower().replace("_", " ")).strip()
    if not base:
        return []

    variants = {base}
    if base in CROSS_DB_SYNONYM_PACKS:
        variants.update(_safe_text(v).lower() for v in CROSS_DB_SYNONYM_PACKS.get(base, []) if _safe_text(v))

    if " " not in base and len(base) >= 4:
        if base.endswith("ies") and len(base) > 4:
            variants.add(base[:-3] + "y")
        elif base.endswith("s") and len(base) > 4:
            variants.add(base[:-1])
        else:
            variants.add(base + "s")

    cleaned = set()
    for v in variants:
        normalized = re.sub(r"\s+", " ", _safe_text(v).lower().replace("_", " ")).strip()
        if normalized:
            cleaned.add(normalized)
    return list(cleaned)


def _query_has_generic_term(query_text: str, compact_text: str, term: str) -> bool:
    for variant in _expand_generic_term_variants(term):
        pattern = r"\b" + re.escape(variant).replace(r"\ ", r"(?:[\W_]+)") + r"\b"
        if re.search(pattern, query_text):
            return True

        compact_variant = _normalized_compact_text(variant)
        if len(compact_variant) >= 4 and compact_variant in compact_text:
            return True

    return False


def _intent_matches_query(query_lower: str, spec: Dict[str, Any]) -> bool:
    any_of = [t for t in spec.get("any_of", []) if t]
    all_of = [t for t in spec.get("all_of", []) if t]
    if any_of and not _contains_any(query_lower, any_of):
        return False
    if all_of and not _contains_all(query_lower, all_of):
        return False
    return True


def _resolve_fetch_plan(query_lower: str, domains: List[str]) -> Dict[str, Any]:
    matched_intents: List[str] = []
    sources = set()
    columns = set()

    for domain in domains:
        specs = QUERY_KEYWORD_DATA_MAP.get(domain, {})
        for intent_name, spec in specs.items():
            if not _intent_matches_query(query_lower, spec):
                continue
            matched_intents.append(f"{domain}.{intent_name}")
            for src in spec.get("sources", []) or []:
                if isinstance(src, (list, tuple)) and len(src) == 2:
                    sources.add((str(src[0]), str(src[1])))
            for col in spec.get("columns", []) or []:
                if col:
                    columns.add(str(col))

    return {
        "intents": matched_intents,
        "sources": sorted(sources),
        "columns": sorted(columns),
    }


@lru_cache(maxsize=1)
def _load_summarised_linker_metadata() -> Dict[str, Any]:
    try:
        # linker_path = Path(__file__).resolve().parent / "summarised" / "summarised_metadata.json"
        linker_path = Path(__file__).resolve().parent / "summarised" / "collection_usage_linker.json"
        if not linker_path.exists():
            return {}
        data = json.loads(linker_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _query_tokens_for_linker(query_lower: str) -> List[str]:
    text = re.sub(r"[^a-z0-9_\s]", " ", query_lower.lower())
    words = [w for w in text.split() if len(w) >= 3]
    bigrams = [f"{words[i]} {words[i+1]}" for i in range(len(words) - 1)]
    return list(dict.fromkeys(words + bigrams))


def _extract_databases_from_usage_line(usage_line: str) -> List[str]:
    text = _safe_text(usage_line)
    if not text:
        return []

    db_matches = re.findall(r"\bin\s+([a-zA-Z0-9_]+)", text)
    allowed = {"authentication", "numoni_customer", "numoni_merchant"}
    ordered = []
    seen = set()
    for db_name in db_matches:
        db_norm = _safe_text(db_name)
        if db_norm not in allowed or db_norm in seen:
            continue
        seen.add(db_norm)
        ordered.append(db_norm)
    return ordered


def _build_keyword_linker_from_collection_usage(metadata: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    keyword_linker: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    if not isinstance(metadata, dict):
        return {}

    for collection_name, entry in metadata.items():
        if not collection_name:
            continue
        usage_line = ""
        raw_keywords: List[str] = []

        if isinstance(entry, list):
            if entry:
                usage_line = _safe_text(entry[0])
            if len(entry) > 1 and isinstance(entry[1], list):
                raw_keywords = [_safe_text(k).lower() for k in entry[1] if _safe_text(k)]
        elif isinstance(entry, dict):
            usage_line = _safe_text(entry.get("usage") or entry.get("description") or "")
            kw = entry.get("keywords")
            if isinstance(kw, list):
                raw_keywords = [_safe_text(k).lower() for k in kw if _safe_text(k)]

        db_names = _extract_databases_from_usage_line(usage_line)
        if not db_names:
            continue

        token_pool = set()
        token_pool.add(_safe_text(collection_name).lower())
        token_pool.add(_safe_text(collection_name).lower().replace("_", " "))

        for kw in raw_keywords:
            token_pool.add(kw)
            token_pool.add(kw.replace("_", " "))

        key_fields = re.findall(r"key fields:\s*([^\)]+)", usage_line, flags=re.IGNORECASE)
        for fields_blob in key_fields:
            for field in fields_blob.split(","):
                field_name = _safe_text(field).lower()
                if field_name:
                    token_pool.add(field_name)

        for db_name in db_names:
            for token in token_pool:
                token_norm = re.sub(r"\s+", " ", token).strip()
                if not token_norm:
                    continue
                keyword_linker[token_norm].append({"database": db_name, "collection": _safe_text(collection_name)})

    for token, entries in list(keyword_linker.items()):
        unique_entries = []
        seen_pairs = set()
        for item in entries:
            pair = (item.get("database"), item.get("collection"))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            unique_entries.append(item)
        keyword_linker[token] = unique_entries

    return dict(keyword_linker)


def _resolve_linker_sources(query_lower: str) -> Dict[str, Any]:
    metadata = _load_summarised_linker_metadata()
    if isinstance(metadata, dict) and "keyword_linker" in metadata:
        keyword_linker = metadata.get("keyword_linker", {})
    else:
        keyword_linker = _build_keyword_linker_from_collection_usage(metadata)
    if not isinstance(keyword_linker, dict):
        return {"sources": [], "matched_tokens": []}

    tokens = _query_tokens_for_linker(query_lower)
    source_counts = defaultdict(int)
    matched_tokens = []

    normalized_lookup: Dict[str, List[Dict[str, str]]] = {}
    for raw_token, entries in keyword_linker.items():
        norm = _normalized_compact_text(raw_token)
        if not norm:
            continue
        if norm not in normalized_lookup:
            normalized_lookup[norm] = []
        if isinstance(entries, list):
            normalized_lookup[norm].extend(entries)

    for token in tokens:
        entries = keyword_linker.get(token)
        if not isinstance(entries, list):
            token_norm = _normalized_compact_text(token)
            entries = normalized_lookup.get(token_norm)
        if not isinstance(entries, list):
            continue
        matched_tokens.append(token)
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            db_name = _safe_text(entry.get("database"))
            collection = _safe_text(entry.get("collection"))
            if not db_name or not collection:
                continue
            source_counts[(db_name, collection)] += 1

    ranked_sources = [
        source for source, _ in sorted(source_counts.items(), key=lambda kv: (-kv[1], kv[0][0], kv[0][1]))
    ]

    return {
        "sources": ranked_sources,
        "matched_tokens": matched_tokens,
    }


def _is_difference_query(query_lower: str) -> bool:
    normalized = re.sub(r"\s+", " ", _safe_text(query_lower).lower()).strip()
    text = f" {normalized} "
    markers = [
        " but not ",
        " but no ",
        " but have no ",
        " have no ",
        " has no ",
        " with no ",
        " no ",
        " not in ",
        " without ",
        " except ",
        " excluding ",
        " minus ",
        " missing in ",
        " absent in ",
    ]
    return any(marker in text for marker in markers)


def _collection_mention_index(query_lower: str, collection_name: str) -> int:
    base = _safe_text(collection_name)
    if not base:
        return -1

    aliases = {
        base.lower(),
        base.lower().replace("_", " "),
        re.sub(r"([a-z])([A-Z])", r"\1 \2", base).lower(),
    }

    ambiguous_parts = {
        "customer", "customers", "merchant", "merchants", "user", "users",
        "detail", "details", "record", "records",
    }
    split_parts = [p for p in re.split(r"[_\s]+", re.sub(r"([a-z])([A-Z])", r"\1 \2", base).lower()) if p]
    for part in split_parts:
        if part in ambiguous_parts:
            continue
        aliases.add(part)
        if len(part) > 3:
            if part.endswith("s"):
                aliases.add(part[:-1])
            else:
                aliases.add(part + "s")

    best = -1
    for alias in aliases:
        alias = re.sub(r"\s+", " ", alias).strip()
        if not alias:
            continue
        pattern = r"\b" + re.escape(alias).replace(r"\ ", r"\s+") + r"\b"
        match = re.search(pattern, query_lower)
        if not match:
            continue
        if best == -1 or match.start() < best:
            best = match.start()
    return best


def _row_identity_keys(row: Dict[str, Any]) -> set:
    keys = set()

    uid = _extract_user_id(row)
    if uid:
        keys.add(f"user:{uid.lower()}")

    email = _normalize_email_key(_extract_email(row))
    if email:
        keys.add(f"email:{email}")

    phone = _normalize_phone(_extract_phone(row))
    if phone:
        keys.add(f"phone:{phone}")

    return keys


def _pick_difference_pair(query_lower: str, linker_sources: List[Tuple[str, str]]) -> Tuple[Tuple[str, str], Tuple[str, str]]:
    unique_sources = []
    seen = set()
    for src in linker_sources or []:
        if not isinstance(src, (list, tuple)) or len(src) != 2:
            continue
        db_name = _safe_text(src[0])
        collection = _safe_text(src[1])
        if not db_name or not collection:
            continue
        pair = (db_name, collection)
        if pair in seen:
            continue
        seen.add(pair)
        unique_sources.append(pair)
        if len(unique_sources) >= 8:
            break

    if len(unique_sources) < 2:
        return ("", ""), ("", "")

    mention_rank = []
    for db_name, collection in unique_sources:
        mention_rank.append((_collection_mention_index(query_lower, collection), db_name, collection))

    mentioned = [item for item in mention_rank if item[0] >= 0]
    if len(mentioned) >= 2:
        mentioned.sort(key=lambda x: (x[0], len(x[2]), x[2]))

        text = f" {query_lower} "
        marker_positions = []
        for marker in [" but not ", " but no ", " but have no ", " not in ", " without ", " have no ", " has no ", " with no ", " no "]:
            pos = text.find(marker)
            if pos >= 0:
                marker_positions.append(pos)

        if marker_positions:
            cut = min(marker_positions)
            left_side = [item for item in mentioned if item[0] <= cut]
            right_side = [item for item in mentioned if item[0] > cut]
            if left_side and right_side:
                left_side.sort(key=lambda x: (-x[0], len(x[2]), x[2]))
                right_side.sort(key=lambda x: (x[0], len(x[2]), x[2]))
                return (left_side[0][1], left_side[0][2]), (right_side[0][1], right_side[0][2])

        if " without " in text and " in " in text:
            return (mentioned[1][1], mentioned[1][2]), (mentioned[0][1], mentioned[0][2])
        return (mentioned[0][1], mentioned[0][2]), (mentioned[-1][1], mentioned[-1][2])

    return unique_sources[0], unique_sources[1]


def _handle_linker_difference_query(
    query_lower: str,
    load_collection_data: Callable[[str, str], List[Dict[str, Any]]],
    linker_sources: List[Tuple[str, str]],
) -> Dict[str, Any]:
    if not _is_difference_query(query_lower):
        return {}

    left, right = _pick_difference_pair(query_lower, linker_sources)
    left_db, left_collection = left
    right_db, right_collection = right
    if not left_db or not left_collection or not right_db or not right_collection:
        return {}

    left_rows = load_collection_data(left_db, left_collection) or []
    right_rows = load_collection_data(right_db, right_collection) or []

    right_keys = set()
    for row in right_rows:
        right_keys.update(_row_identity_keys(row))

    output_rows: List[Dict[str, Any]] = []
    for row in left_rows:
        identity_keys = _row_identity_keys(row)
        if not identity_keys:
            continue
        if identity_keys.isdisjoint(right_keys):
            output_rows.append({
                "User ID": _extract_user_id(row) or "N/A",
                "Email": _extract_email(row) or "N/A",
                "Phone": _extract_phone(row) or "N/A",
                "Present In": f"{left_collection} ({left_db})",
                "Not In": f"{right_collection} ({right_db})",
            })

    output_rows = _dedupe_rows(output_rows, ["User ID", "Email", "Phone", "Present In", "Not In"])

    ordered_collections = [left_collection, right_collection]
    same_db_candidates = []
    for db_name, collection_name in linker_sources or []:
        if _safe_text(db_name) != left_db:
            continue
        if collection_name and collection_name not in same_db_candidates:
            same_db_candidates.append(collection_name)

    priority_collections = ["authuser", "user_sessions", "login_activities", "customerDetails", "merchantDetails"]
    ranked_candidates = []
    for name in priority_collections + same_db_candidates:
        if name not in ranked_candidates:
            ranked_candidates.append(name)

    for collection_name in ranked_candidates:
        if collection_name in ordered_collections:
            continue
        ordered_collections.append(collection_name)
        if len(ordered_collections) >= 3:
            break

    return {
        "handled": True,
        "title": _safe_text(query_lower).strip().title() or "Which Users Match Difference Query",
        "target": "users",
        "database": "cross_db" if left_db != right_db else left_db,
        "collections": ordered_collections,
        "action": "list",
        "filters": {
            "present_in": left_collection,
            "absent_in": right_collection,
        },
        "rows": output_rows,
    }


def _make_source_loader(load_collection_data: Callable[[str, str], List[Dict[str, Any]]], planned_sources, required_sources):
    sources = set(planned_sources or [])
    sources.update(required_sources or [])
    cache: Dict[Any, List[Dict[str, Any]]] = {}
    for db_name, collection_name in sources:
        cache[(db_name, collection_name)] = load_collection_data(db_name, collection_name) or []

    def _get(db_name: str, collection_name: str) -> List[Dict[str, Any]]:
        key = (db_name, collection_name)
        if key not in cache:
            cache[key] = load_collection_data(db_name, collection_name) or []
        return cache[key]

    return _get


def _collect_identity_maps(customer_details, merchant_details):
    customer_by_user = {}
    customer_by_phone = {}
    customer_by_email = {}
    merchant_by_user = {}
    merchant_by_phone = {}
    merchant_by_email = {}

    for row in customer_details:
        cid = _extract_customer_id(row) or _safe_text(row.get("customerId"))
        uid = _extract_user_id(row)
        phone = _normalize_phone(_extract_phone(row))
        email = _normalize_email_key(row.get("email") or row.get("userName"))
        if uid:
            customer_by_user[uid] = cid or uid
        if phone:
            customer_by_phone[phone] = cid or phone
        if email:
            customer_by_email[email] = cid or email

    for row in merchant_details:
        mid = _safe_text(row.get("merchantId") or row.get("_id") or row.get("id"))
        uid = _extract_user_id(row)
        phone = _normalize_phone(_extract_phone(row))
        email = _normalize_email_key(row.get("email") or row.get("userName"))
        if uid:
            merchant_by_user[uid] = mid or uid
        if phone:
            merchant_by_phone[phone] = mid or phone
        if email:
            merchant_by_email[email] = mid or email

    return {
        "customer_by_user": customer_by_user,
        "customer_by_phone": customer_by_phone,
        "customer_by_email": customer_by_email,
        "merchant_by_user": merchant_by_user,
        "merchant_by_phone": merchant_by_phone,
        "merchant_by_email": merchant_by_email,
    }


def _normalized_compact_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", _safe_text(text).lower())


def _expand_cross_db_term(term: str) -> List[str]:
    base = _safe_text(term).lower()
    if not base:
        return []
    variants = {base}
    if base in CROSS_DB_SYNONYM_PACKS:
        variants.update(_safe_text(v).lower() for v in CROSS_DB_SYNONYM_PACKS.get(base, []) if _safe_text(v))
    return [v for v in variants if v]


def _build_query_match_pack(query_lower: str) -> Dict[str, str]:
    text = _safe_text(query_lower).lower()
    text = text.replace("_", " ")
    text = re.sub(r"\s+", " ", text).strip()
    compact = _normalized_compact_text(text)
    return {"text": text, "compact": compact}


def _term_present_in_text(text: str, term: str) -> bool:
    normalized_term = re.sub(r"\s+", " ", _safe_text(term).lower()).strip()
    if not normalized_term:
        return False
    pattern = r"\b" + re.escape(normalized_term).replace(r"\ ", r"(?:[\W_]+)") + r"\b"
    return re.search(pattern, text) is not None


def _query_has_term(pack: Dict[str, str], term: str) -> bool:
    text = pack.get("text", "")
    compact = pack.get("compact", "")
    for variant in _expand_cross_db_term(term):
        if _term_present_in_text(text, variant):
            return True
        compact_variant = _normalized_compact_text(variant)
        if len(compact_variant) >= 5 and compact_variant in compact:
            return True
    return False


def _cross_db_rule_matches(query_lower: str, rule: Dict[str, Any]) -> bool:
    any_of = rule.get("any_of", []) or []
    all_of = rule.get("all_of", []) or []
    pack = _build_query_match_pack(query_lower)

    any_ok = (not any_of) or any(_query_has_term(pack, term) for term in any_of)
    all_ok = (not all_of) or all(_query_has_term(pack, term) for term in all_of)
    return any_ok and all_ok


def _eval_cross_db_rule(rule_key: str, query_lower: str, get_rows: Callable[[str, str], List[Dict[str, Any]]]):
    auth_rows = get_rows("authentication", "authuser")
    customer_details = get_rows("numoni_customer", "customerDetails")
    merchant_details = get_rows("numoni_merchant", "merchantDetails")
    identity = _collect_identity_maps(customer_details, merchant_details)

    customer_users = set(identity["customer_by_user"].keys())
    merchant_users = set(identity["merchant_by_user"].keys())
    customer_phones = set(identity["customer_by_phone"].keys())
    merchant_phones = set(identity["merchant_by_phone"].keys())
    customer_emails = set(identity["customer_by_email"].keys())
    merchant_emails = set(identity["merchant_by_email"].keys())

    rows = []

    if rule_key == "login_without_transaction_either_db":
        login_rows = get_rows("authentication", "login_activities")
        customer_tx = get_rows("numoni_customer", "transaction_history")
        merchant_tx = get_rows("numoni_merchant", "transaction_history")

        login_count_by_user = defaultdict(int)
        for row in login_rows:
            uid = _extract_user_id(row)
            if not uid:
                continue
            login_count_by_user[uid] += 1

        customer_tx_by_user = defaultdict(int)
        customer_id_to_user = {}
        for c in customer_details:
            uid = _extract_user_id(c)
            cid = _extract_customer_id(c)
            if uid and cid:
                customer_id_to_user[cid] = uid

        for row in customer_tx:
            uid = _extract_user_id(row) or _safe_text(row.get("customerUserId"))
            if not uid:
                cid = _extract_customer_id(row)
                uid = customer_id_to_user.get(cid, "") if cid else ""
            if not uid:
                continue
            customer_tx_by_user[uid] += 1

        merchant_tx_by_user = defaultdict(int)
        merchant_id_to_user = {}
        for m in merchant_details:
            uid = _extract_user_id(m)
            mid = _safe_text(m.get("merchantId") or m.get("_id") or m.get("id"))
            if uid and mid:
                merchant_id_to_user[mid] = uid

        for row in merchant_tx:
            uid = _extract_user_id(row)
            if not uid:
                mid = _safe_text(row.get("merchantId"))
                uid = merchant_id_to_user.get(mid, "") if mid else ""
            if not uid:
                continue
            merchant_tx_by_user[uid] += 1

        for uid in sorted(login_count_by_user.keys()):
            c_tx = customer_tx_by_user.get(uid, 0)
            m_tx = merchant_tx_by_user.get(uid, 0)
            if c_tx > 0 or m_tx > 0:
                continue
            rows.append({
                "User ID": uid,
                "Login Activity Count": login_count_by_user.get(uid, 0),
                "Customer DB Transactions": c_tx,
                "Merchant DB Transactions": m_tx,
            })

    elif rule_key == "auth_without_profiles":
        for a in auth_rows:
            uid = _extract_user_id(a)
            phone = _normalize_phone(_extract_phone(a))
            email = _normalize_email_key(a.get("email") or a.get("userName"))
            in_customer = (uid and uid in customer_users) or (phone and phone in customer_phones) or (email and email in customer_emails)
            in_merchant = (uid and uid in merchant_users) or (phone and phone in merchant_phones) or (email and email in merchant_emails)
            if in_customer or in_merchant:
                continue
            rows.append({
                "User ID": uid or "N/A",
                "Email": _safe_text(a.get("email")) or "N/A",
                "Phone": phone or "N/A",
                "In CustomerDetails": "NO",
                "In MerchantDetails": "NO",
            })

    elif rule_key == "login_without_profiles":
        login_rows = get_rows("authentication", "login_activities")
        login_users = {_extract_user_id(r) for r in login_rows if _extract_user_id(r)}
        for uid in sorted(login_users):
            if uid in customer_users or uid in merchant_users:
                continue
            rows.append({
                "User ID": uid,
                "Has Login Activity": "YES",
                "Customer Profile": "NO",
                "Business Profile": "NO",
            })

    elif rule_key == "wallet_without_auth":
        customer_wallet = get_rows("numoni_customer", "wallet")
        merchant_wallet = get_rows("numoni_merchant", "wallet")

        auth_user_ids = {_extract_user_id(a) for a in auth_rows if _extract_user_id(a)}
        auth_phones = {_normalize_phone(_extract_phone(a)) for a in auth_rows if _normalize_phone(_extract_phone(a))}
        auth_emails = {_normalize_email_key(a.get("email") or a.get("userName")) for a in auth_rows if _normalize_email_key(a.get("email") or a.get("userName"))}

        for row in customer_wallet:
            cid = _extract_customer_id(row)
            uid = _extract_user_id(row)
            phone = _normalize_phone(_extract_phone(row))
            email = _normalize_email_key(row.get("email") or row.get("userName"))
            has_auth = (uid and uid in auth_user_ids) or (phone and phone in auth_phones) or (email and email in auth_emails)
            if has_auth:
                continue
            if not cid and not uid:
                continue
            rows.append({
                "Entity": "Customer",
                "Customer ID": cid or "N/A",
                "User ID": uid or "N/A",
                "Wallet Balance": round(_extract_float(row, ["balance", "walletBalance", "amount", "availableBalance"]), 2),
                "Auth Record": "NO",
            })

        for row in merchant_wallet:
            mid = _safe_text(row.get("merchantId") or row.get("_id") or row.get("id"))
            uid = _extract_user_id(row)
            phone = _normalize_phone(_extract_phone(row))
            email = _normalize_email_key(row.get("email") or row.get("userName"))
            has_auth = (uid and uid in auth_user_ids) or (phone and phone in auth_phones) or (email and email in auth_emails)
            if has_auth:
                continue
            if not mid and not uid:
                continue
            rows.append({
                "Entity": "Merchant",
                "Merchant ID": mid or "N/A",
                "User ID": uid or "N/A",
                "Wallet Balance": round(_extract_float(row, ["balance", "walletBalance", "amount", "availableBalance"]), 2),
                "Auth Record": "NO",
            })

    elif rule_key == "shared_phone_in_auth":
        auth_phone_groups = defaultdict(lambda: {
            "auth_rows": [],
            "customer_count": 0,
            "merchant_count": 0,
            "customer_ids": set(),
            "merchant_ids": set(),
        })
        for a in auth_rows:
            uid = _extract_user_id(a)
            phone = _normalize_phone(_extract_phone(a))
            email = _normalize_email_key(a.get("email") or a.get("userName"))
            if not phone:
                continue
            in_customer = (uid and uid in customer_users) or (phone and phone in customer_phones) or (email and email in customer_emails)
            in_merchant = (uid and uid in merchant_users) or (phone and phone in merchant_phones) or (email and email in merchant_emails)
            group = auth_phone_groups[phone]
            group["auth_rows"].append(a)
            if in_customer:
                group["customer_count"] += 1
                if uid:
                    group["customer_ids"].add(uid)
            if in_merchant:
                group["merchant_count"] += 1
                if uid:
                    group["merchant_ids"].add(uid)

        for phone, group in sorted(auth_phone_groups.items(), key=lambda kv: len(kv[1]["auth_rows"]), reverse=True):
            if group["customer_count"] <= 0 or group["merchant_count"] <= 0:
                continue
            auth_count = len(group["auth_rows"])
            rows.append({
                "Phone": phone,
                "Auth Users": auth_count,
                "Customer-linked Auth Users": group["customer_count"],
                "Merchant-linked Auth Users": group["merchant_count"],
                "Customer Profile": "YES",
                "Merchant Profile": "YES",
            })

    elif rule_key == "shared_device_in_sessions":
        session_rows = get_rows("authentication", "user_sessions")
        device_groups = defaultdict(lambda: {
            "customer_users": set(),
            "merchant_users": set(),
            "session_count": 0,
        })

        for session in session_rows:
            uid = _extract_user_id(session)
            if not uid:
                continue

            device_candidates = [
                _safe_text(session.get("deviceId")),
                _safe_text(session.get("deviceToken")),
                _safe_text(session.get("deviceIdentifier")),
                _safe_text(session.get("deviceName")),
                _safe_text(session.get("fcmToken")),
                _safe_text(session.get("token")),
                _safe_text(session.get("pushToken")),
                _safe_text(session.get("userAgent")),
                _normalize_phone(session.get("phoneNumber") or session.get("mobile") or session.get("phone")),
            ]
            device_key = next((d for d in device_candidates if d), "")
            if not device_key:
                continue

            group = device_groups[device_key]
            group["session_count"] += 1
            if uid in customer_users:
                group["customer_users"].add(uid)
            if uid in merchant_users:
                group["merchant_users"].add(uid)

        for device_key, group in sorted(device_groups.items(), key=lambda kv: kv[1]["session_count"], reverse=True):
            if not group["customer_users"] or not group["merchant_users"]:
                continue
            rows.append({
                "Device": device_key,
                "Session Count": group["session_count"],
                "Customer Users": len(group["customer_users"]),
                "Merchant Users": len(group["merchant_users"]),
                "Customer User IDs": ", ".join(sorted(list(group["customer_users"]))[:5]),
                "Merchant User IDs": ", ".join(sorted(list(group["merchant_users"]))[:5]),
            })

    elif rule_key == "users_in_both_customer_merchant":
        customer_by_uid = {}
        customer_by_email = {}
        customer_by_phone = {}
        for row in customer_details:
            uid = _extract_user_id(row)
            em = _normalize_email_key(row.get("email") or row.get("userName"))
            ph = _normalize_phone(_extract_phone(row))
            if uid:
                customer_by_uid[uid] = row
            if em:
                customer_by_email[em] = row
            if ph:
                customer_by_phone[ph] = row

        merchant_by_uid = {}
        merchant_by_email = {}
        merchant_by_phone = {}
        for row in merchant_details:
            uid = _extract_user_id(row)
            em = _normalize_email_key(row.get("email") or row.get("userName"))
            ph = _normalize_phone(_extract_phone(row))
            if uid:
                merchant_by_uid[uid] = row
            if em:
                merchant_by_email[em] = row
            if ph:
                merchant_by_phone[ph] = row

        paired_keys = []
        for uid in sorted(set(customer_by_uid.keys()) & set(merchant_by_uid.keys())):
            paired_keys.append(("user_id", uid))
        for em in sorted(set(customer_by_email.keys()) & set(merchant_by_email.keys())):
            paired_keys.append(("email", em))
        for ph in sorted(set(customer_by_phone.keys()) & set(merchant_by_phone.keys())):
            paired_keys.append(("phone", ph))

        seen_pairs = set()
        for key_type, key_value in paired_keys:
            if key_type == "user_id":
                c = customer_by_uid.get(key_value, {})
                m = merchant_by_uid.get(key_value, {})
            elif key_type == "email":
                c = customer_by_email.get(key_value, {})
                m = merchant_by_email.get(key_value, {})
            else:
                c = customer_by_phone.get(key_value, {})
                m = merchant_by_phone.get(key_value, {})

            pair_sig = (
                _extract_user_id(c) or _normalize_email_key(c.get("email") or c.get("userName")) or _normalize_phone(_extract_phone(c)),
                _extract_user_id(m) or _normalize_email_key(m.get("email") or m.get("userName")) or _normalize_phone(_extract_phone(m)),
            )
            if pair_sig in seen_pairs:
                continue
            seen_pairs.add(pair_sig)

            rows.append({
                "Match Type": key_type,
                "Match Key": key_value,
                "User ID": _extract_user_id(c) or _extract_user_id(m) or "N/A",
                "Customer ID": _extract_customer_id(c) or "N/A",
                "Merchant ID": _safe_text(m.get("merchantId") or m.get("_id") or m.get("id")) or "N/A",
                "Customer Name": _extract_customer_name(c) or "N/A",
                "Merchant Name": _safe_text(m.get("businessName") or m.get("name") or m.get("brandName")) or "N/A",
                "Customer Email": _safe_text(c.get("email")) or "N/A",
                "Merchant Email": _safe_text(m.get("email")) or "N/A",
                "Customer Phone": _normalize_phone(_extract_phone(c)) or "N/A",
                "Merchant Phone": _normalize_phone(_extract_phone(m)) or "N/A",
            })

    elif rule_key == "transactions_both_dbs":
        customer_tx = get_rows("numoni_customer", "transaction_history")
        merchant_tx = get_rows("numoni_merchant", "transaction_history")

        customer_user_hits = defaultdict(int)
        for row in customer_tx:
            uid = _extract_user_id(row) or _safe_text(row.get("customerUserId"))
            if not uid:
                cid = _extract_customer_id(row)
                if cid and cid in identity["customer_by_user"].values():
                    uid = next((k for k, v in identity["customer_by_user"].items() if v == cid), "")
            if uid:
                customer_user_hits[uid] += 1

        merchant_user_hits = defaultdict(int)
        merchant_id_to_user = {(_safe_text(m.get("merchantId") or m.get("_id") or m.get("id"))): _extract_user_id(m) for m in merchant_details}
        for row in merchant_tx:
            uid = _extract_user_id(row)
            if not uid:
                mid = _safe_text(row.get("merchantId"))
                uid = merchant_id_to_user.get(mid, "") if mid else ""
            if uid:
                merchant_user_hits[uid] += 1

        both_users = sorted(set(customer_user_hits.keys()) & set(merchant_user_hits.keys()))
        for uid in both_users:
            rows.append({
                "User ID": uid,
                "Customer DB Transactions": customer_user_hits.get(uid, 0),
                "Merchant DB Transactions": merchant_user_hits.get(uid, 0),
            })

    elif rule_key == "auth_with_customer_not_merchant":
        for a in auth_rows:
            uid = _extract_user_id(a)
            phone = _normalize_phone(_extract_phone(a))
            email = _normalize_email_key(a.get("email") or a.get("userName"))
            in_customer = (uid and uid in customer_users) or (phone and phone in customer_phones) or (email and email in customer_emails)
            in_merchant = (uid and uid in merchant_users) or (phone and phone in merchant_phones) or (email and email in merchant_emails)
            if not in_customer or in_merchant:
                continue
            rows.append({
                "User ID": uid or "N/A",
                "Email": _safe_text(a.get("email")) or "N/A",
                "Phone": phone or "N/A",
                "In CustomerDetails": "YES",
                "In MerchantDetails": "NO",
            })

    elif rule_key == "auth_with_merchant_not_customer":
        for a in auth_rows:
            uid = _extract_user_id(a)
            phone = _normalize_phone(_extract_phone(a))
            email = _normalize_email_key(a.get("email") or a.get("userName"))
            in_customer = (uid and uid in customer_users) or (phone and phone in customer_phones) or (email and email in customer_emails)
            in_merchant = (uid and uid in merchant_users) or (phone and phone in merchant_phones) or (email and email in merchant_emails)
            if not in_merchant or in_customer:
                continue
            rows.append({
                "User ID": uid or "N/A",
                "Email": _safe_text(a.get("email")) or "N/A",
                "Phone": phone or "N/A",
                "In CustomerDetails": "NO",
                "In MerchantDetails": "YES",
            })

    elif rule_key == "merchants_without_auth":
        auth_user_ids = {_extract_user_id(a) for a in auth_rows if _extract_user_id(a)}
        auth_phones = {_normalize_phone(_extract_phone(a)) for a in auth_rows if _normalize_phone(_extract_phone(a))}
        auth_emails = {_normalize_email_key(a.get("email") or a.get("userName")) for a in auth_rows if _normalize_email_key(a.get("email") or a.get("userName"))}

        for m in merchant_details:
            uid = _extract_user_id(m)
            phone = _normalize_phone(_extract_phone(m))
            email = _normalize_email_key(m.get("email") or m.get("userName"))
            has_auth = (uid and uid in auth_user_ids) or (phone and phone in auth_phones) or (email and email in auth_emails)
            if has_auth:
                continue
            rows.append({
                "Merchant ID": _safe_text(m.get("merchantId") or m.get("_id") or m.get("id")) or "N/A",
                "Merchant User ID": uid or "N/A",
                "Merchant Name": _safe_text(m.get("businessName") or m.get("name") or m.get("brandName")) or "N/A",
                "Merchant Email": _safe_text(m.get("email")) or "N/A",
                "Merchant Phone": phone or "N/A",
                "In Authentication": "NO",
            })

    elif rule_key == "merchant_financial_active_digital_inactive":
        merchant_tx = get_rows("numoni_merchant", "transaction_history")
        login_rows = get_rows("authentication", "login_activities")

        tx_count_by_user = defaultdict(int)
        revenue_by_user = defaultdict(float)
        merchant_id_to_user = {}
        for m in merchant_details:
            mid = _safe_text(m.get("merchantId") or m.get("_id") or m.get("id"))
            uid = _extract_user_id(m)
            if mid and uid:
                merchant_id_to_user[mid] = uid

        for tx in merchant_tx:
            uid = _extract_user_id(tx)
            if not uid:
                mid = _safe_text(tx.get("merchantId"))
                uid = merchant_id_to_user.get(mid, "")
            if not uid:
                continue
            tx_count_by_user[uid] += 1
            revenue_by_user[uid] += _extract_float(tx, ["amountPaid", "amount", "totalAmountPaid", "transactionAmount", "amountByWallet"])

        login_count_by_user = defaultdict(int)
        for lg in login_rows:
            uid = _extract_user_id(lg)
            if not uid:
                continue
            if _safe_text(lg.get("activityType")).upper() not in {"", "LOGIN"}:
                continue
            login_count_by_user[uid] += 1

        for m in merchant_details:
            uid = _extract_user_id(m)
            if not uid:
                continue
            if tx_count_by_user.get(uid, 0) <= 0:
                continue
            if login_count_by_user.get(uid, 0) > 0:
                continue
            rows.append({
                "Merchant ID": _safe_text(m.get("merchantId") or m.get("_id") or m.get("id")) or "N/A",
                "Merchant User ID": uid,
                "Merchant Name": _safe_text(m.get("businessName") or m.get("name") or m.get("brandName")) or "N/A",
                "Transaction Count": tx_count_by_user.get(uid, 0),
                "Revenue": round(revenue_by_user.get(uid, 0.0), 2),
                "Login Activity Count": login_count_by_user.get(uid, 0),
            })

    elif rule_key == "customer_digital_active_financial_inactive":
        customer_tx = get_rows("numoni_customer", "transaction_history")
        login_rows = get_rows("authentication", "login_activities")

        customer_user_to_name = {}
        customer_user_to_id = {}
        for c in customer_details:
            uid = _extract_user_id(c)
            cid = _extract_customer_id(c)
            if not uid:
                continue
            customer_user_to_id[uid] = cid or uid
            customer_user_to_name[uid] = _extract_customer_name(c) or "N/A"

        login_count_by_user = defaultdict(int)
        for lg in login_rows:
            uid = _extract_user_id(lg)
            if not uid:
                continue
            if _safe_text(lg.get("activityType")).upper() not in {"", "LOGIN"}:
                continue
            login_count_by_user[uid] += 1

        tx_count_by_user = defaultdict(int)
        for tx in customer_tx:
            uid = _extract_user_id(tx) or _safe_text(tx.get("customerUserId"))
            if not uid:
                continue
            tx_count_by_user[uid] += 1

        for uid, login_count in login_count_by_user.items():
            if login_count <= 0:
                continue
            if tx_count_by_user.get(uid, 0) > 0:
                continue
            rows.append({
                "Customer ID": customer_user_to_id.get(uid, "N/A"),
                "Customer User ID": uid,
                "Customer Name": customer_user_to_name.get(uid, "N/A"),
                "Login Activity Count": login_count,
                "Transaction Count": 0,
            })

    elif rule_key == "users_inconsistent_status":
        customer_by_user = {}
        for c in customer_details:
            uid = _extract_user_id(c)
            if uid:
                customer_by_user[uid] = c

        merchant_by_user = {}
        for m in merchant_details:
            uid = _extract_user_id(m)
            if uid:
                merchant_by_user[uid] = m

        for a in auth_rows:
            uid = _extract_user_id(a)
            phone = _normalize_phone(_extract_phone(a))
            email = _normalize_email_key(a.get("email") or a.get("userName"))

            c_row = customer_by_user.get(uid, {}) if uid else {}
            if not c_row and phone and phone in identity["customer_by_phone"]:
                cid = identity["customer_by_phone"].get(phone)
                c_row = next((c for c in customer_details if (_extract_customer_id(c) == cid or _extract_user_id(c) == cid)), {})
            if not c_row and email and email in identity["customer_by_email"]:
                cid = identity["customer_by_email"].get(email)
                c_row = next((c for c in customer_details if (_extract_customer_id(c) == cid or _extract_user_id(c) == cid)), {})

            m_row = merchant_by_user.get(uid, {}) if uid else {}
            if not m_row and phone and phone in identity["merchant_by_phone"]:
                mid = identity["merchant_by_phone"].get(phone)
                m_row = next((m for m in merchant_details if (_safe_text(m.get("merchantId")) == mid or _extract_user_id(m) == mid)), {})
            if not m_row and email and email in identity["merchant_by_email"]:
                mid = identity["merchant_by_email"].get(email)
                m_row = next((m for m in merchant_details if (_safe_text(m.get("merchantId")) == mid or _extract_user_id(m) == mid)), {})

            auth_status = _safe_text(a.get("status") or a.get("userStatus")).upper()
            customer_status = _safe_text(c_row.get("status") if c_row else "").upper()
            merchant_status = _safe_text((m_row.get("verificationStatus") or m_row.get("status")) if m_row else "").upper()

            present_systems = [
                ("AUTH", True, auth_status),
                ("CUSTOMER", bool(c_row), customer_status),
                ("MERCHANT", bool(m_row), merchant_status),
            ]
            linked_systems = [s for s in present_systems if s[1]]
            if len(linked_systems) < 2:
                continue

            non_empty_statuses = {s for _, _, s in linked_systems if s and s != "N/A"}
            has_empty_status = any((not s or s == "N/A") for _, _, s in linked_systems)
            inconsistent = (len(non_empty_statuses) > 1) or (has_empty_status and len(non_empty_statuses) >= 1)
            if not inconsistent:
                continue

            rows.append({
                "User ID": uid or "N/A",
                "Email": _safe_text(a.get("email")) or "N/A",
                "Auth Status": auth_status or "N/A",
                "Customer Status": customer_status or "N/A",
                "Merchant Status": merchant_status or "N/A",
                "Distinct Status Count": len(non_empty_statuses),
                "Missing Status In Linked System": "YES" if has_empty_status else "NO",
            })

    elif rule_key == "merchant_revenue_no_audit":
        merchant_tx = get_rows("numoni_merchant", "transaction_history")
        audit_rows = get_rows("authentication", "audit_trail")

        merchant_id_to_user = {}
        for m in merchant_details:
            mid = _safe_text(m.get("merchantId") or m.get("_id") or m.get("id"))
            uid = _extract_user_id(m)
            if mid and uid:
                merchant_id_to_user[mid] = uid

        revenue_by_user = defaultdict(float)
        tx_count_by_user = defaultdict(int)
        for tx in merchant_tx:
            uid = _extract_user_id(tx)
            if not uid:
                uid = merchant_id_to_user.get(_safe_text(tx.get("merchantId")), "")
            if not uid:
                continue
            amount = _extract_float(tx, ["amountPaid", "amount", "totalAmountPaid", "transactionAmount", "amountByWallet"])
            revenue_by_user[uid] += amount
            tx_count_by_user[uid] += 1

        audit_by_user = defaultdict(int)
        for a in audit_rows:
            role = _safe_text(a.get("userType") or a.get("type")).upper()
            if role not in {"", "MERCHANT", "UNKNOWN"}:
                continue
            uid = _extract_user_id(a)
            if uid:
                audit_by_user[uid] += 1

        for m in merchant_details:
            uid = _extract_user_id(m)
            if not uid:
                continue
            if revenue_by_user.get(uid, 0.0) <= 0:
                continue
            if audit_by_user.get(uid, 0) > 0:
                continue
            rows.append({
                "Merchant ID": _safe_text(m.get("merchantId") or m.get("_id") or m.get("id")) or "N/A",
                "Merchant User ID": uid,
                "Merchant Name": _safe_text(m.get("businessName") or m.get("name") or m.get("brandName")) or "N/A",
                "Revenue": round(revenue_by_user.get(uid, 0.0), 2),
                "Transaction Count": tx_count_by_user.get(uid, 0),
                "Audit Trail Events": 0,
            })

    return rows


def _handle_cross_db_dictionary_query(
    query_lower: str,
    load_collection_data: Callable[[str, str], List[Dict[str, Any]]],
    linker_sources: List[Tuple[str, str]] | None = None,
):
    linker_source_set = set(linker_sources or [])
    ordered_rules = list(CROSS_DB_QUERY_MAP.items())
    if linker_source_set:
        ordered_rules = sorted(
            ordered_rules,
            key=lambda kv: -len(set(tuple(x) for x in kv[1].get("sources", []) if isinstance(x, (list, tuple)) and len(x) == 2) & linker_source_set),
        )

    for rule_key, rule in ordered_rules:
        if not _cross_db_rule_matches(query_lower, rule):
            continue

        source_loader = _make_source_loader(load_collection_data, rule.get("sources", []), required_sources=[])
        rows = _eval_cross_db_rule(rule_key, query_lower, source_loader)

        return {
            "handled": True,
            "title": rule.get("title") or "Which Users (Cross DB)",
            "rows": rows,
            "target": rule.get("target", "users"),
            "single": False,
            "database": rule.get("database", "cross_db"),
            "collections": rule.get("collections", []),
            "action": rule.get("action", "list"),
            "filters": rule.get("filters", {}),
            "intent_plan": [f"cross_db.{rule_key}"],
            "fetch_columns": rule.get("columns", []),
        }
    return None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return default


def _parse_number_token(token: str) -> int:
    text = _safe_text(token).lower()
    alias_words = {
        "none": 0,
        "null": 0,
        "nil": 0,
        "no": 0,
        "once": 1,
        "single": 1,
        "twice": 2,
        "thrice": 3,
    }
    if text in alias_words:
        return alias_words[text]

    word_units = {
        "zero": 0,
        "none": 0,
        "null": 0,
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
        "eleven": 11,
        "twelve": 12,
        "thirteen": 13,
        "fourteen": 14,
        "fifteen": 15,
        "sixteen": 16,
        "seventeen": 17,
        "eighteen": 18,
        "nineteen": 19,
    }
    word_tens = {
        "twenty": 20,
        "thirty": 30,
        "forty": 40,
        "fifty": 50,
        "sixty": 60,
        "seventy": 70,
        "eighty": 80,
        "ninety": 90,
    }
    word_scales = {
        "hundred": 100,
        "thousand": 1000,
        "million": 1000000,
    }

    word_text = re.sub(r"[^a-z\s-]", " ", text).replace("-", " ")
    parts = [p for p in word_text.split() if p and p not in {"and", "time", "times"}]
    if parts:
        total = 0
        current = 0
        parsed_any = False
        invalid = False
        for part in parts:
            if part in word_units:
                current += word_units[part]
                parsed_any = True
            elif part in word_tens:
                current += word_tens[part]
                parsed_any = True
            elif part in word_scales:
                scale = word_scales[part]
                parsed_any = True
                if scale == 100:
                    current = max(current, 1) * scale
                else:
                    total += max(current, 1) * scale
                    current = 0
            else:
                invalid = True
                break
        if parsed_any and not invalid:
            return total + current

    normalized = text.replace(",", "")
    normalized = re.sub(r"(st|nd|rd|th)$", "", normalized)
    normalized = re.sub(r"x$", "", normalized)

    if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized):
        try:
            return int(float(normalized))
        except Exception:
            return -1

    m = re.search(r"-?\d+(?:\.\d+)?", normalized)
    if m:
        try:
            return int(float(m.group(0)))
        except Exception:
            return -1

    return -1


def _parse_base_count_condition(query_lower: str):
    number_phrase = r"([a-z0-9,\.\-]+(?:\s+[a-z0-9,\.\-]+){0,4})"
    patterns = [
        (rf"\b(more than|mre than|mor than|more then|over|greater than)\s+{number_phrase}(?:\s+times?)?\b", ">"),
        (rf"\b(at least|minimum of|min)\s+{number_phrase}(?:\s+times?)?\b", ">="),
        (rf"\b(less than|fewer than|under|below|lower than)\s+{number_phrase}(?:\s+times?)?\b", "<"),
        (rf"\b(at most|maximum of|max|no more than)\s+{number_phrase}(?:\s+times?)?\b", "<="),
        (rf"\b(exactly|equal to|equals?)\s+{number_phrase}(?:\s+times?)?\b", "="),
    ]
    for pattern, op in patterns:
        m = re.search(pattern, query_lower)
        if not m:
            continue
        value = _parse_number_token(m.group(2))
        if value >= 0:
            return (op, value)
    return None


def _metric_has_negation(query_lower: str, metric_terms: List[str]) -> bool:
    for term in metric_terms:
        term_rx = re.escape(term)
        if re.search(rf"\b(no|without|never|zero)\b(?:\W+\w+){{0,4}}\W+{term_rx}\b", query_lower):
            return True
        if re.search(rf"\b{term_rx}\b(?:\W+\w+){{0,4}}\W+\b(no|without|never|zero)\b", query_lower):
            return True
    return False


def _parse_metric_condition(query_lower: str, metric_terms: List[str], default_positive=(">", 0), default_negative=("=", 0)):
    if not _contains_any(query_lower, metric_terms):
        return None

    if _metric_has_negation(query_lower, metric_terms):
        return default_negative

    op_patterns = [
        (r"(more than|mre than|mor than|more then|over|greater than)", ">"),
        (r"(at least|minimum of|min)", ">="),
        (r"(less than|fewer than|under|below|lower than)", "<"),
        (r"(at most|maximum of|max|no more than)", "<="),
        (r"(exactly|equal to|equals?)", "="),
    ]

    number_phrase = r"([a-z0-9,\.\-]+(?:\s+[a-z0-9,\.\-]+){0,4})"

    for term in metric_terms:
        term_rx = re.escape(term)
        for op_rx, op in op_patterns:
            pre = re.search(rf"\b{op_rx}\s+{number_phrase}(?:\s+times?)?\b(?:\W+(?!but\b|and\b)\w+){{0,2}}\W+{term_rx}\b", query_lower)
            if pre:
                value = _parse_number_token(pre.group(2))
                if value >= 0:
                    return (op, value)
            post = re.search(rf"\b{term_rx}\b(?:\W+(?!but\b|and\b)\w+){{0,2}}\W+{op_rx}\s+{number_phrase}(?:\s+times?)?\b", query_lower)
            if post:
                value = _parse_number_token(post.group(2))
                if value >= 0:
                    return (op, value)
    return default_positive


def _matches_condition(value: float, condition) -> bool:
    if not condition:
        return True
    op, threshold = condition
    if op == ">":
        return value > threshold
    if op == ">=":
        return value >= threshold
    if op == "<":
        return value < threshold
    if op == "<=":
        return value <= threshold
    return value == threshold


def _condition_text(condition) -> str:
    if not condition:
        return "any"
    op, threshold = condition
    return f"{op}{threshold}"


def _location_key(row: Dict[str, Any]) -> str:
    parts = [
        _safe_text(row.get("address")),
        _safe_text(row.get("street")),
        _safe_text(row.get("city")),
        _safe_text(row.get("country")),
        _safe_text(row.get("postalCode")),
    ]
    parts = [p for p in parts if p]
    if parts:
        return " | ".join(parts)
    return _safe_text(row.get("storeNo"))


def _extract_oid(value: Any) -> str:
    if isinstance(value, dict):
        return _safe_text(value.get("$oid") or value.get("oid") or "")
    return _safe_text(value)


@lru_cache(maxsize=1)
def _load_collection_metadata() -> Dict[str, Dict[str, Any]]:
    base = Path(__file__).resolve().parents[1] / "part2_analysing_the_collection"
    files = [
        base / "authentication_collections_metadata.json",
        base / "numoni_merchant_collections_metadata.json",
        base / "numoni_customer_collections_metadata.json",
    ]

    combined: Dict[str, Dict[str, Any]] = {}
    for fp in files:
        try:
            if not fp.exists():
                continue
            data = json.loads(fp.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for name, meta in data.items():
                    if isinstance(meta, dict):
                        combined[name.lower()] = meta
        except Exception:
            continue
    return combined


def _metadata_fields(collection_name: str) -> set:
    metadata = _load_collection_metadata()
    meta = metadata.get(collection_name.lower(), {})
    fields = set(meta.get("fields", []) or [])
    sample_values = meta.get("sample_values", {}) or {}
    fields.update(sample_values.keys())
    return {str(f).strip() for f in fields if str(f).strip()}


def _extract_float(row: Dict[str, Any], fields: List[str]) -> float:
    for field in fields:
        value = row.get(field)
        if value is None:
            continue
        try:
            return float(str(value).replace(",", ""))
        except Exception:
            continue
    return 0.0


def _extract_date(row: Dict[str, Any], fields: List[str]):
    for field in fields:
        value = row.get(field)
        if value is None:
            continue
        try:
            if isinstance(value, dict) and "$date" in value:
                text = str(value["$date"])
                return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
            if isinstance(value, str):
                return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            continue
    return None


def handle_which_query(query: str, load_collection_data: Callable[[str, str], List[Dict[str, Any]]]) -> Dict[str, Any]:
    query_clean = re.sub(r"^[\s\-\*•\d\.)\(]+", "", query or "").strip()
    query_lower = query_clean.lower()
    if not re.match(r"^\s*which\b", query_lower):
        return {"handled": False}

    target = _extract_target(query_lower)

    linker_hint = _resolve_linker_sources(query_lower)

    has_customer_and_merchant_scope = _contains_any(query_lower, ["customer", "customers", "buyer", "client"]) and _contains_any(query_lower, ["merchant", "merchants", "business", "vendor", "store"])
    asks_cross_db = _contains_any(query_lower, ["across", "cross", "both", "either", "in either db", "in both db", "database", "databases", "systems"])
    run_cross_db_first = target in {"user", "users"} or has_customer_and_merchant_scope or asks_cross_db

    if run_cross_db_first:
        cross_db_result = _handle_cross_db_dictionary_query(
            query_lower,
            load_collection_data,
            linker_sources=linker_hint.get("sources", []),
        )
        if cross_db_result:
            if linker_hint.get("matched_tokens"):
                cross_db_result["linker_tokens"] = linker_hint.get("matched_tokens", [])[:12]
            return cross_db_result

        linker_difference_result = _handle_linker_difference_query(
            query_lower,
            load_collection_data,
            linker_sources=linker_hint.get("sources", []),
        )
        if linker_difference_result:
            if linker_hint.get("matched_tokens"):
                linker_difference_result["linker_tokens"] = linker_hint.get("matched_tokens", [])[:12]
            return linker_difference_result
    status_filter = _get_status_filter(query_lower)
    is_high, is_low = _is_highest_or_lowest(query_lower)

    domain_candidates = []
    if target in {"customer", "client", "buyer"}:
        domain_candidates.append("customer")
    if target in {"merchant", "vendor", "store", "business"}:
        domain_candidates.append("merchant")
    if target in {"deal", "offer", "promotion"}:
        domain_candidates.append("deals")
    if target in {"user", "users"}:
        domain_candidates.append("user")
    if _contains_any(query_lower, ["auth", "authentication", "login", "session", "audit"]):
        domain_candidates.append("authentication")
    if not domain_candidates:
        domain_candidates = ["merchant", "customer", "authentication"]

    fetch_plan = _resolve_fetch_plan(query_lower, domain_candidates)
    combined_sources = list(dict.fromkeys((fetch_plan.get("sources", []) or []) + (linker_hint.get("sources", []) or [])))

    get_rows = _make_source_loader(
        load_collection_data,
        combined_sources,
        required_sources=[("numoni_merchant", "transaction_history")],
    )

    merchant_tx = get_rows("numoni_merchant", "transaction_history")

    # Deal-domain WHICH/SHOW/GET queries: metadata-aware linking (deals, dealimage, merchantDetails, authuser)
    if target in {"deal", "offer", "promotion"}:
        deals = get_rows("numoni_merchant", "deals")
        deal_images = get_rows("numoni_merchant", "dealimage")
        merchant_details = get_rows("numoni_merchant", "merchantDetails")
        auth_users = get_rows("authentication", "authuser")

        deals_fields = _metadata_fields("deals")

        merchant_by_user = {}
        for row in merchant_details:
            uid = _extract_user_id(row)
            if not uid:
                continue
            merchant_by_user[uid] = {
                "Merchant ID": _safe_text(row.get("merchantId") or row.get("_id") or row.get("id")) or "N/A",
                "Merchant User ID": uid,
                "Merchant Name": _safe_text(row.get("businessName") or row.get("name") or row.get("brandName")) or "N/A",
                "Merchant Email": _safe_text(row.get("email")) or "N/A",
            }

        auth_by_user = {}
        for row in auth_users:
            uid = _extract_user_id(row)
            if uid:
                auth_by_user[uid] = row

        image_deal_ids = set()
        for img in deal_images:
            deal_id = _extract_oid(img.get("dealId"))
            if deal_id:
                image_deal_ids.add(deal_id)

        def deal_is_expired(row: Dict[str, Any]) -> bool:
            status_text = _safe_text(row.get("dealStatus") or row.get("status")).upper()
            if "EXPIRED" in status_text:
                return True
            end_date = _extract_date(row, ["endDate", "expiryDate", "updatedDt"])
            return bool(end_date and end_date < datetime.utcnow())

        def deal_is_active(row: Dict[str, Any]) -> bool:
            if "isActive" in deals_fields:
                return _to_bool(row.get("isActive"))
            return _safe_text(row.get("dealStatus") or row.get("status")).upper() in {"ACTIVE", "LIVE"}

        rows: List[Dict[str, Any]] = []
        title = "Which Deals"
        collections_used = ["deals"]
        filters_used: Dict[str, Any] = {}
        action_used = "list"

        ask_expired = _contains_any(query_lower, ["expired", "ended", "past end", "end date passed"])
        ask_active = _contains_any(query_lower, ["active", "still active", "currently active", "live"])
        ask_without_images = _contains_any(query_lower, ["without image", "without images", "no image", "missing image"])
        ask_merchant_deals = _contains_any(query_lower, ["merchant"]) and _contains_any(query_lower, ["deal"])

        for d in deals:
            deal_id = _extract_oid(d.get("_id") or d.get("dealId") or d.get("id"))
            user_id = _extract_user_id(d)

            if ask_expired and not deal_is_expired(d):
                continue
            if ask_active and not deal_is_active(d):
                continue
            if ask_without_images and deal_id and deal_id in image_deal_ids:
                continue

            row = {
                "Deal ID": deal_id or "N/A",
                "Deal Name": _safe_text(d.get("name")) or "N/A",
                "Deal Type": _safe_text(d.get("dealType")) or "N/A",
                "Deal Status": _safe_text(d.get("dealStatus") or d.get("status")).upper() or "N/A",
                "Is Active": "TRUE" if deal_is_active(d) else "FALSE",
                "End Date": _safe_text(d.get("endDate")) or "N/A",
                "Merchant User ID": user_id or "N/A",
            }

            merchant = merchant_by_user.get(user_id or "")
            if merchant:
                row.update({
                    "Merchant ID": merchant["Merchant ID"],
                    "Merchant Name": merchant["Merchant Name"],
                    "Merchant Email": merchant["Merchant Email"],
                })

            if user_id in auth_by_user:
                auth = auth_by_user[user_id]
                row.update({
                    "Auth Status": _safe_text(auth.get("status")) or "N/A",
                    "Auth Email Verified": _safe_text(auth.get("emailVerified")) or "N/A",
                })

            row["Has Deal Image"] = "NO" if (not deal_id or deal_id not in image_deal_ids) else "YES"
            rows.append(row)

        if ask_expired and ask_active:
            title = "Which Deals Are Expired But Still Active"
            filters_used = {"dealStatus": "EXPIRED", "isActive": True}
        elif ask_without_images and ask_merchant_deals:
            title = "Which Merchants Have Deals Without Images"
            filters_used = {"dealimage": "missing"}
        elif ask_without_images:
            title = "Which Deals Have No Images"
            filters_used = {"dealimage": "missing"}
        elif ask_expired:
            title = "Which Deals Are Expired"
            filters_used = {"dealStatus": "EXPIRED"}

        collections_used = ["deals"]
        if ask_without_images or ask_merchant_deals:
            collections_used.append("dealimage")
        if ask_merchant_deals:
            collections_used.append("merchantDetails")
        if _contains_any(query_lower, ["auth", "authentication", "verified"]):
            collections_used.append("authuser")

        return {
            "handled": True,
            "title": title,
            "rows": rows,
            "target": "deals",
            "single": False,
            "database": "numoni_merchant",
            "collections": collections_used,
            "action": action_used,
            "filters": filters_used,
            "intent_plan": fetch_plan["intents"],
            "fetch_columns": fetch_plan["columns"],
        }

    # Special case: customers interacted with merchants that received payouts
    if (
        ("customer" in query_lower or "customers" in query_lower)
        and "interact" in query_lower
        and "merchant" in query_lower
        and "payout" in query_lower
    ):
        payout_rows = load_collection_data("numoni_merchant", "merchant_payout") or []
        payout_collection = "merchant_payout"
        if not payout_rows:
            payout_rows = load_collection_data("numoni_merchant", "merchant_payout_initiatives") or []
            payout_collection = "merchant_payout_initiatives"

        payout_by_merchant = {}
        for row in payout_rows:
            merchant_id = str(row.get("merchantId", "")).strip()
            if not merchant_id:
                continue
            payout_by_merchant.setdefault(merchant_id, []).append(row)

        rows = []
        for tx in merchant_tx:
            merchant_id = str(tx.get("merchantId", "")).strip()
            if merchant_id not in payout_by_merchant:
                continue

            customer_id = str(tx.get("customerId", "")).strip()
            customer_name = str(tx.get("customerName", "")).strip()
            if not customer_id and not customer_name:
                continue

            payout_samples = payout_by_merchant.get(merchant_id, [])
            payout_status = "N/A"
            payout_date = "N/A"
            if payout_samples:
                sample = payout_samples[0]
                payout_status = str(sample.get("status", "N/A")).upper()
                payout_date = sample.get("payoutDate") or sample.get("createdAt") or sample.get("createdDt") or "N/A"

            rows.append({
                "Customer ID": customer_id or "N/A",
                "Customer Name": customer_name or "N/A",
                "Merchant ID": merchant_id or "N/A",
                "Merchant Name": str(tx.get("merchantName", "")).strip() or "N/A",
                "Transaction Status": str(tx.get("status", "")).upper() or "N/A",
                "Payout Status": payout_status,
                "Payout Date": payout_date,
            })

        rows = _dedupe_rows(rows, ["Customer ID", "Customer Name", "Merchant ID"])

        return {
            "handled": True,
            "title": "Customers Who Interacted With Merchants That Received Payouts",
            "rows": rows,
            "target": "customers",
            "single": False,
            "database": "numoni_merchant",
            "collections": ["transaction_history", payout_collection],
            "action": "list",
            "filters": {"relation": "customers ↔ merchants with payouts"},
        }

    # Special case: customers redeemed rewards at merchants with active deals
    if (
        "customer" in query_lower
        and "redeem" in query_lower
        and "reward" in query_lower
        and "merchant" in query_lower
        and "active deal" in query_lower
    ):
        deals = load_collection_data("numoni_merchant", "deals") or []
        merchant_details = load_collection_data("numoni_merchant", "merchantDetails") or []

        user_to_merchant = {}
        for m in merchant_details:
            user_id = _extract_user_id(m)
            merchant_id = _safe_text(m.get("merchantId") or m.get("_id") or m.get("id"))
            if user_id and merchant_id:
                user_to_merchant[user_id] = merchant_id

        active_merchant_ids = set()
        all_deal_merchants = set()
        for row in deals:
            raw_merchant_id = _safe_text(row.get("merchantId"))
            raw_user_id = _extract_user_id(row)

            merchant_id = raw_merchant_id or user_to_merchant.get(raw_user_id, "")
            if merchant_id:
                all_deal_merchants.add(merchant_id)

            if _to_bool(row.get("isActive")) or _safe_text(row.get("status")).upper() == "ACTIVE" or _safe_text(row.get("dealStatus")).upper() == "ACTIVE":
                if merchant_id:
                    active_merchant_ids.add(merchant_id)

        if not active_merchant_ids:
            active_merchant_ids = set(all_deal_merchants)

        rows = []
        for tx in merchant_tx:
            merchant_id = _safe_text(tx.get("merchantId"))
            if active_merchant_ids and (not merchant_id or merchant_id not in active_merchant_ids):
                continue

            customer_id = _extract_customer_id(tx)
            customer_name = _extract_customer_name(tx)
            if not customer_id and not customer_name:
                continue

            rows.append({
                "Customer ID": customer_id or "N/A",
                "Customer Name": customer_name or "N/A",
                "Merchant ID": merchant_id or "N/A",
                "Merchant Name": _safe_text(tx.get("merchantName")) or "N/A",
                "Transaction Status": _safe_text(tx.get("status")).upper() or "N/A",
                "Deal Active": "YES",
            })

        rows = _dedupe_rows(rows, ["Customer ID", "Customer Name", "Merchant ID"])
        return {
            "handled": True,
            "title": "Customers Redeemed Rewards At Merchants With Active Deals",
            "rows": rows,
            "target": "customers",
            "single": False,
            "database": "numoni_merchant",
            "collections": ["transaction_history", "deals", "merchantDetails"],
            "action": "list",
            "filters": {"relation": "customer ↔ merchant with active deal"},
        }

    # Special case: customers frequently share money with others
    if (
        ("customer" in query_lower or "customers" in query_lower)
        and "share money" in query_lower
    ):
        share_rows = load_collection_data("numoni_customer", "customer_share_money") or []
        customer_details = load_collection_data("numoni_customer", "customerDetails") or []

        customer_name_map = {}
        for c in customer_details:
            cid = str(c.get("customerId", "")).strip()
            name = str(c.get("name") or c.get("customerName") or c.get("userName") or "").strip()
            if cid and name:
                customer_name_map[cid] = name

        freq = {}
        for row in share_rows:
            sender_id = str(row.get("sentCustomerId", "")).strip()
            receiver_id = str(row.get("receiveCustomerId", "")).strip()
            if not sender_id:
                continue

            amount_val = 0.0
            try:
                amount_val = float(str(row.get("transactionAmount", 0)).replace(",", ""))
            except Exception:
                amount_val = 0.0

            if sender_id not in freq:
                freq[sender_id] = {
                    "Customer ID": sender_id,
                    "Customer Name": customer_name_map.get(sender_id, "N/A"),
                    "Share Count": 0,
                    "Total Shared Amount": 0.0,
                    "Recipients": set(),
                    "Statuses": set(),
                }

            freq[sender_id]["Share Count"] += 1
            freq[sender_id]["Total Shared Amount"] += amount_val
            if receiver_id:
                freq[sender_id]["Recipients"].add(receiver_id)
            status = str(row.get("status", "")).upper().strip()
            if status:
                freq[sender_id]["Statuses"].add(status)

        rows = []
        for rec in freq.values():
            rows.append({
                "Customer ID": rec["Customer ID"],
                "Customer Name": rec["Customer Name"],
                "Share Count": rec["Share Count"],
                "Unique Recipients": len(rec["Recipients"]),
                "Total Shared Amount": round(rec["Total Shared Amount"], 2),
                "Statuses": ", ".join(sorted(rec["Statuses"])) if rec["Statuses"] else "N/A",
            })

        rows = sorted(rows, key=lambda x: x["Share Count"], reverse=True)

        if is_high or is_low:
            rows = [rows[0]] if (rows and is_high) else ([rows[-1]] if rows else [])

        return {
            "handled": True,
            "title": "Customers Who Frequently Share Money With Others",
            "rows": rows,
            "target": "customers",
            "single": bool(is_high or is_low),
            "database": "numoni_customer",
            "collections": ["customer_share_money", "customerDetails"],
            "action": "top_n" if is_high else ("bottom_n" if is_low else "list"),
            "filters": {"relation": "sentCustomerId -> receiveCustomerId"},
        }

    # Customer-domain WHICH queries (use customer DB tables, not merchant transaction table)
    if target in {"customer", "client", "buyer"}:
        customer_details = get_rows("numoni_customer", "customerDetails")
        wallet_rows = get_rows("numoni_customer", "wallet")
        customer_tx = get_rows("numoni_customer", "transaction_history")
        wallet_ledger = get_rows("numoni_customer", "customer_wallet_ledger")
        pay_on_us_notifications = get_rows("numoni_customer", "pay_on_us_notifications")
        invoice_rows = get_rows("numoni_customer", "invoice")
        favourite_deal_rows = get_rows("numoni_customer", "favourite_deal")
        customer_location_rows = get_rows("numoni_customer", "customerlocation")
        auth_users = get_rows("authentication", "authuser")
        login_rows = get_rows("authentication", "login_activities")
        user_sessions = get_rows("authentication", "user_sessions")
        audit_trail_rows = get_rows("authentication", "audit_trail")

        customer_id_by_user = {}
        for row in customer_tx:
            tx_uid = _safe_text(row.get("customerUserId") or row.get("userId"))
            tx_cid = _safe_text(row.get("customerId"))
            if tx_uid and tx_cid and tx_uid not in customer_id_by_user:
                customer_id_by_user[tx_uid] = tx_cid

        for row in wallet_ledger:
            wl_uid = _safe_text(row.get("userId") or row.get("customerUserId"))
            wl_cid = _safe_text(row.get("customerId"))
            if wl_uid and wl_cid and wl_uid not in customer_id_by_user:
                customer_id_by_user[wl_uid] = wl_cid

        name_by_customer = {}
        status_by_customer = {}
        customer_ids_by_normalized_name = defaultdict(set)

        def _normalize_person_name(value: Any) -> str:
            return re.sub(r"\s+", " ", _safe_text(value).lower()).strip()

        for row in customer_details:
            uid = _extract_user_id(row)
            cid = customer_id_by_user.get(uid) or _safe_text(row.get("customerId")) or _extract_customer_id(row)
            if not cid:
                continue
            cname = _extract_customer_name(row) or "N/A"
            name_by_customer[cid] = cname
            status_by_customer[cid] = _safe_text(row.get("status")).upper()
            normalized_name = _normalize_person_name(cname)
            if normalized_name:
                customer_ids_by_normalized_name[normalized_name].add(cid)

        wallet_balance_by_customer = {}
        for row in wallet_rows:
            cid = _extract_customer_id(row)
            if not cid:
                continue
            bal = _extract_float(row, ["balance", "amount", "walletBalance", "availableBalance"])
            wallet_balance_by_customer[cid] = bal

        tx_by_customer = defaultdict(list)
        tx_total_amount_by_customer = defaultdict(float)
        tx_merchant_ids_by_customer = defaultdict(set)
        tx_invoice_refs_by_customer = defaultdict(set)
        for row in customer_tx:
            cid = _extract_customer_id(row)
            if not cid:
                continue
            tx_by_customer[cid].append(row)
            tx_total_amount_by_customer[cid] += _extract_float(row, ["transactionAmount", "amount", "totalAmountPaid", "totalAmount", "amountPaid", "amountByWallet"])

            merchant_id = _safe_text(row.get("merchantId") or row.get("receiveMerchantId") or row.get("sentMerchantId") or row.get("businessId") or row.get("vendorId") or row.get("storeId"))
            if merchant_id:
                tx_merchant_ids_by_customer[cid].add(merchant_id)

            for invoice_key in ["invoiceId", "invoiceNo", "invoiceNumber", "invoiceRef", "invoiceReference", "referenceId", "referenceNumber"]:
                invoice_ref = _safe_text(row.get(invoice_key))
                if invoice_ref:
                    tx_invoice_refs_by_customer[cid].add(invoice_ref)

        ledger_count = defaultdict(int)
        wallet_ledger_latest_dt_by_customer = {}
        for row in wallet_ledger:
            cid = _extract_customer_id(row)
            if cid:
                ledger_count[cid] += 1
                dt = _extract_date(row, ["createdDt", "createdDate", "transactionDate", "date", "entryDate", "updatedDt"])
                if dt and (cid not in wallet_ledger_latest_dt_by_customer or dt > wallet_ledger_latest_dt_by_customer[cid]):
                    wallet_ledger_latest_dt_by_customer[cid] = dt

        customer_by_user = {}
        customer_by_phone = {}
        customer_by_email = {}
        for row in customer_details:
            uid = _extract_user_id(row)
            cid = customer_id_by_user.get(uid) or _safe_text(row.get("customerId")) or _extract_customer_id(row)
            if not cid:
                continue
            ph = _extract_phone(row)
            em = _normalize_email(row.get("email") or row.get("userName"))
            if uid:
                customer_by_user[uid] = cid
            if ph:
                customer_by_phone[ph] = cid
            if em:
                customer_by_email[em] = cid

        notification_count_by_customer = defaultdict(int)
        for row in pay_on_us_notifications:
            cid = _safe_text(row.get("customerId") or row.get("customerUserId"))
            if not cid:
                uid = _extract_user_id(row)
                if uid and uid in customer_by_user:
                    cid = customer_by_user[uid]
            if not cid:
                sender = row.get("senderDetails") if isinstance(row.get("senderDetails"), dict) else {}
                sender_name = _normalize_person_name(sender.get("name"))
                sender_candidates = customer_ids_by_normalized_name.get(sender_name, set())
                if len(sender_candidates) == 1:
                    cid = next(iter(sender_candidates))
            if cid:
                notification_count_by_customer[cid] += 1

        invoice_count_by_customer = defaultdict(int)
        invoice_refs_by_customer = defaultdict(set)
        for row in invoice_rows:
            cid = _extract_customer_id(row)
            if not cid:
                uid = _extract_user_id(row)
                if uid and uid in customer_by_user:
                    cid = customer_by_user[uid]
            if not cid:
                continue
            invoice_count_by_customer[cid] += 1
            for invoice_key in ["invoiceId", "invoiceNo", "invoiceNumber", "invoiceRef", "referenceId", "referenceNumber", "_id", "id"]:
                inv_ref = _extract_oid(row.get(invoice_key)) if invoice_key in {"_id", "id"} else _safe_text(row.get(invoice_key))
                if inv_ref:
                    invoice_refs_by_customer[cid].add(inv_ref)

        favourite_merchant_ids_by_customer = defaultdict(set)
        favourite_deal_count_by_customer = defaultdict(int)
        for row in favourite_deal_rows:
            cid = _extract_customer_id(row)
            if not cid:
                uid = _extract_user_id(row)
                if uid and uid in customer_by_user:
                    cid = customer_by_user[uid]
            if not cid:
                continue
            favourite_deal_count_by_customer[cid] += 1
            merchant_id = _safe_text(row.get("merchantId") or row.get("businessId") or row.get("vendorId") or row.get("storeId"))
            if merchant_id:
                favourite_merchant_ids_by_customer[cid].add(merchant_id)

        latest_location_change_by_customer = {}
        for row in customer_location_rows:
            cid = _extract_customer_id(row)
            if not cid:
                uid = _extract_user_id(row)
                if uid and uid in customer_by_user:
                    cid = customer_by_user[uid]
            if not cid:
                continue
            dt = _extract_date(row, ["updatedDt", "createdDt", "changedDt", "modifiedDt", "createdDate", "updatedDate", "date"])
            if not dt:
                continue
            if cid not in latest_location_change_by_customer or dt > latest_location_change_by_customer[cid]:
                latest_location_change_by_customer[cid] = dt

        auth_by_customer = {}
        deleted_auth_by_customer = {}
        for a in auth_users:
            auth_user_id = _extract_user_id(a)
            auth_phone = _extract_phone(a)
            auth_email = _normalize_email(a.get("email") or a.get("userName"))

            cid = ""
            if auth_user_id and auth_user_id in customer_by_user:
                cid = customer_by_user[auth_user_id]
            elif auth_phone and auth_phone in customer_by_phone:
                cid = customer_by_phone[auth_phone]
            elif auth_email and auth_email in customer_by_email:
                cid = customer_by_email[auth_email]

            if not cid:
                continue
            if cid not in auth_by_customer:
                auth_by_customer[cid] = a

            auth_deleted = str(a.get("isDeleted", "0")).strip() in {"1", "true", "True"}
            if auth_deleted:
                deleted_auth_by_customer[cid] = a

        login_count_by_customer = defaultdict(int)
        last_login_by_customer = {}
        for lg in login_rows:
            uid = _extract_user_id(lg)
            if not uid:
                continue
            if _safe_text(lg.get("activityType")).upper() not in {"", "LOGIN"}:
                continue

            cid = customer_by_user.get(uid)
            if not cid:
                continue

            login_count_by_customer[cid] += 1
            dt = _extract_date(lg, ["activityTime", "createdDt", "createdTime", "date"])
            if dt and (cid not in last_login_by_customer or dt > last_login_by_customer[cid]):
                last_login_by_customer[cid] = dt

        active_session_count_by_customer = defaultdict(int)
        for s in user_sessions:
            uid = _extract_user_id(s)
            if not uid:
                continue
            cid = customer_by_user.get(uid)
            if not cid:
                continue
            if _to_bool(s.get("isActive")):
                active_session_count_by_customer[cid] += 1

        audit_login_count_by_customer = defaultdict(int)
        for a in audit_trail_rows:
            role = _safe_text(a.get("userType") or a.get("type")).upper()
            if role not in {"", "CUSTOMER", "UNKNOWN"}:
                continue
            action = _safe_text(a.get("action")).upper()
            if action and action != "LOGIN":
                continue

            uid = _extract_user_id(a)
            cid = ""
            if uid and uid in customer_by_user:
                cid = customer_by_user[uid]
            else:
                created_by = _normalize_email(a.get("createdBy") or a.get("userName"))
                if created_by and created_by in customer_by_email:
                    cid = customer_by_email[created_by]

            if cid:
                audit_login_count_by_customer[cid] += 1

        today = datetime.utcnow().date()
        recent_cutoff = datetime.utcnow() - timedelta(days=30)

        rows = []
        filters_used = {}
        collections_used = ["customerDetails", "wallet", "transaction_history", "customer_wallet_ledger"]
        action_used = "list"
        title_used = "Which Customers"

        login_condition = _parse_metric_condition(query_lower, ["logged in", "login", "login activity", "login records", "logins"], default_positive=(">", 0), default_negative=("=", 0))
        tx_condition = _parse_metric_condition(query_lower, ["transactions", "transaction", "transaction history", "transacted"], default_positive=(">", 0), default_negative=("=", 0))
        session_condition = _parse_metric_condition(query_lower, ["active sessions", "active session", "sessions", "session"], default_positive=(">", 0), default_negative=("=", 0))
        audit_condition = _parse_metric_condition(query_lower, ["audit", "audit trail", "audit login", "audit trail login"], default_positive=(">", 0), default_negative=("=", 0))
        wallet_condition = _parse_metric_condition(query_lower, ["wallet balance", "wallet", "balance"], default_positive=(">", 0), default_negative=("=", 0))

        # Customer auth/login/session/audit cross-db intents
        if _contains_any(query_lower, ["wallet balance", "wallet"]) and _contains_any(query_lower, ["greater than", "more than", "higher than", "above", "exceed"]) and _contains_any(query_lower, ["total transaction amount", "transaction amount", "total transactions", "transaction total"]):
            collections_used = ["customerDetails", "wallet", "transaction_history"]
            filters_used = {"wallet_balance": "> total_transaction_amount"}
            title_used = "Which Customers Have Wallet Balance Greater Than Their Total Transaction Amount"

            for cid, balance in wallet_balance_by_customer.items():
                total_tx_amount = tx_total_amount_by_customer.get(cid, 0.0)
                if balance <= total_tx_amount:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Wallet Balance": round(balance, 2),
                    "Total Transaction Amount": round(total_tx_amount, 2),
                    "Difference": round(balance - total_tx_amount, 2),
                })

        elif _contains_any(query_lower, ["transaction history", "transactions", "transaction"]) and _contains_any(query_lower, ["no ledger", "without ledger", "no ledger entries", "without ledger entries", "no wallet ledger"]):
            collections_used = ["customerDetails", "transaction_history", "customer_wallet_ledger"]
            filters_used = {"transactions": ">0", "ledger_entries": 0}
            title_used = "Which Customers Have Transaction History But No Ledger Entries"

            for cid, txs in tx_by_customer.items():
                tx_count = len(txs)
                if tx_count <= 0:
                    continue
                if ledger_count.get(cid, 0) > 0:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Transaction Count": tx_count,
                    "Wallet Ledger Entries": 0,
                })

        elif _contains_any(query_lower, ["notification", "notifications"]) and _contains_any(query_lower, ["never transacted", "no transaction", "without transaction", "zero transaction"]):
            collections_used = ["customerDetails", "pay_on_us_notifications", "transaction_history"]
            filters_used = {"notifications": ">0", "transactions": 0}
            title_used = "Which Customers Received Notifications But Never Transacted"

            for cid, notif_count in notification_count_by_customer.items():
                if notif_count <= 0:
                    continue
                if len(tx_by_customer.get(cid, [])) > 0:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Notification Count": notif_count,
                    "Transaction Count": 0,
                })

        elif _contains_any(query_lower, ["invoice", "invoices"]) and _contains_any(query_lower, ["no transactions linked", "no transaction linked", "without linked transaction", "not linked to transactions", "no transactions"]):
            collections_used = ["customerDetails", "invoice", "transaction_history"]
            filters_used = {"invoices": ">0", "linked_transactions": 0}
            title_used = "Which Customers Created Invoices But Have No Transactions Linked To Them"

            for cid, inv_count in invoice_count_by_customer.items():
                if inv_count <= 0:
                    continue
                invoice_refs = invoice_refs_by_customer.get(cid, set())
                tx_refs = tx_invoice_refs_by_customer.get(cid, set())
                linked_count = len(invoice_refs & tx_refs) if invoice_refs else 0
                if linked_count > 0:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Invoice Count": inv_count,
                    "Linked Transaction Count": linked_count,
                })

        elif _contains_any(query_lower, ["favourite", "favorite"]) and _contains_any(query_lower, ["deal", "deals"]) and _contains_any(query_lower, ["never purchased", "no purchase", "without purchase", "never bought"]):
            collections_used = ["customerDetails", "favourite_deal", "transaction_history"]
            filters_used = {"favourite_deals": ">0", "purchases_from_favourite_merchant": 0}
            title_used = "Which Customers Marked Deals As Favourite But Never Purchased From That Merchant"

            for cid, fav_count in favourite_deal_count_by_customer.items():
                if fav_count <= 0:
                    continue
                fav_merchants = favourite_merchant_ids_by_customer.get(cid, set())
                if not fav_merchants:
                    continue
                purchased_merchants = tx_merchant_ids_by_customer.get(cid, set())
                overlap = fav_merchants & purchased_merchants
                if overlap:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Favourite Deal Count": fav_count,
                    "Favourite Merchant Count": len(fav_merchants),
                    "Purchased From Favourite Merchant": "NO",
                })

        elif _contains_any(query_lower, ["changed", "moved", "relocated"]) and _contains_any(query_lower, ["location"]) and _contains_any(query_lower, ["no wallet activity", "without wallet activity", "no wallet ledger", "no wallet entries", "had no wallet activity afterward", "no wallet activity afterward"]):
            collections_used = ["customerDetails", "customerlocation", "customer_wallet_ledger"]
            filters_used = {"location_changed": True, "wallet_activity_after_change": 0}
            title_used = "Which Customers Changed Their Location But Had No Wallet Activity Afterward"

            for cid, location_change_dt in latest_location_change_by_customer.items():
                latest_wallet_dt = wallet_ledger_latest_dt_by_customer.get(cid)
                has_wallet_after_change = bool(latest_wallet_dt and latest_wallet_dt > location_change_dt)
                if has_wallet_after_change:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Latest Location Change": location_change_dt.isoformat(sep=" ", timespec="seconds"),
                    "Latest Wallet Activity": latest_wallet_dt.isoformat(sep=" ", timespec="seconds") if latest_wallet_dt else "N/A",
                    "Wallet Activity After Change": "NO",
                })

        elif _contains_any(query_lower, ["login activity", "login activities", "login"]) and _contains_any(query_lower, ["no wallet", "without wallet"]):
            collections_used = ["customerDetails", "login_activities", "wallet"]
            filters_used = {"login_activity": _condition_text(login_condition), "wallet": "missing"}
            title_used = "Which Customers Have Login Activity But No Wallet"

            for cid, count in login_count_by_customer.items():
                if not _matches_condition(count, login_condition):
                    continue
                if cid in wallet_balance_by_customer:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Login Activity Count": count,
                    "Last Login": last_login_by_customer[cid].isoformat(sep=" ", timespec="seconds") if cid in last_login_by_customer else "N/A",
                    "Has Wallet": "NO",
                })

        elif _contains_any(query_lower, ["authentication records", "auth records", "authentication account", "authentication accounts", "auth account", "auth accounts"]) and _contains_any(query_lower, ["logged in", "login", "login activity", "login records", "logins"]):
            collections_used = ["customerDetails", "authuser", "login_activities"]
            filters_used = {"auth_record": "exists", "login_records": _condition_text(login_condition)}
            title_used = "Which Customers Have Authentication Records By Login Activity"

            if login_condition == ("=", 0):
                title_used = "Which Customers Have Authentication Records But Never Logged In"
            elif login_condition:
                title_used = "Which Customers Have Authentication Records And Login Activity Matching Condition"

            for cid, auth in auth_by_customer.items():
                login_count = login_count_by_customer.get(cid, 0)
                if not _matches_condition(login_count, login_condition):
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Auth Email": _safe_text(auth.get("email")) or "N/A",
                    "Auth Status": _safe_text(auth.get("status")) or "N/A",
                    "Login Records": login_count,
                })

        elif _contains_any(query_lower, ["active sessions", "active session"]) and _contains_any(query_lower, ["transactions", "transaction", "transaction history"]):
            collections_used = ["customerDetails", "user_sessions", "transaction_history"]
            filters_used = {"active_sessions": _condition_text(session_condition), "transactions": _condition_text(tx_condition)}
            title_used = "Which Customers Have Active Sessions But Zero Transactions"

            for cid, session_count in active_session_count_by_customer.items():
                if not _matches_condition(session_count, session_condition):
                    continue
                tx_count = len(tx_by_customer.get(cid, []))
                if not _matches_condition(tx_count, tx_condition):
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Active Sessions": session_count,
                    "Transaction Count": tx_count,
                })

        elif _contains_any(query_lower, ["deleted in authentication", "deleted in auth", "authentication deleted", "auth deleted"]) and _contains_any(query_lower, ["wallet balance", "still have wallet", "has wallet"]):
            collections_used = ["customerDetails", "authuser", "wallet"]
            filters_used = {"auth_deleted": True, "wallet_balance": _condition_text(wallet_condition)}
            title_used = "Which Customers Were Deleted In Authentication But Still Have Wallet Balance"

            for cid, auth in deleted_auth_by_customer.items():
                balance = wallet_balance_by_customer.get(cid, 0.0)
                if not _matches_condition(balance, wallet_condition):
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Auth IsDeleted": _safe_text(auth.get("isDeleted")) or "N/A",
                    "Wallet Balance": round(balance, 2),
                })

        elif _contains_any(query_lower, ["audit trail", "audit", "audit trail login", "audit login"]) and _contains_any(query_lower, ["transactions", "transaction", "transaction history", "transacted"]):
            collections_used = ["customerDetails", "audit_trail", "transaction_history"]
            filters_used = {"audit_login_events": _condition_text(audit_condition), "transaction_history": _condition_text(tx_condition)}
            title_used = "Which Customers Triggered Audit Trail Login Events But Have No Transaction History"

            for cid, audit_count in audit_login_count_by_customer.items():
                if not _matches_condition(audit_count, audit_condition):
                    continue
                tx_count = len(tx_by_customer.get(cid, []))
                if not _matches_condition(tx_count, tx_condition):
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Audit Login Events": audit_count,
                    "Transaction Count": tx_count,
                })

        # which customers exist in customer DB but not in authentication
        elif _contains_any(query_lower, ["not in authentication", "missing in authentication", "without authentication"]) and _contains_any(query_lower, ["customer db", "customer", "customers"]):
            collections_used = ["customerDetails", "authuser"]
            filters_used = {"customer_db": "exists", "auth_record": "missing"}
            title_used = "Which Customers Exist In Customer DB But Not In Authentication"

            auth_user_ids = set()
            auth_phones = set()
            auth_emails = set()
            for a in auth_users:
                uid = _extract_user_id(a)
                phone = _extract_phone(a)
                email = _normalize_email(a.get("email"))
                if uid:
                    auth_user_ids.add(uid)
                if phone:
                    auth_phones.add(phone)
                if email:
                    auth_emails.add(email)

            for c in customer_details:
                cid = _extract_customer_id(c)
                uid = _extract_user_id(c)
                phone = _extract_phone(c)
                email = _normalize_email(c.get("email"))
                matched = (uid and uid in auth_user_ids) or (phone and phone in auth_phones) or (email and email in auth_emails)
                if matched:
                    continue
                rows.append({
                    "Customer ID": cid or "N/A",
                    "Customer User ID": uid or "N/A",
                    "Customer Name": _extract_customer_name(c) or "N/A",
                    "Customer Email": _safe_text(c.get("email")) or "N/A",
                    "Customer Phone": phone or "N/A",
                    "In Authentication": "NO",
                })

        # which customers have auth records but no wallet
        elif "auth records" in query_lower and "no wallet" in query_lower:
            collections_used = ["customerDetails", "wallet", "authuser"]
            filters_used = {"auth_records": "exists", "wallet": "none"}

            customer_by_user = {}
            customer_by_phone = {}
            customer_by_email = {}
            for row in customer_details:
                cid = _extract_customer_id(row)
                if not cid:
                    continue
                customer_by_user[_extract_user_id(row)] = cid
                phone = _extract_phone(row)
                email = _extract_email(row).lower()
                if phone:
                    customer_by_phone[phone] = cid
                if email:
                    customer_by_email[email] = cid

            for a in auth_users:
                auth_user_id = _extract_user_id(a)
                auth_phone = _extract_phone(a)
                auth_email = _extract_email(a).lower()

                cid = ""
                if auth_user_id and auth_user_id in customer_by_user:
                    cid = customer_by_user[auth_user_id]
                elif auth_phone and auth_phone in customer_by_phone:
                    cid = customer_by_phone[auth_phone]
                elif auth_email and auth_email in customer_by_email:
                    cid = customer_by_email[auth_email]

                if not cid:
                    continue
                if cid in wallet_balance_by_customer:
                    continue

                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Auth User ID": auth_user_id or "N/A",
                    "Auth Email": _safe_text(a.get("email")) or "N/A",
                    "Auth Phone": auth_phone or "N/A",
                    "Account Locked": _safe_text(a.get("accountLocked")) or "N/A",
                    "Email Verified": _safe_text(a.get("emailVerified")) or "N/A",
                    "Has Wallet": "NO",
                })

        elif "logged in" in query_lower and "never transacted" in query_lower:
            collections_used = ["customerDetails", "transaction_history", "login_activities", "authuser"]
            filters_used = {"login_records": "exists", "transactions": "none"}

            for cid, count in login_count_by_customer.items():
                if cid in tx_by_customer and len(tx_by_customer[cid]) > 0:
                    continue
                auth_row = auth_by_customer.get(cid, {})
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Login Count": count,
                    "Last Login": last_login_by_customer[cid].isoformat(sep=" ", timespec="seconds") if cid in last_login_by_customer else "N/A",
                    "Auth Email": _safe_text(auth_row.get("email")) or "N/A",
                })

        # which customers have failed transactions
        elif "failed" in query_lower and "transaction" in query_lower:
            filters_used = {"status": "FAILED"}
            for cid, txs in tx_by_customer.items():
                failed = [t for t in txs if _safe_text(t.get("status")).upper() == "FAILED"]
                if not failed:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Failed Transactions": len(failed),
                    "Last Transaction Status": _safe_text(failed[-1].get("status")).upper() or "N/A",
                    "Latest Failed Ref": _safe_text(failed[-1].get("transactionReferenceId") or failed[-1].get("transactionId") or failed[-1].get("transactionNo")) or "N/A",
                })

        # which customers have a wallet but no transactions
        elif "wallet" in query_lower and ("no transactions" in query_lower or "no transaction" in query_lower):
            filters_used = {"wallet": "exists", "transactions": "none"}
            for cid, bal in wallet_balance_by_customer.items():
                if cid in tx_by_customer and len(tx_by_customer[cid]) > 0:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Wallet Balance": bal,
                    "Transaction Count": 0,
                })

        # which customers have transactions but zero wallet balance
        elif "transaction" in query_lower and "zero wallet balance" in query_lower:
            filters_used = {"transactions": "exists", "wallet_balance": 0}
            for cid, txs in tx_by_customer.items():
                bal = wallet_balance_by_customer.get(cid, 0.0)
                if abs(bal) > 1e-9:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Transaction Count": len(txs),
                    "Wallet Balance": bal,
                })

        # which customers made transactions today
        elif "transaction" in query_lower and "today" in query_lower:
            filters_used = {"transaction_date": "today"}
            for cid, txs in tx_by_customer.items():
                today_txs = [t for t in txs if (_extract_date(t, ["transactionDate", "date", "createdDate", "createdDt"]) and _extract_date(t, ["transactionDate", "date", "createdDate", "createdDt"]).date() == today)]
                if not today_txs:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Transactions Today": len(today_txs),
                })

        # which customers received refunds
        elif "refund" in query_lower:
            filters_used = {"transaction_type": "refund"}
            for cid, txs in tx_by_customer.items():
                refunds = []
                for t in txs:
                    text = " ".join([
                        _safe_text(t.get("transactionCategory")),
                        _safe_text(t.get("operationType")),
                        _safe_text(t.get("title")),
                        _safe_text(t.get("description")),
                    ]).lower()
                    if "refund" in text:
                        refunds.append(t)
                if refunds:
                    rows.append({
                        "Customer ID": cid,
                        "Customer Name": name_by_customer.get(cid, "N/A"),
                        "Refund Transactions": len(refunds),
                    })

        # which customers never made any transaction
        elif "never" in query_lower and "transaction" in query_lower:
            filters_used = {"transactions": "none"}
            for cid in name_by_customer.keys():
                if cid in tx_by_customer and len(tx_by_customer[cid]) > 0:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Transaction Count": 0,
                })

        # which customers have wallet activity but are inactive
        elif "wallet activity" in query_lower and "inactive" in query_lower:
            filters_used = {"wallet_activity": ">0", "status": "INACTIVE"}
            for cid, count in ledger_count.items():
                if count <= 0:
                    continue
                status = status_by_customer.get(cid, "")
                if status != "INACTIVE":
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Wallet Ledger Entries": count,
                    "Customer Status": status,
                })

        # which customers made the highest total spend
        elif "highest" in query_lower and ("total spend" in query_lower or "spend" in query_lower):
            filters_used = {"metric": "total_spend"}
            action_used = "top_n"
            spend_rows = []
            for cid, txs in tx_by_customer.items():
                total = 0.0
                for t in txs:
                    total += _extract_float(t, ["totalAmountPaid", "transactionAmount", "amount", "totalAmount"])
                if total > 0:
                    spend_rows.append({
                        "Customer ID": cid,
                        "Customer Name": name_by_customer.get(cid, "N/A"),
                        "Total Spend": round(total, 2),
                        "Transaction Count": len(txs),
                    })
            spend_rows = sorted(spend_rows, key=lambda x: x["Total Spend"], reverse=True)
            rows = spend_rows[:1] if spend_rows else []

        # which customers have multiple wallet ledger entries
        elif "multiple" in query_lower and "wallet ledger" in query_lower:
            filters_used = {"wallet_ledger_entries": ">1"}
            for cid, count in ledger_count.items():
                if count <= 1:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Wallet Ledger Entries": count,
                })

        # which customers have wallet balance but no recent transactions
        elif "wallet balance" in query_lower and "no recent transactions" in query_lower:
            filters_used = {"wallet_balance": ">0", "recent_transactions": "none_30_days"}
            for cid, bal in wallet_balance_by_customer.items():
                if bal <= 0:
                    continue
                txs = tx_by_customer.get(cid, [])
                has_recent = False
                for t in txs:
                    tx_date = _extract_date(t, ["transactionDate", "date", "createdDate", "createdDt"])
                    if tx_date and tx_date >= recent_cutoff:
                        has_recent = True
                        break
                if has_recent:
                    continue
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Wallet Balance": round(bal, 2),
                    "Recent Transactions (30d)": 0,
                })

        # fallback for other customer WHICH queries: keep existing behavior but use customer tx first
        else:
            for cid, txs in tx_by_customer.items():
                rows.append({
                    "Customer ID": cid,
                    "Customer Name": name_by_customer.get(cid, "N/A"),
                    "Transaction Count": len(txs),
                })

        if is_high or is_low:
            # For generic highest/lowest customer queries when rows are list-like
            if rows and "Total Spend" not in rows[0]:
                rows = sorted(rows, key=lambda x: x.get("Transaction Count", 0), reverse=is_high)
                rows = rows[:1]
                action_used = "top_n" if is_high else "bottom_n"

        return {
            "handled": True,
            "title": title_used,
            "rows": rows,
            "target": "customers",
            "single": bool(is_high or is_low),
            "database": "numoni_customer",
            "collections": collections_used,
            "action": action_used,
            "filters": filters_used,
            "intent_plan": fetch_plan["intents"],
            "fetch_columns": fetch_plan["columns"],
        }

    # User-domain WHICH queries: role overlap across systems
    if target in {"user", "users"} and _contains_any(query_lower, ["role", "roles"]):
        auth_users = get_rows("authentication", "authuser")
        audit_trail = get_rows("authentication", "audit_trail")
        customer_details = get_rows("numoni_customer", "customerDetails")
        merchant_details = get_rows("numoni_merchant", "merchantDetails")

        auth_fields = _metadata_fields("authuser")

        customer_user_ids = {_extract_user_id(c) for c in customer_details if _extract_user_id(c)}
        merchant_user_ids = {_extract_user_id(m) for m in merchant_details if _extract_user_id(m)}

        customer_emails = {_normalize_email(c.get("email") or c.get("userName")) for c in customer_details if _normalize_email(c.get("email") or c.get("userName"))}
        merchant_emails = {_normalize_email(m.get("email") or m.get("userName")) for m in merchant_details if _normalize_email(m.get("email") or m.get("userName"))}
        customer_phones = {_extract_phone(c) for c in customer_details if _extract_phone(c)}
        merchant_phones = {_extract_phone(m) for m in merchant_details if _extract_phone(m)}

        roles_by_identity = defaultdict(set)
        for row in audit_trail:
            uname = _normalize_email(row.get("userName") or row.get("createdBy"))
            if not uname:
                continue
            role = _safe_text(row.get("userType") or row.get("type")).upper()
            if role and role != "UNKNOWN":
                roles_by_identity[uname].add(role)

        auth_by_email = {}
        for a in auth_users:
            key = _normalize_email(a.get("email") or a.get("userName"))
            if key and key not in auth_by_email:
                auth_by_email[key] = a

        rows = []
        seen_identity = set()
        for a in auth_users:
            uid = _extract_user_id(a)
            if not uid:
                continue

            auth_email = _normalize_email(a.get("email") or a.get("userName"))
            auth_phone = _extract_phone(a)

            all_roles = set()
            user_type = _safe_text(a.get("userType")).upper()
            if user_type:
                all_roles.add(user_type)

            if "roles" in auth_fields and isinstance(a.get("roles"), list):
                for role in a.get("roles", []):
                    role_text = _safe_text(role).upper()
                    if role_text:
                        all_roles.add(role_text.replace("ROLE_", ""))

            if auth_email and auth_email in roles_by_identity:
                all_roles.update(roles_by_identity[auth_email])

            in_customer = (uid in customer_user_ids) or (auth_email and auth_email in customer_emails) or (auth_phone and auth_phone in customer_phones)
            in_merchant = (uid in merchant_user_ids) or (auth_email and auth_email in merchant_emails) or (auth_phone and auth_phone in merchant_phones)

            if in_customer:
                all_roles.add("CUSTOMER")
            if in_merchant:
                all_roles.add("MERCHANT")

            normalized_roles = {r for r in all_roles if r and r != "UNKNOWN"}
            if len(normalized_roles) <= 1:
                continue

            if auth_email:
                seen_identity.add(auth_email)

            rows.append({
                "User ID": uid,
                "User Name": _safe_text(a.get("name")) or "N/A",
                "Email": _safe_text(a.get("email")) or "N/A",
                "Phone": auth_phone or "N/A",
                "Roles": ", ".join(sorted(normalized_roles)),
                "Role Count": len(normalized_roles),
                "In Customer DB": "YES" if in_customer else "NO",
                "In Merchant DB": "YES" if in_merchant else "NO",
            })

        for identity, role_set in roles_by_identity.items():
            normalized_roles = {r for r in role_set if r and r != "UNKNOWN"}
            if len(normalized_roles) <= 1:
                continue
            if identity in seen_identity:
                continue

            auth = auth_by_email.get(identity, {})
            phone = _extract_phone(auth)
            in_customer = identity in customer_emails or (phone and phone in customer_phones)
            in_merchant = identity in merchant_emails or (phone and phone in merchant_phones)

            rows.append({
                "User ID": _extract_user_id(auth) or "N/A",
                "User Name": _safe_text(auth.get("name")) or "N/A",
                "Email": identity,
                "Phone": phone or "N/A",
                "Roles": ", ".join(sorted(normalized_roles)),
                "Role Count": len(normalized_roles),
                "In Customer DB": "YES" if in_customer else "NO",
                "In Merchant DB": "YES" if in_merchant else "NO",
            })

        rows = sorted(rows, key=lambda x: x["Role Count"], reverse=True)

        return {
            "handled": True,
            "title": "Which Users Have Multiple Roles Across Systems",
            "rows": rows,
            "target": "users",
            "single": False,
            "database": "authentication",
            "collections": ["authuser", "audit_trail", "customerDetails", "merchantDetails"],
            "action": "list",
            "filters": {"role_count": ">1"},
            "intent_plan": fetch_plan["intents"],
            "fetch_columns": fetch_plan["columns"],
        }

    # Merchant-domain WHICH queries (location/auth/login/transaction joins)
    if target in {"merchant", "vendor", "store", "business"}:
        merchant_details = get_rows("numoni_merchant", "merchantDetails")
        merchant_locations = get_rows("numoni_merchant", "merchantlocation")
        auth_users = get_rows("authentication", "authuser")
        login_rows = get_rows("authentication", "login_activities")
        user_sessions = get_rows("authentication", "user_sessions")
        audit_trail_rows = get_rows("authentication", "audit_trail")
        merchant_wallet = get_rows("numoni_merchant", "wallet")
        merchant_payout = get_rows("numoni_merchant", "merchant_payout")
        business_images = get_rows("numoni_merchant", "businessimage")
        deals_rows = get_rows("numoni_merchant", "deals")
        deal_images = get_rows("numoni_merchant", "dealimage")
        notifications_rows = get_rows("numoni_merchant", "notifications")
        merchant_wallet_ledger = get_rows("numoni_merchant", "merchant_wallet_ledger")

        merchants = []
        merchant_by_user = {}
        merchant_by_id = {}
        merchant_by_email = {}
        merchant_by_phone = {}
        merchant_by_key = {}

        for idx, m in enumerate(merchant_details):
            merchant_id = _safe_text(m.get("merchantId") or m.get("_id") or m.get("id"))
            user_id = _extract_user_id(m)
            email = _normalize_email(m.get("email"))
            phone = _extract_phone(m)
            key = user_id or merchant_id or email or f"merchant_{idx}"

            data = {
                "key": key,
                "merchant_id": merchant_id or "N/A",
                "user_id": user_id or "N/A",
                "name": _safe_text(m.get("businessName") or m.get("name") or m.get("brandName")) or "N/A",
                "email": _safe_text(m.get("email")) or "N/A",
                "email_norm": email,
                "phone": phone or "N/A",
                "verification": _safe_text(m.get("verificationStatus")).upper() or "UNSPECIFIED",
                "reg_level": _safe_int(m.get("regLevel"), 0),
                "registered": _to_bool(m.get("registeredBusiness")),
                "is_deleted": str(m.get("isDeleted", "0")).strip() in {"1", "true", "True"},
                "created": _extract_date(m, ["createdDt", "createdDate", "updatedDt"]),
            }
            merchants.append(data)
            merchant_by_key[key] = data

            if user_id:
                merchant_by_user[user_id] = key
            if merchant_id:
                merchant_by_id[merchant_id] = key
            if email:
                merchant_by_email[email] = key
            if phone and phone != "N/A":
                merchant_by_phone[phone] = key

        auth_by_merchant_key = {}
        for a in auth_users:
            auth_user_type = _safe_text(a.get("userType")).upper()
            if auth_user_type not in {"", "MERCHANT", "UNKNOWN"}:
                continue

            au_user = _extract_user_id(a)
            au_email_norm = _normalize_email(a.get("email"))
            au_phone = _extract_phone(a)

            m_key = ""
            if au_user and au_user in merchant_by_user:
                m_key = merchant_by_user[au_user]
            elif au_email_norm and au_email_norm in merchant_by_email:
                m_key = merchant_by_email[au_email_norm]
            elif au_phone and au_phone in merchant_by_phone:
                m_key = merchant_by_phone[au_phone]

            if not m_key:
                continue

            if m_key not in auth_by_merchant_key:
                auth_by_merchant_key[m_key] = a

        login_count_by_key = defaultdict(int)
        last_login_by_key = {}
        for row in login_rows:
            uid = _extract_user_id(row)
            if not uid or uid not in merchant_by_user:
                continue

            if _safe_text(row.get("activityType")).upper() not in {"", "LOGIN"}:
                continue

            m_key = merchant_by_user[uid]
            login_count_by_key[m_key] += 1
            dt = _extract_date(row, ["activityTime", "createdDt", "date"])
            if dt and (m_key not in last_login_by_key or dt > last_login_by_key[m_key]):
                last_login_by_key[m_key] = dt

        tx_count_by_key = defaultdict(int)
        tx_active_count_by_key = defaultdict(int)
        tx_revenue_by_key = defaultdict(float)
        tx_revenue_by_branch = defaultdict(float)
        tx_by_branch_merchant = defaultdict(lambda: defaultdict(float))
        tx_branch_set_by_key = defaultdict(set)
        for tx in merchant_tx:
            m_id = _safe_text(tx.get("merchantId"))
            if not m_id or m_id not in merchant_by_id:
                continue
            m_key = merchant_by_id[m_id]
            tx_count_by_key[m_key] += 1

            status = _safe_text(tx.get("status")).upper()
            if status not in {"FAILED", "CANCELLED", "REVERSED"}:
                tx_active_count_by_key[m_key] += 1

            amt = _extract_float(tx, ["amountPaid", "amount", "totalAmountPaid", "transactionAmount", "amountByWallet"]) 
            tx_revenue_by_key[m_key] += amt

            branch = _safe_text(tx.get("branchName") or tx.get("city") or tx.get("location"))
            if branch:
                tx_branch_set_by_key[m_key].add(branch)
                tx_revenue_by_branch[branch] += amt
                tx_by_branch_merchant[branch][m_key] += amt

        wallet_merchant_ids = set()
        wallet_balance_by_merchant_id = {}
        for w in merchant_wallet:
            w_mid = _safe_text(w.get("merchantId") or w.get("_id") or w.get("id"))
            if w_mid:
                wallet_merchant_ids.add(w_mid)
                wallet_balance_by_merchant_id[w_mid] = _extract_float(w, ["amount", "balance", "walletBalance"])

        payout_by_merchant_id = defaultdict(list)
        for p in merchant_payout:
            p_mid = _safe_text(p.get("merchantId"))
            if p_mid:
                payout_by_merchant_id[p_mid].append(p)

        business_image_count_by_key = defaultdict(int)
        for img in business_images:
            uid = _extract_user_id(img)
            if uid and uid in merchant_by_user:
                business_image_count_by_key[merchant_by_user[uid]] += 1

        deals_count_by_key = defaultdict(int)
        deal_ids_by_key = defaultdict(set)
        for d in deals_rows:
            uid = _extract_user_id(d)
            if not uid or uid not in merchant_by_user:
                continue
            m_key = merchant_by_user[uid]
            deals_count_by_key[m_key] += 1
            deal_id = _extract_oid(d.get("_id") or d.get("dealId") or d.get("id"))
            if deal_id:
                deal_ids_by_key[m_key].add(deal_id)

        deal_image_count_by_key = defaultdict(int)
        for di in deal_images:
            did = _extract_oid(di.get("dealId") or di.get("dealID") or di.get("_id"))
            if not did:
                continue
            for m_key, did_set in deal_ids_by_key.items():
                if did in did_set:
                    deal_image_count_by_key[m_key] += 1

        notification_count_by_key = defaultdict(int)
        for n in notifications_rows:
            if _safe_text(n.get("usertype")).upper() not in {"", "MERCHANT"}:
                continue
            uid = _extract_user_id(n)
            if uid and uid in merchant_by_user:
                notification_count_by_key[merchant_by_user[uid]] += 1

        active_session_count_by_key = defaultdict(int)
        for s in user_sessions:
            uid = _extract_user_id(s)
            if not uid or uid not in merchant_by_user:
                continue
            if _to_bool(s.get("isActive")):
                active_session_count_by_key[merchant_by_user[uid]] += 1

        audit_action_count_by_key = defaultdict(int)
        for a in audit_trail_rows:
            user_type = _safe_text(a.get("userType") or a.get("type")).upper()
            if user_type not in {"", "MERCHANT", "UNKNOWN"}:
                continue

            uid = _extract_user_id(a)
            mapped_key = ""
            if uid and uid in merchant_by_user:
                mapped_key = merchant_by_user[uid]
            else:
                created_by = _normalize_email(a.get("createdBy") or a.get("userName"))
                if created_by and created_by in merchant_by_email:
                    mapped_key = merchant_by_email[created_by]

            if mapped_key:
                audit_action_count_by_key[mapped_key] += 1

        wallet_ledger_count_by_merchant_id = defaultdict(int)
        for wl in merchant_wallet_ledger:
            mid = _safe_text(wl.get("merchantId"))
            if mid:
                wallet_ledger_count_by_merchant_id[mid] += 1

        locations_by_key = defaultdict(list)
        location_set_by_key = defaultdict(set)
        city_set_by_key = defaultdict(set)
        location_key_to_merchants = defaultdict(set)
        duplicate_count_by_key = defaultdict(int)
        for loc in merchant_locations:
            uid = _extract_user_id(loc)
            if not uid or uid not in merchant_by_user:
                continue
            m_key = merchant_by_user[uid]
            lkey = _location_key(loc)
            if not lkey:
                continue
            locations_by_key[m_key].append(loc)
            if lkey in location_set_by_key[m_key]:
                duplicate_count_by_key[m_key] += 1
            location_set_by_key[m_key].add(lkey)
            location_key_to_merchants[lkey].add(m_key)
            city = _safe_text(loc.get("city"))
            if city:
                city_set_by_key[m_key].add(city)

        rows: List[Dict[str, Any]] = []
        title = "Which Merchants"
        collections_used = ["merchantDetails"]
        action_used = "list"
        filters_used: Dict[str, Any] = {}

        login_condition = _parse_metric_condition(query_lower, ["logged in", "login", "login activity", "login records", "logins"], default_positive=(">", 0), default_negative=("=", 0))
        tx_condition = _parse_metric_condition(query_lower, ["transactions", "transaction", "transaction history", "processed transactions"], default_positive=(">", 0), default_negative=("=", 0))
        session_condition = _parse_metric_condition(query_lower, ["active sessions", "active session", "sessions", "session"], default_positive=(">", 0), default_negative=("=", 0))
        audit_condition = _parse_metric_condition(query_lower, ["audit", "audit trail", "audit actions"], default_positive=(">", 0), default_negative=("=", 0))
        wallet_condition = _parse_metric_condition(query_lower, ["wallet balance", "wallet", "balance"], default_positive=(">", 0), default_negative=("=", 0))

        def merchant_base_row(m: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "Merchant ID": m["merchant_id"],
                "Merchant User ID": m["user_id"],
                "Merchant Name": m["name"],
                "Merchant Email": m["email"],
            }

        # Location intents
        if "location" in query_lower or _contains_any(query_lower, ["outside region", "outside registered region", "outside their registered region"]):
            if _contains_any(query_lower, ["same location", "same locations", "shared location", "shared locations"]):
                for lkey, mkeys in location_key_to_merchants.items():
                    if len(mkeys) <= 1:
                        continue
                    names = sorted([merchant_by_key[k]["name"] for k in mkeys])
                    rows.append({
                        "Location": lkey,
                        "Merchant Count": len(mkeys),
                        "Merchants": ", ".join(names[:8]),
                    })
                rows = sorted(rows, key=lambda x: x["Merchant Count"], reverse=True)
                title = "Which Merchants Operate In Same Locations"
                collections_used = ["merchantDetails", "merchantlocation"]
                filters_used = {"shared_location": "merchant_count>1"}

            elif _contains_all(query_lower, ["multiple", "location"]) and _contains_any(query_lower, ["only one location", "one location", "single location"]) and "transaction" in query_lower:
                for m in merchants:
                    m_key = m["key"]
                    loc_count = len(location_set_by_key.get(m_key, set()))
                    tx_loc_count = len(tx_branch_set_by_key.get(m_key, set()))
                    if loc_count <= 1:
                        continue
                    if tx_loc_count != 1:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Location Count": loc_count,
                        "Transaction Location Count": tx_loc_count,
                        "Transaction Location": next(iter(tx_branch_set_by_key[m_key])) if tx_branch_set_by_key.get(m_key) else "N/A",
                    })
                    rows.append(row)
                title = "Which Merchants Have Multiple Locations But Transactions From Only One Location"
                collections_used = ["merchantDetails", "merchantlocation", "transaction_history"]
                filters_used = {"location_count": ">1", "transaction_location_count": 1}

            elif _contains_any(query_lower, ["multiple", "many", "more than one", "most location", "most locations"]):
                want_most = _contains_any(query_lower, ["most location", "most locations", "highest"])
                for m in merchants:
                    m_key = m["key"]
                    count = len(location_set_by_key.get(m_key, set()))
                    if want_most and count <= 0:
                        continue
                    if not want_most and count <= 1:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Location Count": count,
                        "Cities": ", ".join(sorted(city_set_by_key.get(m_key, set()))[:6]) or "N/A",
                    })
                    rows.append(row)
                rows = sorted(rows, key=lambda x: x["Location Count"], reverse=True)
                if want_most and rows:
                    top_count = rows[0]["Location Count"]
                    rows = [r for r in rows if r["Location Count"] == top_count]
                    action_used = "top_n"
                title = "Which Merchants Operate In Multiple Locations"
                collections_used = ["merchantDetails", "merchantlocation"]
                filters_used = {"location_count": "top" if want_most else ">1"}

            elif _contains_any(query_lower, ["no location", "without location", "not assigned", "no location assigned"]):
                for m in merchants:
                    m_key = m["key"]
                    if len(location_set_by_key.get(m_key, set())) > 0:
                        continue
                    row = merchant_base_row(m)
                    row["Location Count"] = 0
                    rows.append(row)
                title = "Which Merchants Have No Location Assigned"
                collections_used = ["merchantDetails", "merchantlocation"]
                filters_used = {"location_count": 0}

            elif _contains_any(query_lower, ["duplicate location", "duplicate locations", "duplicate location records"]):
                for m in merchants:
                    m_key = m["key"]
                    dup = duplicate_count_by_key.get(m_key, 0)
                    if dup <= 0:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Duplicate Location Records": dup,
                        "Distinct Locations": len(location_set_by_key.get(m_key, set())),
                    })
                    rows.append(row)
                rows = sorted(rows, key=lambda x: x["Duplicate Location Records"], reverse=True)
                title = "Which Merchants Have Duplicate Location Records"
                collections_used = ["merchantDetails", "merchantlocation"]
                filters_used = {"duplicate_locations": ">0"}

            elif _contains_any(query_lower, ["changed", "moved", "relocated"]):
                for m in merchants:
                    m_key = m["key"]
                    if len(location_set_by_key.get(m_key, set())) <= 1:
                        continue
                    latest_update = None
                    for loc in locations_by_key.get(m_key, []):
                        dt = _extract_date(loc, ["updatedDt", "createdDt"])
                        if dt and (latest_update is None or dt > latest_update):
                            latest_update = dt
                    row = merchant_base_row(m)
                    row.update({
                        "Distinct Locations": len(location_set_by_key.get(m_key, set())),
                        "Latest Location Update": latest_update.isoformat(sep=" ", timespec="seconds") if latest_update else "N/A",
                    })
                    rows.append(row)
                rows = sorted(rows, key=lambda x: x["Distinct Locations"], reverse=True)
                title = "Which Merchants Changed Their Business Location"
                collections_used = ["merchantDetails", "merchantlocation"]
                filters_used = {"distinct_locations": ">1"}

            elif _contains_all(query_lower, ["highest", "revenue", "location"]):
                top_branch = max(tx_revenue_by_branch, key=tx_revenue_by_branch.get) if tx_revenue_by_branch else ""
                if top_branch:
                    for m_key, rev in sorted(tx_by_branch_merchant[top_branch].items(), key=lambda kv: kv[1], reverse=True):
                        m = merchant_by_key.get(m_key)
                        if not m:
                            continue
                        row = merchant_base_row(m)
                        row.update({
                            "Top Revenue Location": top_branch,
                            "Location Revenue": round(rev, 2),
                        })
                        rows.append(row)
                title = "Which Merchants Operate In Highest Revenue Location"
                collections_used = ["merchantDetails", "merchantlocation", "transaction_history"]
                filters_used = {"location": top_branch or "N/A", "metric": "revenue"}

            elif _contains_all(query_lower, ["locations", "no transactions"]) or _contains_all(query_lower, ["location", "no transaction"]):
                for m in merchants:
                    m_key = m["key"]
                    if len(location_set_by_key.get(m_key, set())) <= 0:
                        continue
                    if tx_count_by_key.get(m_key, 0) > 0:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Location Count": len(location_set_by_key.get(m_key, set())),
                        "Transaction Count": 0,
                    })
                    rows.append(row)
                title = "Which Merchants Have Locations But No Transactions"
                collections_used = ["merchantDetails", "merchantlocation", "transaction_history"]
                filters_used = {"location_count": ">0", "transactions": 0}

            elif _contains_any(query_lower, ["outside", "outside registered region", "outside their registered region"]):
                for m in merchants:
                    m_key = m["key"]
                    md_country = _safe_text(next((loc.get("country") for loc in locations_by_key.get(m_key, []) if _safe_text(loc.get("country"))), ""))
                    if not md_country:
                        continue
                    out_of_region = any(_safe_text(loc.get("country")) and _safe_text(loc.get("country")) != md_country for loc in locations_by_key.get(m_key, []))
                    if not out_of_region:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Registered Region": md_country,
                        "Outside Region": "YES",
                    })
                    rows.append(row)
                title = "Which Merchants Operate Outside Registered Region"
                collections_used = ["merchantDetails", "merchantlocation"]
                filters_used = {"outside_registered_region": True}

        # Merchant transaction/payout/deal/notification/wallet/session/audit intents
        elif (
            (
                _contains_any(query_lower, ["payout", "deal", "image", "notification", "wallet", "ledger", "inactive", "transaction", "session", "audit"])
                and _contains_any(query_lower, ["no", "without", "never", "zero", "but", "more than", "less than", "at least", "at most", "exactly"])
            )
            or _contains_any(query_lower, ["multiple active sessions", "more than one active session", "many active sessions"])
        ):
            if _contains_any(query_lower, ["login activity", "login activities", "login"]) and _contains_any(query_lower, ["transaction", "transactions", "transaction history"]):
                for m in merchants:
                    m_key = m["key"]
                    login_count = login_count_by_key.get(m_key, 0)
                    if not _matches_condition(login_count, login_condition):
                        continue
                    tx_count = tx_count_by_key.get(m_key, 0)
                    if not _matches_condition(tx_count, tx_condition):
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Login Activity Count": login_count,
                        "Transaction Count": tx_count,
                    })
                    rows.append(row)
                title = "Which Merchants Have Login Activity But No Transaction History"
                collections_used = ["merchantDetails", "login_activities", "transaction_history"]
                filters_used = {"login_activity": _condition_text(login_condition), "transaction_history": _condition_text(tx_condition)}

            elif _contains_any(query_lower, ["active sessions", "active session"]) and _contains_any(query_lower, ["inactive in merchantdetails", "inactive in merchant", "merchantdetails inactive", "inactive merchant", "inactive in merchant details"]):
                for m in merchants:
                    m_key = m["key"]
                    session_count = active_session_count_by_key.get(m_key, 0)
                    if not _matches_condition(session_count, session_condition):
                        continue
                    is_inactive = m["is_deleted"] or m["verification"] in {"INACTIVE", "BLOCKED", "DISABLED"}
                    if not is_inactive:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Active Sessions": session_count,
                        "Verification Status": m["verification"],
                        "Deleted Flag": "YES" if m["is_deleted"] else "NO",
                    })
                    rows.append(row)
                title = "Which Merchants Have Active Sessions But Are Inactive In MerchantDetails"
                collections_used = ["merchantDetails", "user_sessions"]
                filters_used = {"active_sessions": _condition_text(session_condition), "merchant_inactive": True}

            elif _contains_any(query_lower, ["multiple active sessions", "more than one active session", "many active sessions"]):
                multiple_session_condition = _parse_metric_condition(
                    query_lower,
                    ["multiple active sessions", "more than one active session", "many active sessions", "active sessions", "active session"],
                    default_positive=(">", 1),
                    default_negative=("=", 0),
                )
                for m in merchants:
                    m_key = m["key"]
                    session_count = active_session_count_by_key.get(m_key, 0)
                    if not _matches_condition(session_count, multiple_session_condition):
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Active Sessions": session_count,
                    })
                    rows.append(row)
                rows = sorted(rows, key=lambda x: x.get("Active Sessions", 0), reverse=True)
                title = "Which Merchants Have Multiple Active Sessions"
                collections_used = ["merchantDetails", "user_sessions"]
                filters_used = {"active_sessions": _condition_text(multiple_session_condition)}

            elif _contains_any(query_lower, ["audit trail", "audit", "triggered audit trail", "audit actions"]) and _contains_any(query_lower, ["wallet balance", "wallet", "balance"]):
                for m in merchants:
                    m_key = m["key"]
                    mid = m["merchant_id"]
                    audit_count = audit_action_count_by_key.get(m_key, 0)
                    if not _matches_condition(audit_count, audit_condition):
                        continue
                    balance = wallet_balance_by_merchant_id.get(mid, 0.0)
                    if not _matches_condition(balance, wallet_condition):
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Audit Actions": audit_count,
                        "Wallet Balance": round(balance, 2),
                    })
                    rows.append(row)
                title = "Which Merchants Triggered Audit Trail Actions But Have No Wallet Balance"
                collections_used = ["merchantDetails", "audit_trail", "wallet"]
                filters_used = {"audit_actions": _condition_text(audit_condition), "wallet_balance": _condition_text(wallet_condition)}

            elif _contains_any(query_lower, ["locked in authentication", "locked in auth", "account locked", "locked auth"]) and _contains_any(query_lower, ["processed transactions", "still processed transactions", "has transactions", "transaction history"]):
                for m in merchants:
                    m_key = m["key"]
                    auth = auth_by_merchant_key.get(m_key)
                    if not auth:
                        continue
                    locked = _to_bool(auth.get("accountLocked")) or _safe_text(auth.get("status")).upper() in {"LOCKED", "BLOCKED"}
                    if not locked:
                        continue
                    tx_count = tx_count_by_key.get(m_key, 0)
                    if not _matches_condition(tx_count, tx_condition):
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Auth Status": _safe_text(auth.get("status")) or "N/A",
                        "Account Locked": str(auth.get("accountLocked", "N/A")),
                        "Transaction Count": tx_count,
                    })
                    rows.append(row)
                title = "Which Merchants Were Locked In Authentication But Still Processed Transactions"
                collections_used = ["merchantDetails", "authuser", "transaction_history"]
                filters_used = {"auth_locked": True, "transactions": _condition_text(tx_condition)}

            elif _contains_any(query_lower, ["transaction", "transactions", "transaction history"]) and _contains_any(query_lower, ["payout", "payout record", "payout records"]) and _contains_any(query_lower, ["no", "without", "zero", "none", "missing"]):
                for m in merchants:
                    m_key = m["key"]
                    mid = m["merchant_id"]
                    if tx_count_by_key.get(m_key, 0) <= 0:
                        continue
                    if len(payout_by_merchant_id.get(mid, [])) > 0:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Transaction Count": tx_count_by_key.get(m_key, 0),
                        "Payout Records": 0,
                    })
                    rows.append(row)
                title = "Which Merchants Have Transactions But No Payout Record"
                collections_used = ["merchantDetails", "transaction_history", "merchant_payout"]
                filters_used = {"transactions": ">0", "payout_records": 0}

            elif _contains_any(query_lower, ["payout", "payout record", "payout records"]) and _contains_any(query_lower, ["transaction", "transactions", "transaction history"]) and _contains_any(query_lower, ["zero transaction", "no transaction", "without transaction", "never transacted"]):
                for m in merchants:
                    m_key = m["key"]
                    mid = m["merchant_id"]
                    if len(payout_by_merchant_id.get(mid, [])) <= 0:
                        continue
                    if tx_count_by_key.get(m_key, 0) > 0:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Payout Records": len(payout_by_merchant_id.get(mid, [])),
                        "Transaction Count": 0,
                    })
                    rows.append(row)
                title = "Which Merchants Have Payout Records But Zero Transactions"
                collections_used = ["merchantDetails", "merchant_payout", "transaction_history"]
                filters_used = {"payout_records": ">0", "transactions": 0}

            elif _contains_any(query_lower, ["business image", "business images"]) and _contains_any(query_lower, ["deal", "deals"]) and _contains_any(query_lower, ["never", "no deal", "without deal", "never created"]):
                for m in merchants:
                    m_key = m["key"]
                    if business_image_count_by_key.get(m_key, 0) <= 0:
                        continue
                    if deals_count_by_key.get(m_key, 0) > 0:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Business Images": business_image_count_by_key.get(m_key, 0),
                        "Deals Created": 0,
                    })
                    rows.append(row)
                title = "Which Merchants Uploaded Business Images But Never Created Deals"
                collections_used = ["merchantDetails", "businessimage", "deals"]
                filters_used = {"business_images": ">0", "deals_created": 0}

            elif _contains_any(query_lower, ["deal", "deals"]) and _contains_any(query_lower, ["image", "images", "deal image", "deal images"]) and _contains_any(query_lower, ["never", "no image", "without image", "never uploaded"]):
                for m in merchants:
                    m_key = m["key"]
                    if deals_count_by_key.get(m_key, 0) <= 0:
                        continue
                    if deal_image_count_by_key.get(m_key, 0) > 0:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Deals Created": deals_count_by_key.get(m_key, 0),
                        "Deal Images": 0,
                    })
                    rows.append(row)
                title = "Which Merchants Created Deals But Never Uploaded Deal Images"
                collections_used = ["merchantDetails", "deals", "dealimage"]
                filters_used = {"deals_created": ">0", "deal_images": 0}

            elif _contains_any(query_lower, ["notification", "notifications"]) and _contains_any(query_lower, ["wallet activity", "wallet ledger", "wallet"]) and _contains_any(query_lower, ["no", "without", "zero", "none"]):
                for m in merchants:
                    mid = m["merchant_id"]
                    if notification_count_by_key.get(m["key"], 0) <= 0:
                        continue
                    if wallet_ledger_count_by_merchant_id.get(mid, 0) > 0:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Notifications": notification_count_by_key.get(m["key"], 0),
                        "Wallet Ledger Entries": 0,
                    })
                    rows.append(row)
                title = "Which Merchants Received Notifications But Had No Wallet Activity"
                collections_used = ["merchantDetails", "notifications", "merchant_wallet_ledger"]
                filters_used = {"notifications": ">0", "wallet_activity": 0}

            elif "transaction" in query_lower and "inactive" in query_lower:
                for m in merchants:
                    m_key = m["key"]
                    if tx_count_by_key.get(m_key, 0) <= 0:
                        continue
                    is_inactive = m["is_deleted"] or m["verification"] in {"INACTIVE", "BLOCKED", "DISABLED"}
                    if not is_inactive:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Transaction Count": tx_count_by_key.get(m_key, 0),
                        "Verification Status": m["verification"],
                        "Deleted Flag": "YES" if m["is_deleted"] else "NO",
                    })
                    rows.append(row)
                title = "Which Merchants Have Transaction History But Are Marked Inactive"
                collections_used = ["merchantDetails", "transaction_history"]
                filters_used = {"transactions": ">0", "merchant_inactive": True}

            elif _contains_any(query_lower, ["wallet", "wallet balance"]) and _contains_any(query_lower, ["ledger", "ledger entries", "wallet ledger"]) and _contains_any(query_lower, ["no", "without", "zero", "none"]):
                for m in merchants:
                    mid = m["merchant_id"]
                    balance = wallet_balance_by_merchant_id.get(mid, 0.0)
                    if balance <= 0:
                        continue
                    if wallet_ledger_count_by_merchant_id.get(mid, 0) > 0:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Wallet Balance": round(balance, 2),
                        "Wallet Ledger Entries": 0,
                    })
                    rows.append(row)
                title = "Which Merchants Have Wallet Balance But No Ledger Entries"
                collections_used = ["merchantDetails", "wallet", "merchant_wallet_ledger"]
                filters_used = {"wallet_balance": ">0", "wallet_ledger_entries": 0}

        # Auth/login/verification intents
        elif _contains_any(query_lower, ["auth", "authenticated", "authentication", "login", "verified", "onboarding", "blocked", "mismatch", "created before"]):
            has_auth_terms = _contains_any(query_lower, [
                "auth",
                "authentication",
                "authenticated",
                "auth account",
                "auth accounts",
                "authentication account",
                "authentication accounts",
                "auth record",
                "auth records",
            ])
            has_login_terms = _contains_any(query_lower, [
                "login",
                "logged in",
                "login record",
                "login records",
                "login activity",
                "login activities",
            ])
            wants_no_auth = _contains_any(query_lower, [
                "no authentication",
                "without authentication",
                "not in authentication",
                "no auth",
                "without auth",
                "not authenticated",
            ])
            wants_no_login = _contains_any(query_lower, [
                "no login",
                "no login record",
                "no login records",
                "without login",
                "without login records",
                "never logged in",
                "no login activity",
                "no logins",
            ])

            if has_auth_terms and has_login_terms:
                for m in merchants:
                    m_key = m["key"]
                    auth = auth_by_merchant_key.get(m_key)
                    if not auth:
                        continue
                    login_count = login_count_by_key.get(m_key, 0)
                    if not _matches_condition(login_count, login_condition):
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Auth Email": _safe_text(auth.get("email")) or "N/A",
                        "Auth Status": _safe_text(auth.get("status")) or "N/A",
                        "Login Records": login_count,
                    })
                    rows.append(row)
                if login_condition == ("=", 0):
                    title = "Which Merchants Have Authentication Accounts But No Login Records"
                else:
                    title = "Which Merchants Have Authentication Accounts By Login Activity"
                collections_used = ["merchantDetails", "authuser", "login_activities"]
                filters_used = {"auth_record": "exists", "login_records": _condition_text(login_condition)}

            elif has_login_terms and wants_no_auth:
                for m in merchants:
                    m_key = m["key"]
                    if login_count_by_key.get(m_key, 0) <= 0:
                        continue
                    if m_key in auth_by_merchant_key:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Login Records": login_count_by_key.get(m_key, 0),
                        "Last Login": last_login_by_key[m_key].isoformat(sep=" ", timespec="seconds") if m_key in last_login_by_key else "N/A",
                        "In Authentication": "NO",
                    })
                    rows.append(row)
                title = "Which Merchants Have Login Records But No Authentication Accounts"
                collections_used = ["merchantDetails", "login_activities", "authuser"]
                filters_used = {"login_records": ">0", "auth_record": "missing"}

            elif _contains_all(query_lower, ["merchant db", "not in authentication"]) or _contains_all(query_lower, ["exist", "not in authentication"]):
                for m in merchants:
                    if m["key"] in auth_by_merchant_key:
                        continue
                    row = merchant_base_row(m)
                    row["In Authentication"] = "NO"
                    rows.append(row)
                title = "Which Merchants Exist In Merchant DB But Not In Authentication"
                collections_used = ["merchantDetails", "authuser"]
                filters_used = {"auth_record": "missing"}

            elif _contains_all(query_lower, ["authenticated", "not verified"]):
                for m in merchants:
                    auth = auth_by_merchant_key.get(m["key"])
                    if not auth:
                        continue
                    auth_verified = _to_bool(auth.get("emailVerified")) and _to_bool(auth.get("phoneVerified"))
                    merchant_verified = m["verification"] in {"APPROVED", "VERIFIED", "ACTIVE"}
                    if auth_verified and merchant_verified:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Auth Email Verified": str(auth.get("emailVerified", "N/A")),
                        "Auth Phone Verified": str(auth.get("phoneVerified", "N/A")),
                        "Merchant Verification": m["verification"],
                    })
                    rows.append(row)
                title = "Which Merchants Are Authenticated But Not Verified"
                collections_used = ["merchantDetails", "authuser"]
                filters_used = {"auth_record": "exists", "verified": False}

            elif _contains_all(query_lower, ["logged in", "never completed onboarding"]):
                for m in merchants:
                    m_key = m["key"]
                    if login_count_by_key.get(m_key, 0) <= 0:
                        continue
                    onboarding_complete = m["registered"] and m["reg_level"] >= 4 and m["verification"] in {"APPROVED", "VERIFIED", "ACTIVE"}
                    if onboarding_complete:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Login Count": login_count_by_key.get(m_key, 0),
                        "Reg Level": m["reg_level"],
                        "Registered Business": "YES" if m["registered"] else "NO",
                        "Verification Status": m["verification"],
                    })
                    rows.append(row)
                title = "Which Merchants Logged In But Never Completed Onboarding"
                collections_used = ["merchantDetails", "login_activities"]
                filters_used = {"login_records": ">0", "onboarding_complete": False}

            elif _contains_all(query_lower, ["blocked", "active in merchant db"]):
                for m in merchants:
                    auth = auth_by_merchant_key.get(m["key"])
                    if not auth:
                        continue
                    blocked = _to_bool(auth.get("accountLocked")) or _safe_text(auth.get("status")).upper() in {"BLOCKED", "LOCKED", "INACTIVE"}
                    merchant_active = not m["is_deleted"]
                    if not (blocked and merchant_active):
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Auth Status": _safe_text(auth.get("status")).upper() or "N/A",
                        "Account Locked": str(auth.get("accountLocked", "N/A")),
                        "Merchant Active": "YES",
                    })
                    rows.append(row)
                title = "Which Merchants Are Blocked In Authentication But Active In Merchant DB"
                collections_used = ["merchantDetails", "authuser"]
                filters_used = {"auth_blocked": True, "merchant_active": True}

            elif _contains_all(query_lower, ["mismatched", "email"]):
                for m in merchants:
                    auth = auth_by_merchant_key.get(m["key"])
                    if not auth:
                        continue
                    auth_email = _normalize_email(auth.get("email"))
                    m_email = m["email_norm"]
                    if not auth_email or not m_email or auth_email == m_email:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Merchant Email": m["email"],
                        "Auth Email": _safe_text(auth.get("email")) or "N/A",
                    })
                    rows.append(row)
                title = "Which Merchants Have Mismatched Emails Across Systems"
                collections_used = ["merchantDetails", "authuser"]
                filters_used = {"email_mismatch": True}

            elif _contains_all(query_lower, ["created before", "authentication"]):
                for m in merchants:
                    auth = auth_by_merchant_key.get(m["key"])
                    if not auth:
                        continue
                    auth_created = _extract_date(auth, ["createdDt", "createdDate", "updatedDt"])
                    if not m["created"] or not auth_created:
                        continue
                    if m["created"] >= auth_created:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Merchant Created": m["created"].isoformat(sep=" ", timespec="seconds"),
                        "Auth Created": auth_created.isoformat(sep=" ", timespec="seconds"),
                    })
                    rows.append(row)
                title = "Which Merchants Were Created Before Authentication"
                collections_used = ["merchantDetails", "authuser"]
                filters_used = {"merchant_created_before_auth": True}

            elif _contains_all(query_lower, ["authenticated", "no transactions"]):
                for m in merchants:
                    if m["key"] not in auth_by_merchant_key:
                        continue
                    if tx_count_by_key.get(m["key"], 0) > 0:
                        continue
                    row = merchant_base_row(m)
                    row["Transaction Count"] = 0
                    rows.append(row)
                title = "Which Merchants Are Authenticated But Have No Transactions"
                collections_used = ["merchantDetails", "authuser", "transaction_history"]
                filters_used = {"auth_record": "exists", "transactions": 0}

            elif _contains_all(query_lower, ["auth records", "no wallet"]):
                for m in merchants:
                    if m["key"] not in auth_by_merchant_key:
                        continue
                    if m["merchant_id"] in wallet_merchant_ids:
                        continue
                    row = merchant_base_row(m)
                    row["Has Wallet"] = "NO"
                    rows.append(row)
                title = "Which Merchants Have Auth Records But No Wallet"
                collections_used = ["merchantDetails", "authuser", "wallet"]
                filters_used = {"auth_record": "exists", "wallet": "missing"}

            elif _contains_all(query_lower, ["login records", "no business details"]):
                login_user_ids = {_extract_user_id(l) for l in login_rows if _extract_user_id(l)}
                merchant_user_ids = {m["user_id"] for m in merchants if m["user_id"] != "N/A"}
                for uid in sorted(login_user_ids - merchant_user_ids):
                    rows.append({
                        "Merchant User ID": uid,
                        "Business Details": "MISSING",
                    })
                title = "Which Merchants Have Login Records But No Business Details"
                collections_used = ["merchantDetails", "login_activities"]
                filters_used = {"login_record": "exists", "merchant_details": "missing"}

            elif _contains_all(query_lower, ["inactive auth", "active transactions"]):
                for m in merchants:
                    auth = auth_by_merchant_key.get(m["key"])
                    if not auth:
                        continue
                    auth_inactive = _safe_text(auth.get("status")).upper() in {"INACTIVE", "BLOCKED", "LOCKED"} or _to_bool(auth.get("accountLocked"))
                    if not auth_inactive:
                        continue
                    if tx_active_count_by_key.get(m["key"], 0) <= 0:
                        continue
                    row = merchant_base_row(m)
                    row.update({
                        "Auth Status": _safe_text(auth.get("status")).upper() or "N/A",
                        "Active Transaction Count": tx_active_count_by_key.get(m["key"], 0),
                    })
                    rows.append(row)
                title = "Which Merchants Have Inactive Auth But Active Transactions"
                collections_used = ["merchantDetails", "authuser", "transaction_history"]
                filters_used = {"auth_status": "inactive", "active_transactions": ">0"}

        # Generic merchant fallback for unrecognized merchant WHICH queries
        else:
            for m in merchants:
                row = merchant_base_row(m)
                row.update({
                    "Transaction Count": tx_count_by_key.get(m["key"], 0),
                    "Location Count": len(location_set_by_key.get(m["key"], set())),
                })
                rows.append(row)
            rows = sorted(rows, key=lambda x: x.get("Transaction Count", 0), reverse=True)
            collections_used = ["merchantDetails", "transaction_history", "merchantlocation"]

        return {
            "handled": True,
            "title": title,
            "rows": rows,
            "target": "merchants",
            "single": bool(is_high or is_low),
            "database": "numoni_merchant",
            "collections": collections_used,
            "action": action_used,
            "filters": filters_used,
            "intent_plan": fetch_plan["intents"],
            "fetch_columns": fetch_plan["columns"],
        }

    # Authentication-focused 'which' queries
    if "auth" in query_lower or "login" in query_lower or "session" in query_lower or "audit" in query_lower:
        auth_rows = get_rows("authentication", "login_activities")
        if not auth_rows:
            auth_rows = get_rows("authentication", "audit_trail")

        rows = []
        for row in auth_rows:
            rows.append({
                "User ID": str(row.get("userId", "")).strip() or "N/A",
                "User Type": str(row.get("userType", "")).strip() or "N/A",
                "Activity Type": str(row.get("activityType") or row.get("action") or "N/A"),
                "Status": str(row.get("successful") or row.get("status") or "N/A"),
            })

        rows = _dedupe_rows(rows, ["User ID", "Activity Type"])
        return {
            "handled": True,
            "title": "Which Users (Authentication)",
            "rows": rows,
            "target": "users",
            "single": False,
            "database": "authentication",
            "collections": ["login_activities", "audit_trail"],
            "action": "list",
            "filters": {"domain": "authentication"},
            "intent_plan": fetch_plan["intents"],
            "fetch_columns": fetch_plan["columns"],
        }

    # Generic "which X" behavior from merchant transaction history
    filtered_tx = merchant_tx
    if status_filter:
        filtered_tx = [r for r in filtered_tx if str(r.get("status", "")).upper() == status_filter]

    if target in {"merchant", "vendor", "store", "business"}:
        rows = []
        for tx in filtered_tx:
            merchant_id = str(tx.get("merchantId", "")).strip()
            merchant_name = str(tx.get("merchantName", "")).strip()
            if not merchant_id and not merchant_name:
                continue
            rows.append({
                "Merchant ID": merchant_id or "N/A",
                "Merchant Name": merchant_name or "N/A",
            })

        rows = _dedupe_rows(rows, ["Merchant ID", "Merchant Name"])

        if is_high or is_low:
            counts = defaultdict(int)
            names = {}
            for tx in filtered_tx:
                mid = str(tx.get("merchantId", "")).strip() or str(tx.get("merchantName", "")).strip()
                if not mid:
                    continue
                counts[mid] += 1
                names[mid] = str(tx.get("merchantName", "")).strip() or "N/A"

            if counts:
                chosen = max(counts, key=counts.get) if is_high else min(counts, key=counts.get)
                rows = [{"Merchant ID": chosen, "Merchant Name": names.get(chosen, "N/A"), "Activity Count": counts[chosen]}]
            else:
                rows = []

        return {
            "handled": True,
            "title": "Which Merchants",
            "rows": rows,
            "target": "merchants",
            "single": bool(is_high or is_low),
            "database": "numoni_merchant",
            "collections": ["transaction_history"],
            "action": "top_n" if is_high else ("bottom_n" if is_low else "list"),
            "filters": ({"status": status_filter} if status_filter else {}),
            "intent_plan": fetch_plan["intents"],
            "fetch_columns": fetch_plan["columns"],
        }

    if target in {"transaction", "payment", "transfer"}:
        rows = []
        for tx in filtered_tx:
            rows.append({
                "Transaction ID": str(tx.get("transactionId") or tx.get("transactionNo") or "N/A"),
                "Merchant Name": str(tx.get("merchantName", "")).strip() or "N/A",
                "Customer Name": str(tx.get("customerName", "")).strip() or "N/A",
                "Status": str(tx.get("status", "")).upper() or "N/A",
                "Amount": tx.get("amount", "N/A"),
                "Date": tx.get("date", "N/A"),
            })

        if is_high or is_low:
            # default metric: amount
            def amount_value(row):
                try:
                    return float(str(row.get("Amount", 0)).replace(",", ""))
                except Exception:
                    return 0.0
            if rows:
                chosen = max(rows, key=amount_value) if is_high else min(rows, key=amount_value)
                rows = [chosen]

        return {
            "handled": True,
            "title": "Which Transactions",
            "rows": rows,
            "target": "transactions",
            "single": bool(is_high or is_low),
            "database": "numoni_merchant",
            "collections": ["transaction_history"],
            "action": "max" if is_high else ("min" if is_low else "list"),
            "filters": ({"status": status_filter} if status_filter else {}),
            "intent_plan": fetch_plan["intents"],
            "fetch_columns": fetch_plan["columns"],
        }

    # fallback: do not hijack unknown 'which' intents
    return {"handled": False}
