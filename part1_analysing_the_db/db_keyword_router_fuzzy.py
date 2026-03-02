import os
import json
from difflib import SequenceMatcher

# -----------------------------
# Paths
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

CUSTOMER_DB_PATH = os.path.join(BASE_DIR, "databases", "numoni_customer")
MERCHANT_DB_PATH = os.path.join(BASE_DIR, "databases", "numoni_merchant")
AUTH_DB_PATH = os.path.join(BASE_DIR, "databases", "authentication")

CUSTOMER_DETAILS_FILE = os.path.join(CUSTOMER_DB_PATH, "customerDetails.json")
MERCHANT_DETAILS_FILE = os.path.join(MERCHANT_DB_PATH, "merchantDetails.json")
BANK_INFORMATION_FILE = os.path.join(MERCHANT_DB_PATH, "bankInformation.json")


# -----------------------------
# Pure keyword dictionary
# -----------------------------
DB_KEYWORDS = {
    "authentication": [
        "auth", "authentication", "authuser", "auth user", "otp",
        "login", "login activity", "login activities",
        "signin", "sign in", "signin records",
        "signup", "sign up",
        "auth otp", "authentication otp", "login otp", "signin otp", "otp for auth",
        "refresh token", "refresh_token", "refreshtoken",
        "roles", "audit", "audit trail", "audit info", "audit information", "audit log",
        "account deletion", "account deletion request",
        "user sessions", "user_sessions",
        "user device", "user device detail", "userdevicedetail"
    ],
    "numoni_customer": [
        "customer", "customers", "cust",
        "buyer", "client",
        "customer details", "customer profile",
        "customer error",
        "customer location",
        "customer transaction",
        "customer session",
        "wallet", "wallet ledger",
        "topup", "top up", "load money",
        "payment otp", "payment otp verification", "payment_otp_verification", "otp verification",
        "send money", "receive money",
        "registered customer", "customer registered",
        "favorite", "favourite", "favorites deals", "favourite deals",
        "initiative order", "initiative orders", "initiative_orders",
        "sponsored deal", "sponsored deals", "sponsored_deals"
    ],

    "numoni_merchant": [
        "merchant", "merchants",
        "shop", "store", "vendor", "seller", "business",
        "merchant details", "merchant profile",
        "merchant location",
        "bank", "account number", "bank code",
        "pos", "terminal",
        "payout", "settlement", "withdrawal",
        "deal", "deals",
        "review", "reviews", "ratings",
        "notification", "notifications",
        "registered merchant", "merchant registered",
        "transaction", "transactions", "sales", "revenue",
        "region", "regions", "locations", "state", "states", "country", "geography", "regions in nigeria"
    ],

    "both": [
        "compare",
        "comparison",
        "difference",
        "merchant vs customer",
        "customer vs merchant",
        "relationship",
        "foreign key",
        "join",
        "mapping"
    ]
}


def normalize_query(text: str):
    return " ".join(text.lower().strip().split())


def is_likely_business_name(text: str) -> bool:
    """Check if text looks like a business name (caps, multiple words, keywords)"""
    # Has business-like keywords
    business_keywords = ['limited', 'ltd', 'inc', 'corp', 'bank', 'microfinance', 
                        'company', 'enterprises', 'ventures', 'group', 'services',
                        'ministry', 'agency', 'institute', 'foundation', 'centre']
    
    # All caps or Title Case (common for business names)
    if text.isupper() or (text[0].isupper() and ' ' in text):
        return True
    
    # Contains business keywords
    text_lower = text.lower()
    return any(kw in text_lower for kw in business_keywords)


def fuzzy_match(query: str, name: str, threshold=0.70):
    """Check if name fuzzy matches in query with threshold"""
    ratio = SequenceMatcher(None, query, name).ratio()
    return ratio >= threshold


def find_best_match(query: str, names_dict: dict, min_length=4):
    """Find best matching name with highest similarity ratio"""
    if not names_dict:
        return None, 0.0
    
    best_name = None
    best_ratio = 0.0
    
    for name in names_dict.keys():
        # Skip very short names to avoid false positives
        if len(name) < min_length:
            continue
            
        # Check if name is a substring (exact or close)
        if name in query:
            return name, 1.0
        
        # Check fuzzy match
        ratio = SequenceMatcher(None, query, name).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_name = name
    
    return best_name, best_ratio


# -----------------------------
# Load merchant/customer data with ALL name fields
def load_merchant_data(file_path):
    """Load merchant names with fuzzy matching metadata"""
    merchants = {}
    if not os.path.exists(file_path):
        return merchants
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if isinstance(data, list):
            for record in data:
                if isinstance(record, dict):
                    # Collect all name-like fields
                    names = []
                    for field in ["businessName", "brandName", "registeredBusiness"]:
                        val = record.get(field)
                        if isinstance(val, str) and val.strip():
                            names.append(val.strip().lower())
                    
                    # Store with priority (longer names first = more specific)
                    for name in sorted(names, key=len, reverse=True):
                        if name not in merchants:
                            merchants[name] = True
    except Exception:
        pass
    
    return merchants


def load_bank_information(file_path):
    """Load bank names linked to merchants"""
    banks = {}
    if not os.path.exists(file_path):
        return banks
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if isinstance(data, list):
            for record in data:
                if isinstance(record, dict):
                    # Get bank name and account holder name
                    bankname = record.get('bankname')
                    account_holder = record.get('accountHolderName')
                    
                    if isinstance(bankname, str) and bankname.strip():
                        name = bankname.strip().lower()
                        if name not in banks:
                            banks[name] = True
                    
                    if isinstance(account_holder, str) and account_holder.strip():
                        name = account_holder.strip().lower()
                        if name not in banks and len(name) >= 4:
                            banks[name] = True
    except Exception:
        pass
    
    return banks


def load_customer_data(file_path):
    """Load customer names (filtering out very short names)"""
    customers = {}
    if not os.path.exists(file_path):
        return customers
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if isinstance(data, list):
            for record in data:
                if isinstance(record, dict):
                    val = record.get("name")
                    if isinstance(val, str) and val.strip():
                        name = val.strip().lower()
                        # Only include names with 4+ characters (avoid matching "mo" to "MONIEPOINT")
                        if len(name) >= 4 and name not in customers:
                            customers[name] = True
    except Exception:
        pass
    
    return customers


# Load once (global cache)
MERCHANT_NAMES = load_merchant_data(MERCHANT_DETAILS_FILE)
BANK_NAMES = load_bank_information(BANK_INFORMATION_FILE)
# Merge bank names into merchant names
MERCHANT_NAMES.update(BANK_NAMES)
CUSTOMER_NAMES = load_customer_data(CUSTOMER_DETAILS_FILE)


def detect_database(user_query: str):
    query = normalize_query(user_query)
    original_query = user_query  # Keep original for business name detection

    # 0) Explicit collection/table intent should win over generic entity words in requested fields
    if any(term in query for term in ["initiative order", "initiative orders", "initiative_orders", "sponsored deals", "sponsored_deals"]):
        return {
            "selected_dbs": ["numoni_customer"],
            "reason": "Matched explicit CUSTOMER table intent"
        }

    # 1) BOTH keywords first
    for kw in DB_KEYWORDS["both"]:
        if kw in query:
            return {
                "selected_dbs": ["numoni_customer", "numoni_merchant"],
                "reason": f"Matched BOTH keyword: '{kw}'"
            }

    # 2) Authentication keywords
    for kw in DB_KEYWORDS["authentication"]:
        if kw in query:
            return {
                "selected_dbs": ["authentication"],
                "reason": f"Matched AUTHENTICATION keyword: '{kw}'"
            }

    # 3) PRIORITY: Check for explicit "merchant" or "customer" mentions first
    # This ensures "merchant wallet ledger" goes to merchant DB, not customer DB
    explicit_merchant = ["merchant", "merchants"]
    explicit_customer = ["customer", "customers"]
    
    # Check merchant first if explicitly mentioned
    for kw in explicit_merchant:
        if kw in query:
            return {
                "selected_dbs": ["numoni_merchant"],
                "reason": f"Explicit MERCHANT keyword: '{kw}'"
            }
    
    # Check customer if explicitly mentioned
    for kw in explicit_customer:
        if kw in query:
            return {
                "selected_dbs": ["numoni_customer"],
                "reason": f"Explicit CUSTOMER keyword: '{kw}'"
            }

    # 4) Check OTHER customer-specific keywords (like "wallet ledger" alone)
    for kw in sorted(DB_KEYWORDS["numoni_customer"], key=len, reverse=True):
        if kw in query and kw not in explicit_customer:
            return {
                "selected_dbs": ["numoni_customer"],
                "reason": f"Matched CUSTOMER keyword: '{kw}'"
            }

    # 5) Check OTHER merchant-specific keywords
    for kw in DB_KEYWORDS["numoni_merchant"]:
        if kw in query and kw not in explicit_merchant:
            return {
                "selected_dbs": ["numoni_merchant"],
                "reason": f"Matched MERCHANT keyword: '{kw}'"
            }

    # 6) Check if query looks like a BUSINESS NAME
    # Business names usually are capitalized, have multiple words, or business keywords
    if is_likely_business_name(original_query):
        # Try to match in merchant data first
        merchant_match, merchant_ratio = find_best_match(query, MERCHANT_NAMES, min_length=3)
        if merchant_match and merchant_ratio >= 0.40:  # Lower threshold for business names
            return {
                "selected_dbs": ["numoni_merchant"],
                "reason": f"Detected business name '{original_query}'. Matched merchant: '{merchant_match}' ({merchant_ratio:.2%})"
            }
        else:
            # Looks like a business name but not in our DB - assume merchant
            return {
                "selected_dbs": ["numoni_merchant"],
                "reason": f"Business name pattern detected: '{original_query}' (not found in merchant DB, but treated as merchant)"
            }

    # 5) MERCHANT NAME detection (more lenient with min_length=3)
    merchant_match, merchant_ratio = find_best_match(query, MERCHANT_NAMES, min_length=3)
    
    # 6) CUSTOMER NAME detection (stricter with min_length=4)
    customer_match, customer_ratio = find_best_match(query, CUSTOMER_NAMES, min_length=4)
    
    # If both matched, PREFER MERCHANT unless customer has significantly higher ratio
    if merchant_match and customer_match:
        # Merchant gets priority - customer needs to be +15% better to win
        if customer_ratio > merchant_ratio + 0.15:
            return {
                "selected_dbs": ["numoni_customer"],
                "reason": f"Matched CUSTOMER NAME '{customer_match}' (similarity: {customer_ratio:.2%}) >> merchant '{merchant_match}' ({merchant_ratio:.2%})"
            }
        else:
            return {
                "selected_dbs": ["numoni_merchant"],
                "reason": f"Matched MERCHANT NAME '{merchant_match}' (similarity: {merchant_ratio:.2%}) >> customer '{customer_match}' ({customer_ratio:.2%})"
            }
    
    # Only merchant matched
    if merchant_match and merchant_ratio >= 0.45:
        return {
            "selected_dbs": ["numoni_merchant"],
            "reason": f"Matched MERCHANT NAME: '{merchant_match}' (similarity: {merchant_ratio:.2%})"
        }
    
    # Only customer matched (higher threshold)
    if customer_match and customer_ratio >= 0.70:
        return {
            "selected_dbs": ["numoni_customer"],
            "reason": f"Matched CUSTOMER NAME: '{customer_match}' (similarity: {customer_ratio:.2%})"
        }

    return {
        "selected_dbs": ["unknown"],
        "reason": "No keyword or name matched"
    }


if __name__ == "__main__":
    print("🔥 Numoni DB Router (Keyword + Name Detection)")
    print("Type 'exit' to stop.\n")

    while True:
        q = input("Ask something: ")
        if q.lower() == "exit":
            break

        result = detect_database(q)
        print("\n➡️ Suggested DB(s):", result["selected_dbs"])
        print("📌 Reason:", result["reason"])
        print("-" * 70)
