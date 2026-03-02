"""
Numoni Database Router - BERT Semantic Matching Version
========================================================

IMPROVEMENTS OVER FUZZY MATCHING:
1. ✅ Semantic Understanding: BERT understands meaning, not just character similarity
   - "digital wallet" matches "customer wallet" semantically
   - "business transactions" matches "merchant sales" by context
   
2. ✅ Better Intent Recognition: Uses contextual embeddings to understand what user wants
   - "show me shops in Lagos" → merchant (understands "shops" = business/merchant)
   - "client money transfers" → customer (understands "client" = customer context)
   
3. ✅ Lower False Positives: More accurate matching reduces wrong database selection
   - Fuzzy: "mo" could match "MONIEPOINT" (wrong)
   - BERT: Requires semantic relevance, not just character overlap
   
4. ✅ Multi-word Understanding: BERT processes entire phrases, not word-by-word
   - "merchant payment settlement" → correctly identifies as merchant context
   - "customer topup history" → correctly identifies as customer context

5. ✅ Context-aware Scoring: Uses semantic context of entire databases
   - Compares query against database descriptions for better routing
   - Handles ambiguous queries by understanding overall intent

REQUIREMENTS:
- pip install sentence-transformers
- pip install torch (CPU version is fine)
- Model: all-MiniLM-L6-v2 (lightweight, fast, accurate for short text)

THRESHOLDS (Lower than fuzzy due to BERT accuracy):
- Fuzzy threshold: 0.70 → BERT threshold: 0.65 (semantic matching is more precise)
- Merchant match: 0.45 → BERT: 0.40 (better understanding of business context)
- Customer match: 0.70 → BERT: 0.65 (more accurate customer intent detection)
"""

import os
import json
from sentence_transformers import SentenceTransformer, util
import torch
import numpy as np

# -----------------------------
# Paths
# -----------------------------
# Since we're now in part1_analysing_the_db/bert/, go up 2 levels to reach numoni_final/
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

CUSTOMER_DB_PATH = os.path.join(BASE_DIR, "databases", "numoni_customer")
MERCHANT_DB_PATH = os.path.join(BASE_DIR, "databases", "numoni_merchant")
AUTH_DB_PATH = os.path.join(BASE_DIR, "databases", "authentication")

CUSTOMER_DETAILS_FILE = os.path.join(CUSTOMER_DB_PATH, "customerDetails.json")
MERCHANT_DETAILS_FILE = os.path.join(MERCHANT_DB_PATH, "merchantDetails.json")
BANK_INFORMATION_FILE = os.path.join(MERCHANT_DB_PATH, "bankInformation.json")


# -----------------------------
# BERT Model Initialization
# -----------------------------
# Using a lightweight but accurate model for semantic similarity
# all-MiniLM-L6-v2 is fast and good for short text matching
print("🤖 Loading BERT model for semantic matching...")
try:
    BERT_MODEL = SentenceTransformer('all-MiniLM-L6-v2')
    print("✅ BERT model loaded successfully!")
except Exception as e:
    print(f"⚠️ Warning: Could not load BERT model: {e}")
    print("   Falling back to keyword matching only.")
    BERT_MODEL = None

# Cache for embeddings to improve performance
EMBEDDING_CACHE = {}


# -----------------------------
# Enhanced keyword dictionary with semantic context
# -----------------------------
DB_KEYWORDS = {
    "authentication": [
        # Core auth terms
        "auth", "authentication", "authuser", "auth user", "otp",
        "login", "login activity", "login activities",
        "signin", "sign in", "signin records",
        "signup", "sign up",
        "auth otp", "authentication otp", "login otp", "signin otp", "otp for auth",
        "refresh token", "refresh_token", "refreshtoken",
        "roles", "audit", "audit trail", "audit info", "audit information", "audit log",
        "account deletion", "account deletion request",
        "user sessions", "user_sessions",
        "user device", "user device detail", "userdevicedetail",
        # Semantic variations
        "user authentication", "access control", "security logs", "login history",
        "session management", "token refresh", "user access"
    ],
    "numoni_customer": [
        # Core customer terms
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
        # Semantic variations
        "client account", "buyer profile", "consumer data", "end user",
        "money transfer", "funds transfer", "digital wallet", "e-wallet",
        "customer wallet", "user balance", "account balance"
    ],

    "numoni_merchant": [
        # Core merchant terms
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
        "region", "regions", "locations", "state", "states", "country", "geography", "regions in nigeria",
        # Semantic variations
        "business account", "vendor profile", "shop owner", "seller account",
        "payment terminal", "point of sale", "business transactions",
        "merchant settlement", "business payout", "revenue data",
        "business location", "merchant geography", "store locations",
        "merchant banking", "business bank account", "settlement account"
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

# Semantic context for better understanding
DB_CONTEXT = {
    "authentication": "authentication system, user login, security, access control, sessions, tokens",
    "numoni_customer": "customer accounts, digital wallets, money transfers, topups, customer transactions, client data",
    "numoni_merchant": "merchant businesses, shops, stores, vendors, POS terminals, settlements, payouts, business transactions, regions"
}


def normalize_query(text: str):
    return " ".join(text.lower().strip().split())


def get_embedding(text: str):
    """Get BERT embedding for text with caching"""
    if text in EMBEDDING_CACHE:
        return EMBEDDING_CACHE[text]
    
    if BERT_MODEL is None:
        return None
    
    try:
        embedding = BERT_MODEL.encode(text, convert_to_tensor=True)
        EMBEDDING_CACHE[text] = embedding
        return embedding
    except Exception:
        return None


def semantic_similarity(text1: str, text2: str) -> float:
    """Calculate semantic similarity between two texts using BERT"""
    if BERT_MODEL is None:
        # Fallback to simple substring matching
        text1_lower = text1.lower()
        text2_lower = text2.lower()
        if text2_lower in text1_lower or text1_lower in text2_lower:
            return 1.0
        return 0.0
    
    try:
        emb1 = get_embedding(text1)
        emb2 = get_embedding(text2)
        
        if emb1 is None or emb2 is None:
            return 0.0
        
        # Cosine similarity
        similarity = util.cos_sim(emb1, emb2).item()
        return max(0.0, similarity)  # Ensure non-negative
    except Exception:
        return 0.0


def bert_match(query: str, name: str, threshold=0.65) -> bool:
    """Check if name semantically matches query with BERT (lower threshold than fuzzy)"""
    # First check exact substring (fast path)
    if name in query.lower():
        return True
    
    # BERT semantic matching
    similarity = semantic_similarity(query, name)
    return similarity >= threshold


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


def find_best_match(query: str, names_dict: dict, min_length=4) -> tuple:
    """Find best matching name using BERT semantic similarity"""
    if not names_dict:
        return None, 0.0
    
    best_name = None
    best_score = 0.0
    
    # Get query embedding once
    query_emb = get_embedding(query) if BERT_MODEL else None
    
    for name in names_dict.keys():
        # Skip very short names to avoid false positives
        if len(name) < min_length:
            continue
        
        # Check exact substring match first (score = 1.0)
        if name in query:
            return name, 1.0
        
        # BERT semantic similarity
        if query_emb is not None:
            name_emb = get_embedding(name)
            if name_emb is not None:
                try:
                    similarity = util.cos_sim(query_emb, name_emb).item()
                    similarity = max(0.0, similarity)
                    
                    if similarity > best_score:
                        best_score = similarity
                        best_name = name
                except Exception:
                    pass
        else:
            # Fallback: simple substring matching
            if name in query.lower():
                return name, 0.9
    
    return best_name, best_score


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

    # 1) BOTH keywords first
    for kw in DB_KEYWORDS["both"]:
        if kw in query:
            return {
                "selected_dbs": ["numoni_customer", "numoni_merchant"],
                "reason": f"Matched BOTH keyword: '{kw}'",
                "confidence": 1.0
            }

    # 2) Authentication keywords
    for kw in DB_KEYWORDS["authentication"]:
        if kw in query:
            return {
                "selected_dbs": ["authentication"],
                "reason": f"Matched AUTHENTICATION keyword: '{kw}'",
                "confidence": 0.95
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
                "reason": f"Explicit MERCHANT keyword: '{kw}'",
                "confidence": 0.95
            }
    
    # Check customer if explicitly mentioned
    for kw in explicit_customer:
        if kw in query:
            return {
                "selected_dbs": ["numoni_customer"],
                "reason": f"Explicit CUSTOMER keyword: '{kw}'",
                "confidence": 0.95
            }

    # 4) BERT Semantic Matching against DB contexts
    # This helps understand intent beyond keywords
    if BERT_MODEL is not None:
        try:
            context_scores = {}
            for db_name, context in DB_CONTEXT.items():
                if db_name != "both":
                    score = semantic_similarity(query, context)
                    context_scores[db_name] = score
            
            # Get best semantic match
            if context_scores:
                best_db = max(context_scores, key=context_scores.get)
                best_score = context_scores[best_db]
                
                # If semantic score is high enough, use it
                if best_score > 0.55:  # Lower threshold since we have context
                    db_map = {
                        "authentication": "authentication",
                        "numoni_customer": "numoni_customer",
                        "numoni_merchant": "numoni_merchant"
                    }
                    return {
                        "selected_dbs": [db_map[best_db]],
                        "reason": f"BERT semantic match: '{best_db}' (confidence: {best_score:.2%})",
                        "confidence": best_score
                    }
        except Exception:
            pass

    # 5) Check OTHER customer-specific keywords (like "wallet ledger" alone)
    for kw in sorted(DB_KEYWORDS["numoni_customer"], key=len, reverse=True):
        if kw in query and kw not in explicit_customer:
            return {
                "selected_dbs": ["numoni_customer"],
                "reason": f"Matched CUSTOMER keyword: '{kw}'",
                "confidence": 0.85
            }

    # 6) Check OTHER merchant-specific keywords
    for kw in DB_KEYWORDS["numoni_merchant"]:
        if kw in query and kw not in explicit_merchant:
            return {
                "selected_dbs": ["numoni_merchant"],
                "reason": f"Matched MERCHANT keyword: '{kw}'",
                "confidence": 0.85
            }

    # 7) Check if query looks like a BUSINESS NAME
    # Business names usually are capitalized, have multiple words, or business keywords
    if is_likely_business_name(original_query):
        # Try to match in merchant data first
        merchant_match, merchant_score = find_best_match(query, MERCHANT_NAMES, min_length=3)
        if merchant_match and merchant_score >= 0.35:  # Lower threshold for BERT (more accurate)
            return {
                "selected_dbs": ["numoni_merchant"],
                "reason": f"Detected business name '{original_query}'. BERT matched merchant: '{merchant_match}' ({merchant_score:.2%})",
                "confidence": merchant_score
            }
        else:
            # Looks like a business name but not in our DB - assume merchant
            return {
                "selected_dbs": ["numoni_merchant"],
                "reason": f"Business name pattern detected: '{original_query}' (not found in merchant DB, but treated as merchant)",
                "confidence": 0.7
            }

    # 8) MERCHANT NAME detection using BERT (more lenient with min_length=3)
    merchant_match, merchant_score = find_best_match(query, MERCHANT_NAMES, min_length=3)
    
    # 9) CUSTOMER NAME detection using BERT (stricter with min_length=4)
    customer_match, customer_score = find_best_match(query, CUSTOMER_NAMES, min_length=4)
    
    # If both matched, PREFER MERCHANT unless customer has significantly higher score
    if merchant_match and customer_match:
        # Merchant gets priority - customer needs to be +12% better to win (lower than fuzzy due to BERT accuracy)
        if customer_score > merchant_score + 0.12:
            return {
                "selected_dbs": ["numoni_customer"],
                "reason": f"BERT matched CUSTOMER NAME '{customer_match}' (similarity: {customer_score:.2%}) >> merchant '{merchant_match}' ({merchant_score:.2%})",
                "confidence": customer_score
            }
        else:
            return {
                "selected_dbs": ["numoni_merchant"],
                "reason": f"BERT matched MERCHANT NAME '{merchant_match}' (similarity: {merchant_score:.2%}) >> customer '{customer_match}' ({customer_score:.2%})",
                "confidence": merchant_score
            }
    
    # Only merchant matched
    if merchant_match and merchant_score >= 0.40:  # Lower threshold for BERT
        return {
            "selected_dbs": ["numoni_merchant"],
            "reason": f"BERT matched MERCHANT NAME: '{merchant_match}' (similarity: {merchant_score:.2%})",
            "confidence": merchant_score
        }
    
    # Only customer matched (higher threshold)
    if customer_match and customer_score >= 0.65:  # Lower than fuzzy (0.70) due to BERT accuracy
        return {
            "selected_dbs": ["numoni_customer"],
            "reason": f"BERT matched CUSTOMER NAME: '{customer_match}' (similarity: {customer_score:.2%})",
            "confidence": customer_score
        }

    return {
        "selected_dbs": ["unknown"],
        "reason": "No keyword, semantic match, or name matched",
        "confidence": 0.0
    }


if __name__ == "__main__":
    print("🤖 Numoni DB Router (BERT Semantic Matching)")
    print("=" * 70)
    print("Using BERT for intelligent semantic understanding")
    print("Type 'exit' to stop.\n")

    while True:
        q = input("Ask something: ")
        if q.lower() == "exit":
            break

        result = detect_database(q)
        print("\n➡️  Suggested DB(s):", result["selected_dbs"])
        print("📌 Reason:", result["reason"])
        if "confidence" in result:
            print(f"✨ Confidence: {result['confidence']:.1%}")
        print("-" * 70)
