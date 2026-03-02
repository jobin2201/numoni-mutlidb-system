#!/usr/bin/env python
"""
Collection Router - Detects which collection/table to query based on user intent
Uses NLP to match user query with collection metadata
"""
import os
import json
import re
from pathlib import Path
from difflib import SequenceMatcher
from sentence_transformers import SentenceTransformer, util

BASE_DIR = Path(__file__).parent
METADATA_DIR = BASE_DIR.parent

# -----------------------------
# BERT Model Initialization
# -----------------------------
print("🤖 Loading BERT model for collection routing...")
try:
    BERT_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    print("✅ BERT model loaded successfully!")
except Exception as e:
    print(f"⚠️ Warning: Could not load BERT model: {e}")
    print("   Falling back to fuzzy matching only.")
    BERT_MODEL = None

EMBEDDING_CACHE = {}


def load_metadata(database_name):
    """Load collection metadata for a specific database"""
    metadata_file = METADATA_DIR / f"{database_name}_collections_metadata.json"
    
    if not metadata_file.exists():
        return {}
    
    with open(metadata_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_keywords():
    """Load collection keyword mappings"""
    keywords_file = METADATA_DIR / "collection_keywords.json"
    
    if not keywords_file.exists():
        return {}
    
    with open(keywords_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def normalize_text(text):
    """Normalize text for comparison - handles underscores and case"""
    # Convert to lowercase, replace underscores with spaces
    normalized = text.lower().strip()
    normalized = normalized.replace('_', ' ')
    # Collapse multiple spaces
    return " ".join(normalized.split())


def normalize_for_matching(text):
    """More aggressive normalization for value matching"""
    normalized = normalize_text(text)
    # Also try without spaces for compound words
    no_space = normalized.replace(' ', '')
    return normalized, no_space


def get_embedding(text):
    """Get BERT embedding for text with caching"""
    if text in EMBEDDING_CACHE:
        return EMBEDDING_CACHE[text]

    embedding = BERT_MODEL.encode(text, convert_to_tensor=True)
    EMBEDDING_CACHE[text] = embedding
    return embedding


def calculate_similarity(text1, text2):
    """Calculate semantic similarity ratio between two texts"""
    if not text1 or not text2:
        return 0.0

    if BERT_MODEL is None:
        return SequenceMatcher(None, text1, text2).ratio()

    try:
        emb1 = get_embedding(text1)
        emb2 = get_embedding(text2)
        similarity = util.cos_sim(emb1, emb2).item()
        return float(similarity)
    except Exception:
        return SequenceMatcher(None, text1, text2).ratio()


def get_field_synonyms():
    """Map common query terms to actual field names"""
    return {
        'merchant name': ['businessName', 'merchantName', 'name'],
        'merchant names': ['businessName', 'merchantName', 'name'],
        'business name': ['businessName', 'merchantName'],
        'customer name': ['userName', 'customerName', 'name', 'fullName'],
        'name': ['businessName', 'merchantName', 'userName', 'customerName', 'name'],
        'points received': ['smsText', 'points', 'bonusAmount', 'rewardPoints'],
        'numoni points': ['smsText', 'points', 'bonusAmount', 'rewardPoints'],
        'received points': ['smsText', 'points', 'bonusAmount'],
        'highest points': ['smsText', 'points', 'bonusAmount'],
        'location': ['region', 'address', 'location', 'city', 'state'],
        'region': ['region', 'location', 'address'],
        'phone': ['phoneNumber', 'phone', 'mobile'],
        'email': ['email', 'emailAddress'],
        'amount': ['amount', 'totalAmount', 'transactionAmount'],
        'balance': ['balance', 'walletBalance', 'availableBalance'],
        'status': ['status', 'transactionStatus', 'orderStatus'],
        'type': ['type', 'transactionType', 'accountType'],
        # Authentication-specific synonyms
        'login details': ['userId', 'activityType', 'successful', 'deviceId'],
        'login info': ['userId', 'activityType', 'successful', 'sessionId'],
        'user details': ['email', 'phoneNumber', 'password', 'accountLocked'],
        'user info': ['email', 'phoneNumber', 'name', 'password'],
        'authentication': ['email', 'password', 'accountLocked', 'emailVerified'],
        'session': ['sessionId', 'userId', 'isActive'],
        'device': ['deviceId', 'userId'],
        'otp': ['otp', 'otpType', 'expiryTime'],
        'token': ['token', 'isRevoked'],
        'audit': ['action', 'changeLog', 'userId'],
        'permission': ['name', 'permissions'],
        'role': ['name', 'permissions']
    }


def extract_content_phrases(query):
    """Extract phrases that should match TEXT CONTENT (not just field names)"""
    content_patterns = [
        r'(received\s+[\d,\.]+\s+(?:numoni\s+)?points)',
        r'(highest\s+(?:numoni\s+)?points)',
        r'(starting\s+with\s+\w+)',
        r'(similar\s+\w+\s+names?)',
        r'(contains?\s+[\w\s]+)',
    ]
    
    phrases = []
    query_lower = query.lower()
    
    for pattern in content_patterns:
        matches = re.findall(pattern, query_lower)
        phrases.extend(matches)
    
    # Also extract key value phrases
    if 'received' in query_lower and 'points' in query_lower:
        phrases.append('received points')
    if 'numoni points' in query_lower:
        phrases.append('numoni points')
    if 'starting with' in query_lower:
        phrases.append('starting with')
    if 'similar' in query_lower:
        phrases.append('similar')
    
    return phrases


def extract_value_terms(query):
    """Extract potential value terms from query (what user is looking for)"""
    import re
    value_terms = []
    query_lower = query.lower()
    
    # Remove action words and limiters that are NOT data values
    stop_words = [
        'show', 'me', 'the', 'get', 'list', 'find', 'display', 'give',
        'top', 'bottom', 'first', 'last', 'all', 'some', 'any', 'each',
        'how', 'many', 'what', 'which', 'where', 'when', 'who',
        'in', 'for', 'with', 'from', 'to', 'of', 'at', 'by'
    ]
    
    # Remove "top N" and "bottom N" patterns from consideration as values
    cleaned_query = re.sub(r'\b(top|bottom|first|last)\s+\d+\b', '', query_lower)
    
    # 1. Extract from common patterns
    value_indicators = [
        r'type\s+(?:is|as|=|:|of)\s+([\w\s]+?)(?:\s+(?:as|in|for|their|the|and|with)\s|$)',
        r'status\s+(?:is|as|=|:)\s+([\w\s]+?)(?:\s+(?:as|in|for|their|the|and|with)\s|$)',
        r'(?:have|with|contains?)\s+([\w\s]+?)\s+type',
        r'(?:have|with|contains?)\s+([\w\s]+?)\s+status',
    ]
    
    for pattern in value_indicators:
        matches = re.findall(pattern, cleaned_query)
        for match in matches:
            # Clean up the match
            cleaned = match.strip()
            # Remove stop words
            words = [w for w in cleaned.split() if w not in stop_words]
            if words and len(' '.join(words)) > 2:
                value_terms.append(' '.join(words))
                # Also add as single word if it's compound
                if len(words) > 1:
                    value_terms.append('_'.join(words))
                    value_terms.append(''.join(words))
    
    # 2. Extract capitalized/underscore terms (specific values like SHARE_MONEY)
    # But filter out common words
    special_terms = re.findall(r'[A-Z][A-Z_]+|[a-z]+_[a-z]+', query)
    for term in special_terms:
        term_lower = term.lower()
        if term_lower not in stop_words and len(term_lower) > 2:
            value_terms.append(term_lower)
    
    # 3. Look for compound terms (multi-word phrases that might be values)
    # E.g., "share money" in "have share money type"
    compound_patterns = [
        r'\b([a-z]+\s+[a-z]+)\s+(?:type|status|transaction|record)',
        r'(?:type|status)\s+(?:is|as|of)\s+([a-z]+\s+[a-z]+)',
    ]
    for pattern in compound_patterns:
        matches = re.findall(pattern, cleaned_query)
        for match in matches:
            words = [w for w in match.split() if w not in stop_words]
            if words:
                value_terms.append(' '.join(words))
                # Also add underscore version
                value_terms.append('_'.join(words))
    
    # Remove duplicates and filter out single characters/numbers
    unique_terms = []
    for term in set(value_terms):
        # Skip if it's just a number or very short
        if re.match(r'^\d+$', term) or len(term) < 2:
            continue
        # Skip if it's a stop word
        if term.lower() in stop_words:
            continue
        unique_terms.append(term)
    
    return unique_terms


def detect_collection(user_query, database_name):
    """
    Detect which collection to use based on user query
    
    Args:
        user_query: User's natural language question
        database_name: Which database (numoni_customer or numoni_merchant)
    
    Returns:
        dict with selected_collection, reason, confidence, and matched_fields
    """
    query = normalize_text(user_query)
    original_query = user_query.lower()
    
    # Load metadata and keywords
    collections_metadata = load_metadata(database_name)
    collection_keywords = load_keywords()
    field_synonyms = get_field_synonyms()
    content_phrases = extract_content_phrases(user_query)
    
    if not collections_metadata:
        return {
            "selected_collection": None,
            "reason": f"No metadata found for {database_name}",
            "confidence": 0.0
        }
    
    # Extract value terms user is looking for
    value_terms = extract_value_terms(user_query)
    
    # Detect entity creation queries (added/created/registered)
    creation_keywords = ['added', 'created', 'registered', 'signed up', 'joined', 'new customer', 'new merchant']
    is_creation_query = any(keyword in original_query for keyword in creation_keywords)
    
    # Detect transaction queries
    transaction_keywords = ['transaction', 'payment', 'transfer', 'purchase', 'sale', 'deal made']
    is_transaction_query = any(keyword in original_query for keyword in transaction_keywords)
    
    # Detect region queries - should use nigeria_regions collection
    region_query_keywords = ['region', 'regions', 'area', 'state', 'lga', 'location']
    is_region_query = any(keyword in original_query for keyword in region_query_keywords) and 'nigeria' in original_query
    
    # Detect "received points" queries - should use customer_sharepoint_requests collection
    received_points_keywords = ['received points', 'receive points', 'received nuMoni', 'receive nuMoni', 
                               'points received', 'nuMoni received']
    is_received_points_query = any(keyword in original_query for keyword in received_points_keywords)
    
    # Scoring system
    collection_scores = {}
    
    for collection_name, metadata in collections_metadata.items():
        score = 0.0
        reasons = []
        matched_fields = []
        matched_values = []
        value_context_match = False
        
        # CRITICAL RULE: Entity creation queries should use details collections
        if is_creation_query and not is_transaction_query:
            if 'details' in collection_name.lower():
                score += 150  # VERY HIGH priority
                reasons.append("[OK] Entity creation query -> details collection")
            elif 'transaction' in collection_name.lower():
                score -= 100  # STRONG penalty for transaction collections
                reasons.append("[NO] Creation query, not transaction collection")
        
        # CRITICAL RULE: Region queries should use nigeria_regions
        if is_region_query:
            if 'nigeria_regions' in collection_name.lower() or ('regions' in collection_name.lower() and 'nigeria' in collection_name.lower()):
                score += 200  # HIGHEST priority  
                reasons.append("[OK] Region query -> nigeria_regions collection")
            elif 'pos' in collection_name.lower():
                score -= 150  # STRONG penalty for POS collection
                reasons.append("[NO] Region query, not regions collection")
        
        # CRITICAL RULE: "Received points" queries should use customer_sharepoint_requests
        if is_received_points_query:
            if 'sharepoint' in collection_name.lower() or 'share_point' in collection_name.lower() or 'share point' in collection_name.lower():
                score += 200  # HIGHEST priority - this is the points transfer collection
                reasons.append("[OK] 'Received points' query -> customer_sharepoint_requests collection")
            elif collection_name.lower() in ['numoni_customer', 'numoni_merchant', 'nigeria_regions']:
                score -= 100  # STRONG penalty for other main collections
                reasons.append("[NO] Received points query, not sharepoint collection")
        
        # 1. Collection name match + VALUE CONTEXT MATCHING
        collection_name_norm = collection_name.replace('_', ' ')
        collection_name_similarity = calculate_similarity(collection_name_norm, query)
        
        # Extract key subject nouns from query
        key_subjects = []
        subject_patterns = [
            r'\b(regions?|locations?|customers?|merchants?|transactions?|deals?|reviews?|payments?|invoices?|orders?)\b',
            r'\b(wallet|balance|amount|status|type|category|name|details?)\b'
        ]
        for pattern in subject_patterns:
            matches = re.findall(pattern, query)
            key_subjects.extend(matches)
        
        # Check if collection name contains key subject
        collection_has_key_subject = any(subj in collection_name_norm for subj in key_subjects)
        
        # Check if value terms from query appear in collection name (STRONG indicator)
        for value_term in value_terms:
            value_term_clean = value_term.replace('_', ' ')
            # If user asks for "share_money" and collection is "customer_share_money"
            if value_term_clean in collection_name_norm or calculate_similarity(value_term_clean, collection_name_norm) > 0.7:
                score += 50  # High but not highest - actual data value is more important
                reasons.append(f"Collection name matches '{value_term}'")
                value_context_match = True
                break
        
        # Standard collection name matching
        if collection_name_norm in query or collection_name in query:
            if collection_has_key_subject:
                score += 60  # MUCH HIGHER for key subject collections
                reasons.append(f"Collection name with key subject matched")
            else:
                score += 40  # Exact match
                reasons.append(f"Collection name matched")
        elif collection_name_similarity > 0.6:
            boost = 1.5 if collection_has_key_subject else 1.0
            score += 40 * collection_name_similarity * boost
            reasons.append(f"Collection name similar ({collection_name_similarity:.2f})")
        
        # 2. Field name matching (PRIMARY - columns are actual data structure)
        fields = metadata.get("fields", [])
        field_exact_matches = 0
        field_partial_matches = 0
        
        # Extract key subject nouns from query (what the user is asking about)
        key_subjects = []
        subject_patterns = [
            r'\b(regions?|locations?|customers?|merchants?|transactions?|deals?|reviews?|payments?|invoices?|orders?)\b',
            r'\b(wallet|balance|amount|status|type|category|name|details?|points?)\b'
        ]
        for pattern in subject_patterns:
            matches = re.findall(pattern, query)
            key_subjects.extend(matches)
        
        # Check query terms against field synonyms (CRITICAL for understanding intent)
        synonym_matched_fields = []
        for query_term, possible_fields in field_synonyms.items():
            if query_term in query:
                # Check if any of these fields exist in this collection
                for field in fields:
                    if field in possible_fields:
                        matched_fields.append(field)
                        synonym_matched_fields.append(field)
                        score += 45  # HIGH priority for synonym understanding
                        reasons.append(f"Synonym match: '{query_term}' → {field}")
        
        for field in fields:
            field_norm = field.lower().replace('_', ' ')
            field_words = set(field_norm.split())
            query_words = set(query.split())
            
            # Skip if already matched via synonym
            if field in synonym_matched_fields:
                continue
            
            # Check if this field matches a key subject (HIGHEST priority)
            field_is_key_subject = any(subj in field_norm or field_norm in subj for subj in key_subjects)
            
            # Exact field match
            if field_norm in query or field in original_query:
                if field_is_key_subject:
                    score += 40  # DOUBLED for key subject fields
                    reasons.append(f"Key field '{field}' matched")
                else:
                    score += 20
                if field not in matched_fields:
                    matched_fields.append(field)
                field_exact_matches += 1
            # Fuzzy similarity matching
            elif calculate_similarity(field_norm, query) > 0.7:
                similarity = calculate_similarity(field_norm, query)
                boost = 2 if field_is_key_subject else 1
                score += 15 * similarity * boost
                if field not in matched_fields:
                    matched_fields.append(field)
                field_exact_matches += 1
            # Partial word overlap
            elif field_words & query_words:  # Intersection of words
                overlap_count = len(field_words & query_words)
                boost = 2 if field_is_key_subject else 1
                score += 8 * overlap_count * boost
                if field not in matched_fields:
                    matched_fields.append(field)
                field_partial_matches += 1
        
        # Bonus for multiple field matches (content relevance)
        if field_exact_matches > 1:
            score += field_exact_matches * 3
            reasons.append(f"Multiple fields matched ({field_exact_matches} exact)")
        elif matched_fields:
            reasons.append(f"Fields matched: {', '.join(matched_fields[:3])}")
        
        # CRITICAL BONUS: Collection name AND field name both match key subject
        # E.g., "regions" in "nigeria_regions" collection AND "region" field exists
        if collection_has_key_subject and any(
            any(subj in field.lower() for subj in key_subjects) 
            for field in matched_fields
        ):
            score += 50  # MASSIVE BONUS for semantic alignment
            reasons.append(f"Collection + field subject alignment")
        
        # 3. Sample value matching (CRITICAL - actual data content)
        # PRIORITIZE: Content text > Field names > Collection names
        sample_values = metadata.get("sample_values", {})
        value_match_count = 0
        exact_value_field_match = False
        content_text_match = False
        
        # STEP 1: Deep content text matching (HIGHEST PRIORITY)
        # Check if query mentions content phrases like "received points", "starting with", etc.
        for phrase in content_phrases:
            phrase_lower = phrase.lower()
            
            # Check all text fields for content matching
            for field, values in sample_values.items():
                if not isinstance(values, list):
                    values = [values]
                
                for value in values:
                    if value is None:
                        continue
                    
                    value_str = str(value).lower()
                    
                    # Content pattern matching
                    if 'received' in phrase_lower and 'points' in phrase_lower:
                        # Check if this field contains "received X points" text
                        if 'received' in value_str and 'points' in value_str:
                            score += 100  # MASSIVE BOOST - exact content match
                            matched_values.append(f"{field}={value_str[:40]}")
                            content_text_match = True
                            reasons.append(f"✓✓ CONTENT MATCH: '{phrase}' found in {field}")
                            break
                    
                    elif 'highest' in phrase_lower and 'points' in phrase_lower:
                        # Check if field has point values
                        if 'points' in value_str or 'numoni' in value_str:
                            score += 100
                            matched_values.append(f"{field}={value_str[:40]}")
                            content_text_match = True
                            reasons.append(f"✓✓ CONTENT MATCH: points data in {field}")
                            break
                    
                    elif 'starting with' in phrase_lower:
                        # This indicates user wants filtering capability
                        # Check if field has name/text data
                        if field.lower() in ['businessname', 'merchantname', 'name', 'username']:
                            score += 80
                            content_text_match = True
                            reasons.append(f"✓✓ NAME field for filtering: {field}")
                            break
                    
                    elif 'similar' in phrase_lower:
                        # Similar matching needs name fields
                        if field.lower() in ['businessname', 'merchantname', 'name', 'username']:
                            score += 80
                            content_text_match = True
                            reasons.append(f"✓✓ NAME field for similarity: {field}")
                            break
                
                if content_text_match:
                    if field not in matched_fields:
                        matched_fields.append(field)
                    break
            
            if content_text_match:
                break
        
        # STEP 2: Standard value matching (for specific values like statuses, types)
        for field, values in sample_values.items():
            # Skip if we already found content match
            if content_text_match:
                break
            
            # Handle both single values (old format) and lists (new format)
            if not isinstance(values, list):
                values = [values]
            
            # Check each unique value in this field
            for value in values:
                if value is None or value == "":
                    continue
                    
                value_str = str(value)
                value_norm, value_no_space = normalize_for_matching(value_str)
                
                # Check if this value matches what user is looking for
                for value_term in value_terms:
                    value_term_norm, value_term_no_space = normalize_for_matching(value_term)
                    
                    # Exact value match (multiple strategies)
                    is_match = False
                    
                    # 1. Exact normalized match
                    if value_term_norm == value_norm:
                        is_match = True
                    # 2. One contains the other
                    elif value_term_norm in value_norm or value_norm in value_term_norm:
                        is_match = True
                    # 3. Match without spaces (handles "share money" vs "sharemoney")
                    elif value_term_no_space == value_no_space:
                        is_match = True
                    # 4. Partial no-space match
                    elif len(value_term_no_space) > 4 and (value_term_no_space in value_no_space or value_no_space in value_term_no_space):
                        is_match = True
                    
                    if is_match:
                        score += 70  # HIGH PRIORITY - actual data value
                        matched_values.append(f"{field}={value_str[:30]}")
                        exact_value_field_match = True
                        reasons.append(f"✓ Data contains '{value_term}' in {field}")
                        value_match_count += 1
                        break
                
                # Stop if we found exact match for this field
                if exact_value_field_match:
                    break
                
                # General value matching in query (weaker signal)
                if len(value_norm) > 3 and value_norm in query:
                    score += 12
                    matched_values.append(f"{field}={value_str[:30]}")
                    value_match_count += 1
                # Fuzzy value match
                elif len(value_str) > 5:
                    similarity = calculate_similarity(value_norm, query)
                    if similarity > 0.75:
                        score += 6 * similarity
                        matched_values.append(f"{field}≈{value_str[:30]}")
                        value_match_count += 1
            
            # Continue to next field if found match
            if exact_value_field_match:
                break
        
        # Bonus: If value context matches collection name + field exists
        if value_context_match and matched_fields:
            score += 20
            reasons.append(f"Value context + field structure match")
        
        if value_match_count > 0 and not exact_value_field_match:
            reasons.append(f"Data values matched ({value_match_count} values)")
        
        # 4. Keyword matching (TERTIARY - less weight than actual content)
        if collection_name in collection_keywords:
            keywords = collection_keywords[collection_name]["keywords"]
            matched_keywords = [kw for kw in keywords if kw in query]
            if matched_keywords:
                score += len(matched_keywords) * 5  # Reduced from 10
                reasons.append(f"Keywords: {', '.join(matched_keywords[:2])}")
        
        # SPECIAL RULE: Prevent authentication collection cross-matching
        # If query is about login/signin, penalize non-login collections
        auth_collections = ['authuser', 'login_activities', 'signin_records', 'user_sessions', 
                           'userDeviceDetail', 'otp', 'refresh_token_record', 'refreshtoken', 
                           'roles', 'audit_trail', 'account_deletion_request']
        
        # Only apply authentication-specific penalties if we're actually in authentication database
        if database_name == 'authentication':
            # If query mentions "login" or "signin"
            if any(word in query for word in ['login', 'signin', 'sign in', 'sign-in']):
                if collection_name in ['audit_trail', 'account_deletion_request', 'roles']:
                    score -= 50  # Strong penalty for non-login collections
                    reasons.append(f"❌ Login query - penalty for {collection_name}")
                elif collection_name in ['login_activities', 'signin_records']:
                    score += 40  # Strong bonus for login collections
                    reasons.append(f"✓ Login query - bonus for {collection_name}")
            
            # If query mentions "user details", "authenticate", "user info"
            if any(phrase in query for phrase in ['user details', 'user info', 'authenticate', 'user account', 'user profile']):
                if collection_name == 'authuser':
                    score += 45
                    reasons.append(f"✓ User details query - bonus for authuser")
                elif collection_name in ['audit_trail', 'account_deletion_request']:
                    score -= 40
                    reasons.append(f"❌ User details query - penalty for {collection_name}")
            
            # If query mentions "session"
            if any(word in query for word in ['session', 'sessions']):
                if collection_name == 'user_sessions':
                    score += 40
                    reasons.append(f"✓ Session query - bonus for user_sessions")
                elif collection_name in ['audit_trail', 'login_activities']:
                    score -= 30
                    reasons.append(f"❌ Session query - penalty for {collection_name}")
            
            # If query mentions "otp" or "verification" or "one time password"
            if any(word in query for word in ['otp', 'verification', 'verify', 'one time password', 'one-time password']):
                if collection_name == 'otp':
                    score += 45
                    reasons.append(f"✓ OTP query - bonus for otp")
                elif collection_name != 'payment_otp_verification':
                    score -= 20
                    reasons.append(f"❌ OTP query - penalty for {collection_name}")
            
            # If query mentions "audit", "action log", "system activity"
            if any(word in query for word in ['audit', 'audit trail', 'audit log', 'system activity', 'activity log', 'system action']):
                if collection_name == 'audit_trail':
                    score += 50
                    reasons.append(f"✓ Audit query - bonus for audit_trail")
                elif collection_name in ['login_activities', 'authuser']:
                    score -= 35
                    reasons.append(f"❌ Audit query - penalty for {collection_name}")
            
            # If query mentions "delete account", "account deletion", "deactivate"
            if any(phrase in query for phrase in ['delete account', 'account deletion', 'deactivate', 'account closure']):
                if collection_name == 'account_deletion_request':
                    score += 50
                    reasons.append(f"✓ Deletion query - bonus for account_deletion_request")
                elif collection_name in ['authuser', 'audit_trail']:
                    score -= 40
                    reasons.append(f"❌ Deletion query - penalty for {collection_name}")
        
        # 5. Intent-based scoring with transaction/record context
        # Collection type alignment with query intent
        intent_boosters = []
        
        # Transaction queries - boost transaction collections
        if any(word in query for word in ['transaction', 'transactions', 'transact']):
            if 'transaction' in collection_name.lower():
                score += 25
                intent_boosters.append("transaction intent")
        
        # History/records
        if any(word in query for word in ['history', 'records', 'previous', 'past']):
            if 'history' in collection_name:
                score += 15
                intent_boosters.append("history intent")
        
        # Count/How many questions
        if any(word in query for word in ['how many', 'count', 'total', 'number of']):
            score += 3
        
        # List/Show questions
        if any(word in query for word in ['list', 'show', 'display', 'get', 'all']):
            score += 2
        
        # Specific queries (details, specific)
        if any(word in query for word in ['details', 'detail', 'information', 'info', 'specific']):
            score += 3
        
        # Error/issue queries
        if any(word in query for word in ['error', 'issue', 'problem', 'fail']):
            if 'error' in collection_name.lower():
                score += 15
                intent_boosters.append("error intent")
        
        # Location queries
        if any(word in query for word in ['location', 'address', 'where', 'city', 'state']):
            if 'location' in collection_name.lower():
                score += 15
                intent_boosters.append("location intent")
        
        # Wallet/money queries
        if any(word in query for word in ['wallet', 'balance', 'money', 'amount']):
            if 'wallet' in collection_name.lower():
                score += 10
                intent_boosters.append("wallet intent")
        
        # Deal/offer queries
        if any(word in query for word in ['deal', 'offer', 'discount', 'promotion']):
            if 'deal' in collection_name.lower():
                score += 12
                intent_boosters.append("deal intent")
        
        # Review/rating queries
        if any(word in query for word in ['review', 'rating', 'feedback']):
            if 'review' in collection_name.lower():
                score += 12
                intent_boosters.append("review intent")
        
        if intent_boosters:
            reasons.append(f"Intent: {', '.join(intent_boosters)}")
        
        # Store results
        if score > 0:
            collection_scores[collection_name] = {
                "score": score,
                "reasons": reasons,
                "matched_fields": matched_fields[:5],  # Top 5 matched fields
                "matched_values": matched_values[:3],  # Top 3 matched values
                "total_records": metadata.get("total_records", 0)
            }
    
    # Sort by score
    if not collection_scores:
        return {
            "selected_collection": None,
            "reason": "No collection matched the query",
            "confidence": 0.0
        }
    
    # Get top collection
    sorted_collections = sorted(collection_scores.items(), key=lambda x: x[1]["score"], reverse=True)
    top_collection = sorted_collections[0]
    
    collection_name = top_collection[0]
    details = top_collection[1]
    
    # Calculate confidence (0-1 scale) - adjusted for new scoring
    max_possible_score = 250  # Adjusted: collection+field alignment(50) + value(70) + collection(60) + field(40) + intent(25) + misc(5)
    confidence = min(details["score"] / max_possible_score, 1.0)
    
    # Format reason - prioritize content matches
    reason_parts = details["reasons"]
    if details["matched_fields"]:
        reason_parts.append(f"🔑 Fields: {', '.join(details['matched_fields'][:4])}")
    if details.get("matched_values"):
        reason_parts.append(f"📄 Content: {', '.join(details['matched_values'][:2])}")
    
    reason = " | ".join(reason_parts) if reason_parts else "Best match based on content analysis"
    
    # Return top 3 alternatives
    alternatives = [
        {"collection": name, "score": info["score"], "confidence": min(info["score"] / max_possible_score, 1.0)}
        for name, info in sorted_collections[1:4]
    ]
    
    return {
        "selected_collection": collection_name,
        "reason": reason,
        "confidence": round(confidence, 2),
        "score": details["score"],
        "matched_fields": details["matched_fields"],
        "total_records": details["total_records"],
        "alternatives": alternatives
    }


if __name__ == "__main__":
    print("=" * 70)
    print("🔍 Collection Router - Test Mode")
    print("=" * 70)
    
    # Test queries for customer database
    test_queries_customer = [
        "Show me all customer details",
        "How many transactions are there?",
        "List wallet balance",
        "Get customer locations",
        "Show errors",
        "customer load money history",
        "What are the favourite deals?",
        "Show OTP verification records"
    ]
    
    print("\n📊 Testing CUSTOMER Database Collections:")
    print("-" * 70)
    
    for query in test_queries_customer:
        result = detect_collection(query, "numoni_customer")
        print(f"\n❓ Query: '{query}'")
        print(f"✅ Collection: {result['selected_collection']}")
        print(f"📌 Confidence: {result['confidence']} ({result.get('score', 0)} points)")
        print(f"💡 Reason: {result['reason']}")
        if result.get('matched_fields'):
            print(f"🔑 Key fields: {', '.join(result['matched_fields'][:5])}")
    
    # Test queries for merchant database
    test_queries_merchant = [
        "Show merchant business details",
        "List all bank accounts",
        "Get  merchant locations",
        "Show deals and offers",
        "What are the reviews?",
        "merchant wallet transactions",
        "POS terminal information",
        "Show payout records"
    ]
    
    print("\n\n📊 Testing MERCHANT Database Collections:")
    print("-" * 70)
    
    for query in test_queries_merchant:
        result = detect_collection(query, "numoni_merchant")
        print(f"\n❓ Query: '{query}'")
        print(f"✅ Collection: {result['selected_collection']}")
        print(f"📌 Confidence: {result['confidence']} ({result.get('score', 0)} points)")
        print(f"💡 Reason: {result['reason']}")
        if result.get('matched_fields'):
            print(f"🔑 Key fields: {', '.join(result['matched_fields'][:5])}")
