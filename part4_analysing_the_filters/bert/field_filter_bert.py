#!/usr/bin/env python
"""Advanced field filtering - Select specific columns from any collection"""
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from sentence_transformers import SentenceTransformer, util

# BERT model for semantic field matching
print("🤖 Loading BERT model for field filtering...")
try:
    BERT_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    print("✅ BERT model loaded successfully!")
except Exception as e:
    print(f"⚠️ Warning: Could not load BERT model: {e}")
    BERT_MODEL = None

EMBEDDING_CACHE = {}


def get_embedding(text: str):
    if text in EMBEDDING_CACHE:
        return EMBEDDING_CACHE[text]

    embedding = BERT_MODEL.encode(text, convert_to_tensor=True)
    EMBEDDING_CACHE[text] = embedding
    return embedding


def semantic_similarity(text1: str, text2: str) -> float:
    if not text1 or not text2 or BERT_MODEL is None:
        return 0.0

    try:
        emb1 = get_embedding(text1)
        emb2 = get_embedding(text2)
        return float(util.cos_sim(emb1, emb2).item())
    except Exception:
        return 0.0

def extract_field_names(query: str) -> Optional[List[str]]:
    """Extract field names from query like 'get Customer ID, Sender Name, Transaction Reference'"""
    patterns = [
        # "I need these fields" patterns
        r'(?:i\s+need|these\s+following)\s+(?:fields|columns)[:\s]+([^.!?\n]+?)(?:\s+on\s+|\s+for\s+|$)',
        # "with columns" patterns
        r'with\s+(?:columns|fields)[:\s]+([^.!?\n]+?)(?:\s+on\s+|\s+for\s+|$)',
        # "show/get/display data with" patterns
        r'(?:show|get|display|list)\s+(?:me\s+)?(?:data\s+)?(?:with\s+)?(?:columns|fields)[:\s]*([^.!?\n]+?)(?:\s+on\s+|\s+for\s+|$)',
        # "following fields" patterns
        r'following\s+(?:columns|fields)[:\s]*(.+?)(?:\s+on\s+|\s+for\s+|$)',
        # "these fields" patterns
        r'these\s+(?:columns|fields)[:\s]*(.+?)(?:\s+on\s+|\s+for\s+|$)',
        # *** NEW PATTERNS - "I only want" / "show me only" variations ***
        r'i\s+(?:only\s+)?want\s+(?:these\s+)?(?:fields|columns)?[:\s]*([a-z\s,]+?)(?:\s+on\s+|\s+for\s+|only|$)',
        r'(?:show|display|give)\s+me\s+only\s+(?:these\s+)?(?:fields|columns)?[:\s]*([a-z\s,]+?)(?:\s+on\s+|\s+for\s+|$)',
        r'only\s+(?:fields|columns)?[:\s]*([a-z\s,]+?)(?:\s+on\s+|\s+for\s+|$)',
        r'(?:just|only)\s+(?:the\s+)?(?:fields|columns)?[:\s]*([a-z\s,]+?)(?:\s+on\s+|\s+for\s+|$)',
        # "show me [fields] in/from/where" - extract comma-separated list before spatial keyword
        r'(?:show|get)\s+me\s+([a-z\s,]+?)(?:\s+in\s+|\s+from\s+|\s+where\s+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            fields_text = match.group(1)
            # Split by both comma and " and " keyword
            fields = re.split(r',\s*|\s+and\s+', fields_text, flags=re.IGNORECASE)
            fields = [f.strip() for f in fields]
            fields = [f for f in fields if len(f) > 1 and len(f) < 50]
            if len(fields) > 0:
                return fields
    return None

def normalize_field_name(field_name: str, record: Dict) -> str:
    """Match user field name to actual database field name with smart mapping + semantic spelling"""
    field_lower = field_name.lower().strip()
    
    # Smart field name mapping - handles common variations AND misspellings
    smart_mapping = {
        'customer id': ['customerId', 'customerUserId', 'userId'],
        'sender name': ['senderName', 'name'],
        'receiver name': ['receiverName', 'recipientName'],
        'reciever name': ['receiverName', 'recipientName'],  # Common misspelling
        'transaction reference': ['transactionReferenceId', 'sourceTransactionId', 'reference'],
        'transaction ref': ['transactionReferenceId', 'sourceTransactionId'],
        'transaction id': ['transactionReferenceId', 'transactionId'],
        'total amount': ['totalAmountPaid', 'amount', 'totalAmount'],
        'amount': ['totalAmountPaid', 'amount'],
        'sender id': ['senderId'],
        'receiver id': ['receiverId'],
        'reciever id': ['receiverId'],  # Common misspelling
        'transaction name': ['transactionName'],
        'transaction type': ['transactionType'],
        'status': ['status'],
        'date': ['transactionDate', 'createdDate', 'createdDt'],
        'transaction date': ['transactionDate'],
        'created date': ['createdDate', 'createdDt'],
        'wallet type': ['walletType'],
        'balance': ['balance', 'amount'],
        'region': ['region'],
        'state': ['state'],
        'country': ['country'],
    }
    
    # 1. Exact match in record
    for key in record.keys():
        if key.lower() == field_lower:
            return key
    
    # 2. Check smart mapping
    for user_pattern, db_fields in smart_mapping.items():
        if user_pattern == field_lower:
            for db_field in db_fields:
                if db_field in record:
                    return db_field
    
    # 3. Semantic spelling match for common misspellings (e.g., "reciever" → "receiver")
    for user_pattern, db_fields in smart_mapping.items():
        similarity = semantic_similarity(field_lower, user_pattern)
        if similarity > 0.72:
            for db_field in db_fields:
                if db_field in record:
                    return db_field
    
    # 4. Substring match as fallback
    for key in record.keys():
        if field_lower in key.lower():
            return key
        if key.lower() in field_lower:
            return key
    
    return None

def find_all_matching_fields(user_fields: List[str], record: Dict) -> Dict[str, str]:
    """Find all database fields matching user-requested fields"""
    mapping = {}
    for user_field in user_fields:
        matched = normalize_field_name(user_field, record)
        if matched:
            mapping[user_field] = matched
    return mapping

def extract_date_range(query: str) -> Optional[tuple]:
    """Extract date or date range from query. Returns (start_date, end_date)"""
    query_lower = query.lower()
    now = datetime.now()

    if 'last year' in query_lower:
        start = datetime(now.year - 1, 1, 1)
        end = datetime(now.year - 1, 12, 31, 23, 59, 59)
        return (start, end)

    if 'this year' in query_lower:
        start = datetime(now.year, 1, 1)
        end = datetime(now.year, 12, 31, 23, 59, 59)
        return (start, end)

    if 'this month' in query_lower:
        start = datetime(now.year, now.month, 1)
        if now.month == 12:
            next_month_start = datetime(now.year + 1, 1, 1)
        else:
            next_month_start = datetime(now.year, now.month + 1, 1)
        end = next_month_start - timedelta(seconds=1)
        return (start, end)

    if 'last month' in query_lower:
        this_month_start = datetime(now.year, now.month, 1)
        last_month_end = this_month_start - timedelta(seconds=1)
        last_month_start = datetime(last_month_end.year, last_month_end.month, 1)
        return (last_month_start, last_month_end)
    
    month_map = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }

    # Pattern: "from February 1 to 19 2026" (same month range)
    same_month_range = re.search(
        r'from\s+([a-zA-Z]+)\s+(\d{1,2})(?:st|nd|rd|th)?\s*to\s*(\d{1,2})(?:st|nd|rd|th)?(?:\s*,?\s*(\d{4}))?',
        query,
        re.IGNORECASE
    )
    if same_month_range:
        month_str, start_day, end_day, year_str = same_month_range.groups()
        month = month_map.get(month_str.lower()[:3])
        if month:
            year_val = int(year_str) if year_str else now.year
            try:
                start = datetime(year_val, month, int(start_day))
                end = datetime(year_val, month, int(end_day), 23, 59, 59)
                if start <= end:
                    return (start, end)
            except:
                pass

    # Pattern: "from Jan 28 to Feb 3 2026" (cross-month range)
    cross_month_range = re.search(
        r'from\s+([a-zA-Z]+)\s+(\d{1,2})(?:st|nd|rd|th)?\s*to\s*([a-zA-Z]+)\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s*,?\s*(\d{4}))?',
        query,
        re.IGNORECASE
    )
    if cross_month_range:
        start_month_str, start_day, end_month_str, end_day, year_str = cross_month_range.groups()
        start_month = month_map.get(start_month_str.lower()[:3])
        end_month = month_map.get(end_month_str.lower()[:3])
        if start_month and end_month:
            year_val = int(year_str) if year_str else now.year
            try:
                start = datetime(year_val, start_month, int(start_day))
                end = datetime(year_val, end_month, int(end_day), 23, 59, 59)
                if start <= end:
                    return (start, end)
            except:
                pass

    # Extract year if present (2025, 2026, etc)
    year_match = re.search(r'\b(202[0-9]|201[0-9])\b', query)
    year = int(year_match.group(1)) if year_match else 2026
    
    # Parse "on 16th of Feb" or "on February 16" or "on 2025-02-16"
    date_patterns = [
        r'on\s+(\d{1,2})(?:th|st|nd|rd)?\s+(?:of\s+)?(\w+)',  # 16th of Feb
        r'on\s+(\w+)\s+(\d{1,2})',  # February 16
        r'on\s+(\d{4}-\d{2}-\d{2})',  # 2025-02-16
        r'(\d{4}-\d{2}-\d{2})',  # 2025-02-16
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            try:
                if len(match.groups()) == 2:
                    part1, part2 = match.groups()
                    # Try day-month format
                    if part1.isdigit() and len(part1) <= 2:
                        day = int(part1)
                        month_str = part2.lower()[:3]
                        month = month_map.get(month_str, None)
                        if month:
                            date = datetime(year, month, day)
                            return (date, date)
                    # Try month-day format
                    elif part2.isdigit() and len(part2) <= 2:
                        day = int(part2)
                        month_str = part1.lower()[:3]
                        month = month_map.get(month_str, None)
                        if month:
                            date = datetime(year, month, day)
                            return (date, date)
                else:
                    date_str = match.group(1)
                    date = datetime.fromisoformat(date_str)
                    return (date, date)
            except:
                pass
    
    return None

def filter_by_fields(data: List[Dict], field_names: List[str]) -> List[Dict]:
    """Filter data to include only specified fields"""
    if not data or not field_names:
        return data
    
    filtered = []
    for record in data:
        filtered_record = {}
        for user_field in field_names:
            actual_field = normalize_field_name(user_field, record)
            if actual_field:
                filtered_record[actual_field] = record[actual_field]
        
        if filtered_record:
            filtered.append(filtered_record)
    
    return filtered

def filter_by_date(data: List[Dict], start_date: datetime, end_date: datetime, 
                   date_field: str = 'transactionDate') -> List[Dict]:
    """Filter data by date range"""
    filtered = []
    
    for record in data:
        if date_field not in record:
            continue
        
        value = record[date_field]
        try:
            if isinstance(value, str):
                record_date = datetime.fromisoformat(value.replace('Z', '+00:00'))
            elif isinstance(value, dict) and '$date' in value:
                record_date = datetime.fromisoformat(value['$date'].replace('Z', '+00:00'))
            else:
                continue
            
            record_date = record_date.replace(tzinfo=None)
            start = start_date.replace(tzinfo=None)
            end = end_date.replace(tzinfo=None)
            
            if start <= record_date.date() <= end:
                filtered.append(record)
        except:
            pass
    
    return filtered

def apply_field_filters(data: List[Dict], query: str) -> Dict[str, Any]:
    """Main function - apply field selection and date filtering from query"""
    
    if not data:
        return {'error': 'No data provided', 'filtered': []}
    
    # Extract field names
    field_names = extract_field_names(query)
    filtered_data = data
    date_filter_applied = False
    
    # Apply field filtering
    if field_names:
        filtered_data = filter_by_fields(filtered_data, field_names)
        selected_fields = field_names
    else:
        selected_fields = list(data[0].keys()) if data else []
    
    # Extract and apply date filtering
    date_range = extract_date_range(query)
    if date_range:
        start_date, end_date = date_range
        # Auto-detect date field
        date_field = None
        for field in ['transactionDate', 'createdDate', 'createdDt', 'date']:
            if field in (data[0] if data else {}):
                date_field = field
                break
        
        if date_field:
            filtered_data = filter_by_date(filtered_data, start_date, end_date, date_field)
            date_filter_applied = True
    
    return {
        'filtered': filtered_data,
        'fields_selected': bool(field_names),
        'selected_fields': selected_fields if field_names else None,
        'date_filtered': date_filter_applied,
        'total_records': len(filtered_data)
    }
