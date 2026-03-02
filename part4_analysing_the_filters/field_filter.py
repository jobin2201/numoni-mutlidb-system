#!/usr/bin/env python
"""Advanced field filtering - Select specific columns from any collection"""
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from difflib import SequenceMatcher


FIELD_STOP_WORDS = {
    "the", "a", "an", "and", "or", "of", "for", "to", "in", "on", "at", "by", "with",
    "my", "our", "their", "his", "her", "its", "all", "only", "just",
}


def _compact_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (text or "").lower())


def _clean_user_field_phrase(text: str) -> str:
    raw = " ".join((text or "").lower().strip().split())
    raw = re.sub(r"[^a-z0-9_\s]", " ", raw)
    raw = re.sub(r"\b(?:which|that)\s+(?:is|are|was|were)\b.*$", "", raw).strip()
    raw = re.sub(r"\b(?:where|having)\b.*$", "", raw).strip()
    tokens = [token for token in raw.split() if token and token not in FIELD_STOP_WORDS]
    cleaned = " ".join(tokens)
    return cleaned.strip()

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
        # "include ..." patterns (e.g., "..., include a, b, c")
        r'include\s+([^.!?\n]+?)(?:\s+on\s+|\s+for\s+|[.!?]|$)',
        # "including ..." patterns
        r'including\s+([^.!?\n]+?)(?:\s+on\s+|\s+for\s+|[.!?]|$)',
        # *** NEW PATTERNS - "I only want" / "show me only" variations ***
        r'i\s+(?:only\s+)?want\s+(?:these\s+)?(?:fields|columns)?[:\s]*([a-z\s,]+?)(?:\s+on\s+|\s+for\s+|only|[.!?]|$)',
        r'(?:show|display|give)\s+me\s+only\s+(?:these\s+)?(?:fields|columns)?[:\s]*([a-z\s,]+?)(?:\s+on\s+|\s+for\s+|[.!?]|$)',
        r'only\s+(?:fields|columns)?[:\s]*([a-z\s,]+?)(?:\s+on\s+|\s+for\s+|[.!?]|$)',
        r'(?:just|only)\s+(?:the\s+)?(?:fields|columns)?[:\s]*([a-z\s,]+?)(?:\s+on\s+|\s+for\s+|[.!?]|$)',
        # "show me [fields] in/from/where" - extract comma-separated list before spatial keyword
        r'(?:show|get)\s+me\s+([a-z\s,]+?)(?:\s+in\s+|\s+from\s+|\s+where\s+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            fields_text = match.group(1)
            # Guardrail for broad "show me ... from ..." capture to avoid report-title false positives
            if pattern == r'(?:show|get)\s+me\s+([a-z\s,]+?)(?:\s+in\s+|\s+from\s+|\s+where\s+)':
                looks_like_list = (',' in fields_text) or bool(re.search(r'\s+and\s+', fields_text, re.IGNORECASE))
                if not looks_like_list:
                    continue
            # Split by both comma and " and " keyword
            fields = re.split(r',\s*|\s+and\s+', fields_text, flags=re.IGNORECASE)
            cleaned_fields: List[str] = []
            for field in fields:
                cleaned = _clean_user_field_phrase(field)
                if len(cleaned) > 1 and len(cleaned) < 50:
                    cleaned_fields.append(cleaned)
            generic_singletons = {'merchant', 'customer', 'transaction', 'payout', 'wallet', 'report'}
            if len(cleaned_fields) > 1:
                cleaned_fields = [f for f in cleaned_fields if f not in generic_singletons]

            expanded_fields: List[str] = []
            detail_expansions = {
                'customer details': ['customer name', 'customer id'],
                'merchant details': ['merchant name', 'merchant id'],
                'pos details': ['pos id', 'pos name'],
            }
            for field_name in cleaned_fields:
                if field_name in detail_expansions:
                    for expanded in detail_expansions[field_name]:
                        if expanded not in expanded_fields:
                            expanded_fields.append(expanded)
                else:
                    if field_name not in expanded_fields:
                        expanded_fields.append(field_name)

            fields = expanded_fields
            if len(fields) > 0:
                return fields
    return None

def normalize_field_name(field_name: str, record: Dict) -> str:
    """Match user field name to actual database field name with smart mapping + fuzzy spelling"""
    field_lower = _clean_user_field_phrase(field_name)
    field_compact = _compact_text(field_lower)
    
    # Smart field name mapping - handles common variations AND misspellings
    smart_mapping = {
        'customer id': ['customerId', 'customerUserId', 'userId'],
        'merchant id': ['merchantId', 'merchantUserId', 'userId'],
        'pos id': ['posId'],
        'pos name': ['posName'],
        'sender name': ['senderName', 'name'],
        'receiver name': ['receiverName', 'recipientName'],
        'reciever name': ['receiverName', 'recipientName'],  # Common misspelling
        'transaction reference': ['transactionReferenceId', 'sourceTransactionId', 'reference'],
        'transaction reference id': ['transactionReferenceId', 'sourceTransactionId', 'reference'],
        'reference id': ['transactionReferenceId', 'sourceTransactionId', 'reference'],
        'reference': ['transactionReferenceId', 'sourceTransactionId', 'reference'],
        'transaction ref': ['transactionReferenceId', 'sourceTransactionId'],
        'transaction id': ['transactionReferenceId', 'transactionId'],
        'reference ids': ['transactionReferenceId', 'sourceTransactionId', 'reference'],
        'total amount': ['totalAmountPaid', 'amount', 'totalAmount'],
        'total sales': ['totalAmountPaid', 'amount', 'totalAmount'],
        'order amount': ['totalAmountPaid', 'amount', 'totalAmount'],
        'transaction amount': ['trn_in_amount', 'amount', 'amountPaid', 'transactionAmount', 'totalAmountPaid', 'totalAmount'],
        'commission deducted': ['merchantFee', 'transactionFees', 'fee'],
        'commission': ['merchantFee', 'transactionFees', 'fee'],
        'payout amount': ['payoutAmount', 'amount', 'transactionAmount'],
        'amount': ['totalAmountPaid', 'amount'],
        'sender id': ['senderId'],
        'receiver id': ['receiverId'],
        'reciever id': ['receiverId'],  # Common misspelling
        'transaction name': ['transactionName'],
        'deal name': ['transactionName', 'title'],
        'transaction type': ['transactionType'],
        'status': ['status'],
        'settlement status': ['status', 'settlementStatus', 'payoutStatus'],
        'date': ['transactionDate', 'createdDate', 'createdDt'],
        'login date': ['loginDate', 'activityTime', 'lastLoginAt', 'createdDt', 'transactionDate', 'date'],
        'transaction date': ['transactionDate'],
        'created date': ['createdDate', 'createdDt'],
        'wallet balance before': ['before_balance', 'balanceBeforeTransaction', 'balanceBefore'],
        'wallet balance after': ['after_balance', 'balanceAfterTransaction', 'balanceAfter'],
        'wallet balance': ['walletBalance', 'balance_amount', 'balance', 'amountOnHold', 'amount'],
        'balance before': ['before_balance', 'balanceBeforeTransaction', 'balanceBefore'],
        'balance after': ['after_balance', 'balanceAfterTransaction', 'balanceAfter'],
        'wallet type': ['walletType'],
        'balance': ['balance', 'amount'],
        'region': ['region'],
        'state': ['state'],
        'country': ['country'],
        'bank name': ['bankname', 'bankName', 'accountHolderName'],
        'bank code': ['bankcode', 'bankCode', 'bankTransferCode'],
        'customer details': ['customerName', 'customerId', 'customerUserId', 'name'],
        'merchant details': ['merchantName', 'merchantId', 'businessName', 'brandName'],
        'pos details': ['posName', 'posId'],
        'payout status': ['status'],
        'account number': ['accountNo', 'accountNumber'],
        'minimum spent amount': ['minimumSpentAmount'],
        'minimum spent': ['minimumSpentAmount'],
        'min spent amount': ['minimumSpentAmount'],
        'spent amount': ['minimumSpentAmount'],
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
                for actual_key in record.keys():
                    if _compact_text(actual_key) == _compact_text(db_field):
                        return actual_key

    # 3. Fuzzy spelling match for common misspellings (e.g., "reciever" → "receiver")
    for user_pattern, db_fields in smart_mapping.items():
        # Calculate similarity between user input and pattern
        similarity = SequenceMatcher(None, field_lower, _clean_user_field_phrase(user_pattern)).ratio()
        if similarity > 0.75:  # 75% match threshold
            for db_field in db_fields:
                if db_field in record:
                    return db_field
                for actual_key in record.keys():
                    if _compact_text(actual_key) == _compact_text(db_field):
                        return actual_key
    
    # 4. Substring match as fallback
    for key in record.keys():
        key_lower = key.lower()
        key_compact = _compact_text(key)
        if field_lower and field_lower in key_lower:
            return key
        if key_lower and key_lower in field_lower:
            return key
        if field_compact and (field_compact in key_compact or key_compact in field_compact):
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

    today_start = datetime(now.year, now.month, now.day)
    today_end = datetime(now.year, now.month, now.day, 23, 59, 59)

    if 'today' in query_lower:
        return (today_start, today_end)

    if 'yesterday' in query_lower:
        y_start = today_start - timedelta(days=1)
        y_end = today_end - timedelta(days=1)
        return (y_start, y_end)

    days_back_match = re.search(r'\b(\d+)\s+days?\s+(?:back|behind|ago)\b', query_lower)
    if days_back_match:
        days = int(days_back_match.group(1))
        start = today_start - timedelta(days=days)
        end = today_end - timedelta(days=days)
        return (start, end)

    if re.search(r'\b(?:a|an|one)\s+day\s+(?:back|behind|ago)\b', query_lower):
        start = today_start - timedelta(days=1)
        end = today_end - timedelta(days=1)
        return (start, end)

    if re.search(r'\b(?:a|an|one)\s+week\s+(?:back|behind|ago)\b', query_lower):
        start = today_start - timedelta(days=7)
        end = today_end - timedelta(days=7)
        return (start, end)

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

    def _parse_human_date_phrase(date_text: str, default_year: Optional[int] = None) -> Optional[datetime]:
        cleaned = (date_text or '').strip().lower()
        cleaned = re.sub(r'[,]+', ' ', cleaned)
        cleaned = re.sub(r'\b(\d{1,2})(st|nd|rd|th)\b', r'\1', cleaned)
        cleaned = ' '.join(cleaned.split())

        # month day [year] -> dec 1 2025
        match = re.match(r'^([a-zA-Z]+)\s+(\d{1,2})(?:\s+(\d{4}))?$', cleaned)
        if match:
            month_str, day_str, year_str = match.groups()
            month = month_map.get(month_str[:3])
            year_val = int(year_str) if year_str else (default_year or now.year)
            if month:
                try:
                    return datetime(year_val, month, int(day_str))
                except Exception:
                    return None

        # day month [year] -> 1 dec 2025
        match = re.match(r'^(\d{1,2})\s+([a-zA-Z]+)(?:\s+(\d{4}))?$', cleaned)
        if match:
            day_str, month_str, year_str = match.groups()
            month = month_map.get(month_str[:3])
            year_val = int(year_str) if year_str else (default_year or now.year)
            if month:
                try:
                    return datetime(year_val, month, int(day_str))
                except Exception:
                    return None

        # year month day -> 2025 dec 1
        match = re.match(r'^(\d{4})\s+([a-zA-Z]+)\s+(\d{1,2})$', cleaned)
        if match:
            year_str, month_str, day_str = match.groups()
            month = month_map.get(month_str[:3])
            if month:
                try:
                    return datetime(int(year_str), month, int(day_str))
                except Exception:
                    return None

        return None

    # Pattern: "Dec 1 2025 to Feb 19 2026" / "from Dec 1 2025 to Feb 19 2026"
    explicit_range = re.search(
        r'(?:\bfrom\b\s+|\bbetween\b\s+)?([a-zA-Z]+\s+\d{1,2}(?:st|nd|rd|th)?(?:\s*,?\s*\d{4})?|\d{1,2}\s+[a-zA-Z]+(?:\s*,?\s*\d{4})?|\d{4}\s+[a-zA-Z]+\s+\d{1,2})\s+(?:to|and)\s+([a-zA-Z]+\s+\d{1,2}(?:st|nd|rd|th)?(?:\s*,?\s*\d{4})?|\d{1,2}\s+[a-zA-Z]+(?:\s*,?\s*\d{4})?|\d{4}\s+[a-zA-Z]+\s+\d{1,2})',
        query,
        re.IGNORECASE
    )
    if explicit_range:
        left_text, right_text = explicit_range.groups()
        # If one side has year and the other doesn't, borrow year from the explicit side
        left_year_match = re.search(r'\b(20\d{2})\b', left_text)
        right_year_match = re.search(r'\b(20\d{2})\b', right_text)
        left_default_year = int(right_year_match.group(1)) if right_year_match and not left_year_match else None
        right_default_year = int(left_year_match.group(1)) if left_year_match and not right_year_match else None

        start = _parse_human_date_phrase(left_text, default_year=left_default_year)
        end = _parse_human_date_phrase(right_text, default_year=right_default_year)
        if start and end and start <= end:
            return (start, datetime(end.year, end.month, end.day, 23, 59, 59))

    # Pattern: "from January 2026" / "in January 2026" / "January 2026"
    month_year_match = re.search(
        r'\b(?:from|in)?\s*(january|february|march|april|may|june|july|august|september|october|november|december)\s*,?\s*(20\d{2})\b',
        query,
        re.IGNORECASE
    )
    if month_year_match:
        month_str, year_str = month_year_match.groups()
        month = month_map.get(month_str.lower()[:3])
        year_val = int(year_str)
        if month:
            start = datetime(year_val, month, 1)
            if month == 12:
                end = datetime(year_val + 1, 1, 1) - timedelta(seconds=1)
            else:
                end = datetime(year_val, month + 1, 1) - timedelta(seconds=1)
            return (start, end)

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

    # Pattern: "between February 1 and February 19 2026" / "between Feb 1 and 19 2026"
    between_range = re.search(
        r'between\s+([a-zA-Z]+)\s+(\d{1,2})(?:st|nd|rd|th)?\s+and\s+(?:([a-zA-Z]+)\s+)?(\d{1,2})(?:st|nd|rd|th)?(?:\s*,?\s*(\d{4}))?',
        query,
        re.IGNORECASE
    )
    if between_range:
        start_month_str, start_day, end_month_str, end_day, year_str = between_range.groups()
        start_month = month_map.get(start_month_str.lower()[:3])
        end_month = month_map.get((end_month_str or start_month_str).lower()[:3])
        if start_month and end_month:
            year_val = int(year_str) if year_str else now.year
            try:
                start = datetime(year_val, start_month, int(start_day))
                end = datetime(year_val, end_month, int(end_day), 23, 59, 59)
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
    year = int(year_match.group(1)) if year_match else now.year
    
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
                            inferred_year = year
                            if not year_match:
                                tentative = datetime(inferred_year, month, day)
                                if tentative > now:
                                    inferred_year -= 1
                            start = datetime(inferred_year, month, day)
                            end = datetime(inferred_year, month, day, 23, 59, 59)
                            return (start, end)
                    # Try month-day format
                    elif part2.isdigit() and len(part2) <= 2:
                        day = int(part2)
                        month_str = part1.lower()[:3]
                        month = month_map.get(month_str, None)
                        if month:
                            inferred_year = year
                            if not year_match:
                                tentative = datetime(inferred_year, month, day)
                                if tentative > now:
                                    inferred_year -= 1
                            start = datetime(inferred_year, month, day)
                            end = datetime(inferred_year, month, day, 23, 59, 59)
                            return (start, end)
                else:
                    date_str = match.group(1)
                    date = datetime.fromisoformat(date_str)
                    return (date, datetime(date.year, date.month, date.day, 23, 59, 59))
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
