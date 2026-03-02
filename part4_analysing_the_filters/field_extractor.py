#!/usr/bin/env python
"""
Field Extractor - Extract requested fields from natural language queries
Handles patterns like: "show me customer details, I want name, phone, date"
Does NOT interfere with "show me" queries - only extracts fields if present
Distinguishes between field names and location/value filters
"""
import re
from typing import List, Optional

# Common field/column names that are likely actual database columns
LIKELY_FIELD_NAMES = {
    'name', 'id', 'phone', 'email', 'date', 'amount', 'count', 'status',
    'region', 'state', 'country', 'code', 'type', 'description', 'value',
    'customer', 'merchant', 'bank', 'user', 'address', 'city', 'zip',
    'created', 'updated', 'modified', 'settled', 'paid', 'balance',
    'transaction', 'order', 'payment', 'fee', 'commission', 'revenue',
    'firstname', 'lastname', 'username', 'password', 'account', 'number',
    'rating', 'review', 'comment', 'title', 'subject', 'content', 'heading',
    'lga', 'area',  # Added: LGA (Local Government Area) for Nigeria
    'currency', 'reference', 'payout',  # Added: Payment fields
}

# Keywords that indicate location/value filters, NOT field names
FILTER_KEYWORDS = {
    'abia', 'lagos', 'abuja', 'naija', 'nigeria', 'ghana', 'kano',
    'for', 'where', 'in', 'on', 'at', 'from', 'to', 'by',
}

def is_likely_field_name(text: str) -> bool:
    """Check if text looks like actual field/column names"""
    text_lower = text.lower().strip()
    
    # Remove "for " prefix if present (user might say "for state and lga")
    if text_lower.startswith('for '):
        text_lower = text_lower[4:].strip()
    
    # Count how many recognized field names are in the text
    field_count = sum(1 for field in LIKELY_FIELD_NAMES if field in text_lower)
    
    # If it has recognizable field names, it's likely field names
    if field_count >= 2:
        return True
    
    # Check if it has location keywords (which would make it a location filter)
    location_keywords = ['abia', 'lagos', 'abuja', 'naija', 'nigeria', 'ghana', 'kano']
    has_location = any(loc in text_lower for loc in location_keywords)
    
    # If it has location keywords but no other field names, it's likely a location filter
    if has_location and field_count <= 1:
        return False
    
    # Check if it looks like CamelCase or underscored field name
    if '_' in text or (text[0].isupper() if text else False):
        return True
    
    # If text is short and simple, might be a single field name
    words = text_lower.split()
    if len(words) <= 2:  # Max 2 words for a single field
        # Check if no location keywords
        if not has_location:
            return True
    
    return False

def extract_requested_fields(query: str) -> Optional[List[str]]:
    """
    Extract fields requested in a query without affecting show me queries
    Distinguishes between field names and location/value filters
    
    Patterns handled:
    - "show me X, I want field1, field2"
    - "show me X, I need field1, field2"
    - "show me X, I only want field1, field2"
    - "show me X, looking for field1, field2"
    
    DOES NOT match:
    - "show me X, I only want for Abia State" (location filter)
    - "show me X, I want in Lagos" (location filter)
    
    Returns: List of field names if found, None otherwise
    """
    query_lower = query.lower()
    
    # Pattern 1: "...I want field1, field2, field3" (after comma/period/whitespace, with or without "only")
    want_pattern = r'[,\.\s]\s*i\s+(?:only\s+)?want\s+([a-z,\s]+?)(?:\s*$)'
    match = re.search(want_pattern, query_lower)
    if match:
        fields_text = match.group(1)
        # Only proceed if this looks like actual field names, not a location filter
        if is_likely_field_name(fields_text):
            fields = parse_fields_from_text(fields_text)
            if fields:
                return fields
    
    # Pattern 2: "...I need field1, field2, field3" (after comma/period/whitespace)
    need_pattern = r'[,\.\s]\s*i\s+need\s+([a-z,\s]+?)(?:\s*$)'
    match = re.search(need_pattern, query_lower)
    if match:
        fields_text = match.group(1)
        if is_likely_field_name(fields_text):
            fields = parse_fields_from_text(fields_text)
            if fields:
                return fields
    
    # Pattern 3: "...looking for field1, field2" (after comma/period/whitespace)
    looking_pattern = r'[,\.\s]\s*looking\s+for\s+([a-z,\s]+?)(?:\s*$)'
    match = re.search(looking_pattern, query_lower)
    if match:
        fields_text = match.group(1)
        if is_likely_field_name(fields_text):
            fields = parse_fields_from_text(fields_text)
            if fields:
                return fields
    
    # Pattern 4: "...with field1, field2" (after comma/period/whitespace)
    with_pattern = r'[,\.\s]\s*with\s+([a-z,\s]+?)(?:\s*$)'
    match = re.search(with_pattern, query_lower)
    if match:
        fields_text = match.group(1)
        # Make sure this is not "with columns/fields"
        if 'column' not in fields_text and 'field' not in fields_text:
            if is_likely_field_name(fields_text):
                fields = parse_fields_from_text(fields_text)
                if fields and len(fields) > 1:  # Only if multiple fields
                    return fields
    
    return None


def parse_fields_from_text(text: str) -> Optional[List[str]]:
    """
    Parse field names from text, handling multiple separators
    Removes "for" and "and" keywords, returns lowercase field names
    
    Examples:
    - "name, phone, email" -> ["name", "phone", "email"]
    - "for state and lga" -> ["state", "lga"]
    - "name and phone and email" -> ["name", "phone", "email"]
    - "name, phone and email" -> ["name", "phone", "email"]
    """
    if not text or len(text.strip()) == 0:
        return None
    
    # Remove "for" prefix if text starts with it
    text_cleaned = text.strip()
    if text_cleaned.startswith('for '):
        text_cleaned = text_cleaned[4:].strip()  # Remove "for "
    
    # Also remove standalone "and" that appears at start after removing "for"
    if text_cleaned.startswith('and '):
        text_cleaned = text_cleaned[4:].strip()
    
    # Split by comma or "and"
    fields = re.split(r',\s*|\s+and\s+', text_cleaned, flags=re.IGNORECASE)
    
    # Clean up fields
    cleaned_fields = []
    for field in fields:
        field = field.strip().lower()  # Convert to lowercase for standardization
        # Skip very short or very long field names, and common words
        if len(field) > 1 and len(field) < 50 and field not in ['the', 'a', 'an', 'or', 'for']:
            cleaned_fields.append(field)
    
    return cleaned_fields if len(cleaned_fields) > 0 else None


def map_fields_to_columns(requested_fields: List[str], available_columns: List[str]) -> dict:
    """
    Map user-requested field names to exact DataFrame column names
    Returns dictionary: {user_field: actual_column_name}
    
    Handles:
    - Case-insensitive matching
    - Space vs camelCase matching (e.g., "merchant fee" → "merchantFee")
    - Partial matching (e.g., "state" matches "state", "State", "state_name", etc)
    - Returns actual column name from DataFrame
    
    Example:
    requested_fields = ["state", "lga", "merchant fee"]
    available_columns = ["_id", "region", "state", "lga", "merchantFee", "population"]
    returns: {"state": "state", "lga": "lga", "merchant fee": "merchantFee"}
    """
    if not requested_fields or not available_columns:
        return {}
    
    field_mapping = {}
    
    # Helper function to normalize field names for comparison
    def normalize(text):
        """Remove spaces, underscores, hyphens and convert to lowercase"""
        return text.lower().replace(' ', '').replace('_', '').replace('-', '')
    
    for user_field in requested_fields:
        user_field_lower = user_field.lower().strip()
        user_field_normalized = normalize(user_field)
        matched_column = None
        
        # 1. Try exact case-insensitive match
        for col in available_columns:
            if col.lower() == user_field_lower:
                matched_column = col
                break
        
        # 2. Try normalized match (handles "merchant fee" → "merchantFee", "customer_name" → "customerName")
        if not matched_column:
            for col in available_columns:
                if normalize(col) == user_field_normalized:
                    matched_column = col
                    break
        
        # 3. Try partial match (user field is substring of column name)
        if not matched_column:
            for col in available_columns:
                col_lower = col.lower()
                if user_field_lower in col_lower or col_lower in user_field_lower:
                    matched_column = col
                    break
        
        # 4. Store the mapping (even if not matched, store user field)
        if matched_column:
            field_mapping[user_field] = matched_column
        else:
            # Store original field name if no match found
            field_mapping[user_field] = user_field
    
    return field_mapping
