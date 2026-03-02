#!/usr/bin/env python
"""
Advanced Filter Executor - Part 4
Applies date, location, and numeric filters to collection data
"""
import re
from datetime import datetime
from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer, util

# BERT model for semantic text filtering
print("🤖 Loading BERT model for advanced text filters...")
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


def apply_advanced_filters(data: List[Dict], filters: Dict[str, Any], collection_name: str = "", use_updated_date: bool = False) -> tuple:
    """
    Apply advanced filters to data
    
    Args:
        data: List of records
        filters: Dictionary with date_filters, location_filters, numeric_filters, text_filters
        collection_name: Name of the collection (for validation messages)
    
    Returns:
        (filtered_data, validation_messages)
    """
    filtered = data
    messages = []
    
    # Apply date filters
    if filters.get('date_filters'):
        # Check if data has date fields
        date_fields = find_date_fields(data)
        
        if not date_fields:
            # Check if we should suggest a different collection
            if filters.get('suggested_collection'):
                messages.append(f"⚠️ Current collection '{collection_name}' has no date fields. " + 
                              f"Consider using '{filters['suggested_collection']}' instead " +
                              f"({filters.get('collection_reason', '')})")
            else:
                messages.append(f"⚠️ Collection '{collection_name}' has no date fields (createdDt, createdDate, etc.). Cannot apply date filters.")
        else:
            # Check if we should prefer updatedDt over createdDt
            prefer_updated = filters.get('text_filters', {}).get('use_updated_date', False) or use_updated_date
            filtered = apply_date_filters(filtered, filters['date_filters'], prefer_updated=prefer_updated)
            messages.append(f"[OK] Date filter applied using field: {date_fields[0]}")
    
    # Apply location filters (skip for location-based collections)
    if filters.get('location_filters'):
        # Skip location filtering for location-based collections
        # These collections are ABOUT locations, not records AT locations
        is_location_collection = any(
            loc_name in collection_name.lower() 
            for loc_name in ['region', 'location', 'area', 'lga', 'state', 'city']
        )
        
        if not is_location_collection:
            location_fields_found = check_location_fields(data)
            filtered = apply_location_filters(filtered, filters['location_filters'])
            if location_fields_found:
                messages.append(f"[OK] Location filter applied")
            else:
                messages.append(f"⚠️ No location fields found in this collection")
        else:
            # For location collections, the location in the query is already part of the context
            messages.append(f"[OK] Location context recognized (collection contains location data)")
    
    # Apply numeric filters
    if filters.get('numeric_filters'):
        for numeric_filter in filters['numeric_filters']:
            filtered = apply_numeric_filter(filtered, numeric_filter)
        messages.append(f"[OK] Numeric filter applied")
    
    # Apply text filters
    if filters.get('text_filters'):
        filtered = apply_text_filters(filtered, filters['text_filters'])
        messages.append(f"[OK] Text filter applied")
    
    return filtered, messages


def check_location_fields(data: List[Dict]) -> bool:
    """Check if data has location fields"""
    if not data:
        return False
    
    location_fields = ['city', 'state', 'country', 'location', 'address', 
                      'region', 'area', 'lga', 'businessAddress', 'residentialAddress']
    
    sample = data[0] if data else {}
    return any(field in sample for field in location_fields)


def apply_date_filters(data: List[Dict], date_filter: Dict[str, Any], prefer_updated: bool = False) -> List[Dict]:
    """Filter records by date"""
    filter_type = date_filter.get('type')
    
    # Find date fields in data
    date_fields = find_date_fields(data)
    
    if not date_fields:
        return data  # No date fields found
    
    # Choose date field: prefer updatedDt/updatedAt if prefer_updated is True, else createdDt/createdAt
    date_field = date_fields[0]
    if prefer_updated:
        for field in date_fields:
            if 'updat' in field.lower():
                date_field = field
                break
    else:
        for field in date_fields:
            if 'creat' in field.lower():
                date_field = field
                break
    
    filtered = []
    
    for record in data:
        record_date = extract_date_from_record(record, date_field)
        
        if record_date is None:
            continue
        
        # Apply filter based on type
        if filter_type == 'specific_year':
            if record_date.year == date_filter['year']:
                filtered.append(record)
        
        elif filter_type == 'specific_month':
            if (record_date.year == date_filter['year'] and 
                record_date.month == date_filter['month']):
                filtered.append(record)
        
        elif filter_type in ['days_range', 'hours_range', 'minutes_range', 'seconds_range', 'recent', 'months_range']:
            if (date_filter['start_date'] <= record_date <= date_filter['end_date']):
                filtered.append(record)
        
        elif filter_type == 'before':
            if record_date < date_filter['before_date']:
                filtered.append(record)
        
        elif filter_type == 'after':
            if record_date > date_filter['after_date']:
                filtered.append(record)
    
    return filtered


def apply_location_filters(data: List[Dict], location_filter: Dict[str, Any]) -> List[Dict]:
    """Filter records by location"""
    location = location_filter.get('location', '').lower()
    location_type = location_filter.get('type', 'any')
    
    # Find location fields
    location_fields = [
        'city', 'state', 'country', 'location', 'address', 
        'region', 'area', 'lga', 'businessAddress', 'residentialAddress'
    ]
    
    filtered = []
    
    for record in data:
        # Check all location fields
        for field in location_fields:
            value = record.get(field)
            if value and isinstance(value, str):
                if location in value.lower():
                    filtered.append(record)
                    break
    
    return filtered


def apply_numeric_filter(data: List[Dict], numeric_filter: Dict[str, Any]) -> List[Dict]:
    """Apply single numeric filter"""
    field_hint = numeric_filter['field']
    operator = numeric_filter['operator']
    
    # Find actual numeric field in data
    numeric_field = find_numeric_field(data, field_hint)
    
    if not numeric_field:
        return data  # Field not found
    
    filtered = []
    
    for record in data:
        value = extract_numeric_value(record, numeric_field)
        
        if value is None:
            continue
        
        # Apply operator
        if operator == '>':
            if value > numeric_filter['value']:
                filtered.append(record)
        elif operator == '<':
            if value < numeric_filter['value']:
                filtered.append(record)
        elif operator == '=':
            if value == numeric_filter['value']:
                filtered.append(record)
        elif operator == '>=':
            if value >= numeric_filter['value']:
                filtered.append(record)
        elif operator == '<=':
            if value <= numeric_filter['value']:
                filtered.append(record)
        elif operator == 'between':
            if (numeric_filter['min_value'] <= value <= numeric_filter['max_value']):
                filtered.append(record)
    
    return filtered


def apply_text_filters(data: List[Dict], text_filter: Dict[str, Any]) -> List[Dict]:
    """Apply text-based filters with BERT semantic matching for merchant/customer names and received points"""
    
    filtered = data
    
    # "Received points" filter - search in smsText field for received points patterns
    if 'is_received_points' in text_filter and text_filter.get('is_received_points'):
        amount = text_filter.get('received_points_amount')
        from_person = text_filter.get('received_points_from', 'someone')
        
        # Look for "You've received X nuMoni points from Y" pattern in smsText
        sms_fields = ['smsText', 'sms_text', 'message', 'content', 'text']
        
        matched = []
        for record in data:
            found = False
            for field in sms_fields:
                sms_text = str(record.get(field, '')).lower().strip()
                if not sms_text:
                    continue
                
                # Pattern: "received X points" in smsText
                if 'received' in sms_text and 'points' in sms_text:
                    # If amount is specified, check for it
                    if amount:
                        # Look for the amount in the message (allowing for flexible digit formats)
                        amount_pattern = str(amount)
                        if amount_pattern in sms_text:
                            matched.append(record)
                            found = True
                            break
                    else:
                        # No specific amount, just check for "received" and "points"
                        matched.append(record)
                        found = True
                        break
            
            if found:
                continue
        
        filtered = matched
    
    # Merchant/business name filter - with BERT semantic matching
    elif 'merchant_name' in text_filter:
        search_name = text_filter['merchant_name'].lower().strip()
        merchant_fields = ['name', 'businessName', 'merchantName', 'customerName', 'userName', 
                          'merchant_name', 'transactionMerchant', 'vendorName', 'storeName']
        
        matched = []
        for record in data:
            found = False
            for field in merchant_fields:
                field_value = str(record.get(field, '')).lower().strip()
                if not field_value:
                    continue
                
                # Exact match or substring match
                if search_name in field_value or field_value in search_name:
                    matched.append(record)
                    found = True
                    break
                
                # BERT semantic match if similarity > 80%
                if len(search_name) > 3 and len(field_value) > 3:
                    similarity = semantic_similarity(search_name, field_value)
                    if similarity > 0.80:
                        matched.append(record)
                        found = True
                        break
            
            if found:
                continue
        
        filtered = matched
    
    # Name contains filter (generic)
    elif 'name_contains' in text_filter:
        search_text = text_filter['name_contains'].lower()
        name_fields = ['name', 'businessName', 'userName', 'merchantName', 'customerName']
        
        filtered = [
            r for r in filtered
            if any(
                search_text in str(r.get(field, '')).lower()
                for field in name_fields
            )
        ]
    
    # Status filter
    if 'status' in text_filter:
        filtered = [r for r in filtered if r.get('status', '').upper() == text_filter['status']]
    
    # Type filter
    if 'type' in text_filter:
        filtered = [r for r in filtered if r.get('type', '').upper() == text_filter['type']]
    
    return filtered


def find_date_fields(data: List[Dict]) -> List[str]:
    """Find date field names in data"""
    if not data:
        return []
    
    # Common date field names
    date_field_names = [
        'createdAt', 'createdDt', 'updatedAt', 'updatedDt',
        'date', 'timestamp', 'created', 'updated', 'addedAt', 'activityTime'
    ]
    
    # Check first record for date fields
    sample = data[0]
    found_fields = []
    
    for field in date_field_names:
        if field in sample:
            found_fields.append(field)
    
    return found_fields


def extract_date_from_record(record: Dict, field_name: str) -> datetime:
    """Extract datetime from record field (returns naive datetime for comparison)"""
    value = record.get(field_name)
    
    if value is None:
        return None
    
    # Handle MongoDB $date format
    if isinstance(value, dict) and '$date' in value:
        date_str = value['$date']
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            # Convert to naive datetime (remove timezone) for comparison
            return dt.replace(tzinfo=None)
        except:
            pass
    
    # Handle ISO string format
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            # Convert to naive datetime
            return dt.replace(tzinfo=None)
        except:
            pass
    
    # Handle timestamp format
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000)  # Assume milliseconds, returns naive
        except:
            pass
    
    return None


def find_numeric_field(data: List[Dict], field_hint: str) -> str:
    """Find actual numeric field name based on hint"""
    if not data:
        return None
    
    sample = data[0]
    
    # Field mappings based on hint
    field_mappings = {
        'points': ['points', 'bonusAmount', 'rewardPoints', 'nuMoniPoints', 'pointsEarned'],
        'amount': ['amount', 'totalAmount', 'transactionAmount', 'paymentAmount'],
        'balance': ['balance', 'walletBalance', 'availableBalance', 'currentBalance'],
        'count': ['count', 'transactionCount', 'orderCount']
    }
    
    # Get possible field names for this hint
    possible_fields = field_mappings.get(field_hint, [field_hint])
    
    # Check which field exists in data
    for field in possible_fields:
        if field in sample:
            return field
    
    # If hint is about "points", also check smsText for point values
    if field_hint == 'points':
        if 'smsText' in sample:
            return 'smsText'  # Special handling needed
    
    return None


def extract_numeric_value(record: Dict, field_name: str) -> float:
    """Extract numeric value from record field"""
    value = record.get(field_name)
    
    if value is None:
        return None
    
    # Direct numeric value
    if isinstance(value, (int, float)):
        return float(value)
    
    # Extract from text (for smsText with points)
    if isinstance(value, str) and field_name == 'smsText':
        # Extract number before "nuMoni points" or "points"
        match = re.search(r'([\d,]+(?:\.\d+)?)\s+nuMoni points', value)
        if match:
            return float(match.group(1).replace(',', ''))
    
    # Try to convert string to float
    if isinstance(value, str):
        try:
            return float(value.replace(',', ''))
        except:
            pass
    
    return None


if __name__ == "__main__":
    print("Advanced Filter Executor - Test Mode")
    print("=" * 80)
    
    # Test with sample data
    sample_data = [
        {
            'name': 'John Doe',
            'amount': 1500,
            'city': 'Lagos',
            'createdAt': {'$date': '2025-01-15T10:00:00.000Z'}
        },
        {
            'name': 'Jane Smith',
            'amount': 800,
            'city': 'Abuja',
            'createdAt': {'$date': '2024-12-20T10:00:00.000Z'}
        },
        {
            'name': 'Bob Wilson',
            'amount': 2000,
            'city': 'Lagos',
            'createdAt': {'$date': '2026-02-01T10:00:00.000Z'}
        }
    ]
    
    print(f"\nSample data: {len(sample_data)} records")
    
    # Test date filter (last year = 2025)
    date_filter = {'type': 'specific_year', 'year': 2025}
    filtered = apply_date_filters(sample_data, date_filter)
    print(f"Date filter (year=2025): {len(filtered)} records")
    
    # Test location filter
    location_filter = {'location': 'lagos', 'type': 'city'}
    filtered = apply_location_filters(sample_data, location_filter)
    print(f"Location filter (Lagos): {len(filtered)} records")
    
    # Test numeric filter
    numeric_filter = {'field': 'amount', 'operator': '>', 'value': 1000}
    filtered = apply_numeric_filter(sample_data, numeric_filter)
    print(f"Numeric filter (amount > 1000): {len(filtered)} records")
    
    print("\n" + "=" * 80)
