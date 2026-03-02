#!/usr/bin/env python
"""
Advanced Filter Executor - Part 4
Applies date, location, and numeric filters to collection data
"""
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable


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
    """Apply text-based filters with fuzzy matching for merchant/customer names and received points"""
    from difflib import SequenceMatcher
    
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
    
    # Merchant/business name filter - with fuzzy matching
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
                
                # Fuzzy match if similarity > 80%
                if len(search_name) > 3 and len(field_value) > 3:
                    similarity = SequenceMatcher(None, search_name, field_value).ratio()
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

    # Last/latest transaction filter: keep only most recent record
    if text_filter.get('last_transaction') and filtered:
        date_fields = find_date_fields(filtered)
        if date_fields:
            date_field = date_fields[0]
            # Prefer transaction/date-like fields for transaction queries
            for candidate in ['date', 'transactionDate', 'createdDt', 'createdDate', 'updatedDt', 'updatedDate']:
                if candidate in date_fields:
                    date_field = candidate
                    break

            filtered = sorted(
                filtered,
                key=lambda r: extract_date_from_record(r, date_field) or datetime.min,
                reverse=True
            )[:1]
        else:
            filtered = filtered[-1:]
    
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


def parse_record_date(value: Any) -> Optional[datetime]:
    """Parse mixed date value formats into naive datetime."""
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            unit = 'ms' if value > 10**11 else 's'
            dt = datetime.fromtimestamp((value / 1000.0) if unit == 'ms' else float(value))
            return dt
        if isinstance(value, dict) and '$date' in value:
            value = value.get('$date')
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return dt.replace(tzinfo=None)
    except Exception:
        return None
    return None


def apply_date_range_filter(data: List[Dict], start_date: datetime, end_date: datetime,
                            date_fields: Optional[List[str]] = None) -> List[Dict]:
    """Apply inclusive date-range filtering using first valid date field per row."""
    if not data:
        return data
    if date_fields is None:
        date_fields = ['transactionDate', 'createdDate', 'createdDt', 'updatedDt', 'updatedAt', 'activityTime', 'date', 'payoutDate', 'createdAt', 'entryDate']

    filtered: List[Dict] = []
    for row in data:
        row_dt = None
        for field in date_fields:
            if field in row:
                parsed = parse_record_date(row.get(field))
                if parsed is not None:
                    row_dt = parsed
                    break
        if row_dt is None:
            continue
        if start_date.date() <= row_dt.date() <= end_date.date():
            filtered.append(row)
    return filtered


def build_merchant_payout_dataset(load_collection_data: Callable[[str, str], List[Dict]]) -> List[Dict]:
    """Build merged merchant payout rows from payout-related collections with merchant name enrichment."""
    payout_main = load_collection_data('numoni_merchant', 'merchant_payout') or []
    payout_initiatives = load_collection_data('numoni_merchant', 'merchant_payout_initiatives') or []
    payout_retry = load_collection_data('numoni_merchant', 'payout_retry_records') or []
    merchant_details = load_collection_data('numoni_merchant', 'merchantDetails') or []
    wallet_ledger_rows = load_collection_data('numoni_merchant', 'merchant_wallet_ledger') or []

    def _to_float(value: Any) -> Optional[float]:
        try:
            return None if value is None else float(value)
        except Exception:
            return None

    merchant_name_by_id: Dict[str, str] = {}
    merchant_name_by_user_id: Dict[str, str] = {}
    for row in merchant_details:
        merchant_id = str(row.get('_id', '')).strip()
        user_id = str(row.get('userId', '')).strip()
        merchant_name = row.get('businessName') or row.get('brandName') or row.get('registeredBusiness') or row.get('email')
        if merchant_id and merchant_name:
            merchant_name_by_id[merchant_id] = merchant_name
        if user_id and merchant_name:
            merchant_name_by_user_id[user_id] = merchant_name

    initiative_amount_by_merchant: Dict[str, tuple] = {}
    for row in payout_initiatives:
        merchant_id = str(row.get('merchantId', '')).strip()
        amount_val = _to_float(row.get('amount'))
        if not merchant_id or amount_val is None:
            continue
        dt_val = parse_record_date(row.get('createdDt') or row.get('updatedDt'))
        prev = initiative_amount_by_merchant.get(merchant_id)
        if prev is None or ((dt_val is not None) and (prev[1] is None or dt_val > prev[1])):
            initiative_amount_by_merchant[merchant_id] = (amount_val, dt_val)

    ledger_amount_by_ref: Dict[str, float] = {}
    for row in wallet_ledger_rows:
        amount_val = _to_float(row.get('trn_in_amount') or row.get('amountByWallet') or row.get('amountBrandWallet'))
        if amount_val is None:
            continue
        for ref_key in ['transactionId', 'transactionNo', 'transactionReferenceId', 'invoiceRefId']:
            ref_val = str(row.get(ref_key, '')).strip()
            if ref_val:
                ledger_amount_by_ref[ref_val] = amount_val

    merged_rows: List[Dict] = []
    for row in payout_initiatives:
        merchant_id = str(row.get('merchantId', '')).strip()
        merchant_user_id = str(row.get('merchantIdUserId', '')).strip()
        merged_rows.append({
            'merchantId': merchant_id,
            'merchantName': merchant_name_by_id.get(merchant_id) or merchant_name_by_user_id.get(merchant_user_id),
            'payoutAmount': _to_float(row.get('amount')),
            'referenceId': row.get('reference') or row.get('paymentReferenceToken'),
            'payoutDate': row.get('createdDt') or row.get('updatedDt'),
            'status': row.get('status'),
            'sourceTable': 'merchant_payout_initiatives',
        })

    for row in payout_main:
        merchant_id = str(row.get('merchantId', '')).strip()
        payout_amount = _to_float(row.get('amount') or row.get('payoutAmount'))
        if payout_amount is None and merchant_id in initiative_amount_by_merchant:
            payout_amount = initiative_amount_by_merchant[merchant_id][0]
        merged_rows.append({
            'merchantId': merchant_id,
            'merchantName': merchant_name_by_id.get(merchant_id),
            'payoutAmount': payout_amount,
            'referenceId': row.get('reference') or row.get('transactionId'),
            'payoutDate': row.get('payoutDate') or row.get('createdAt') or row.get('updatedAt'),
            'status': row.get('status'),
            'sourceTable': 'merchant_payout',
        })

    for row in payout_retry:
        merchant_id = str(row.get('merchantId', '')).strip()
        ref_id = row.get('transactionId') or row.get('ledgerId')
        payout_amount = _to_float(row.get('amount') or row.get('transactionAmount'))
        if payout_amount is None and ref_id is not None:
            payout_amount = ledger_amount_by_ref.get(str(ref_id).strip())
        if payout_amount is None and merchant_id in initiative_amount_by_merchant:
            payout_amount = initiative_amount_by_merchant[merchant_id][0]
        merged_rows.append({
            'merchantId': merchant_id,
            'merchantName': merchant_name_by_id.get(merchant_id),
            'payoutAmount': payout_amount,
            'referenceId': ref_id,
            'payoutDate': row.get('createdAt') or row.get('updatedAt'),
            'status': row.get('status'),
            'sourceTable': 'payout_retry_records',
        })

    return merged_rows


def detect_comparison_operator(query_lower: str) -> Optional[str]:
    """Detect numeric comparison operator from natural language."""
    if any(token in query_lower for token in ['greater than or equal to', 'greater than equal to', 'more than or equal to', 'at least', 'not less than', '>=', 'greater than or equals']):
        return 'gte'
    if any(token in query_lower for token in ['less than or equal to', 'lesser than or equal to', 'at most', 'not greater than', '<=', 'less than or equals']):
        return 'lte'
    if any(token in query_lower for token in ['not equal to', 'not equals', '!=', '<>']):
        return 'neq'
    if any(token in query_lower for token in ['equal to', 'equals', 'same as', 'exactly', '==']):
        return 'eq'
    if any(token in query_lower for token in ['greater than', 'more than', 'higher than', 'above', '>']):
        return 'gt'
    if any(token in query_lower for token in ['less than', 'lesser than', 'lower than', 'smaller than', 'below', '<']):
        return 'lt'
    return None


def apply_login_link_filter(data: List[Dict], requested_date: Optional[tuple],
                            load_collection_data: Callable[[str, str], List[Dict]]) -> List[Dict]:
    """Keep transaction rows whose customers logged in during requested date range and add loginDate."""
    login_rows = load_collection_data('authentication', 'login_activities') or []
    customer_rows = load_collection_data('numoni_customer', 'customerDetails') or []

    login_user_to_latest: Dict[str, datetime] = {}
    for row in login_rows:
        user_id = str(row.get('userId', '')).strip()
        if not user_id:
            continue
        login_dt = parse_record_date(row.get('activityTime'))
        if login_dt is None:
            continue
        if requested_date and not (requested_date[0].date() <= login_dt.date() <= requested_date[1].date()):
            continue
        prev = login_user_to_latest.get(user_id)
        if prev is None or login_dt > prev:
            login_user_to_latest[user_id] = login_dt

    customer_id_to_login_dt: Dict[str, datetime] = {}
    for row in customer_rows:
        user_id = str(row.get('userId', '')).strip()
        customer_id = str(row.get('_id', '')).strip()
        if user_id and customer_id and user_id in login_user_to_latest:
            customer_id_to_login_dt[customer_id] = login_user_to_latest[user_id]

    filtered: List[Dict] = []
    for row in data:
        customer_id = str(row.get('customerId', '')).strip()
        login_dt = customer_id_to_login_dt.get(customer_id)
        if login_dt is None:
            continue
        new_row = dict(row)
        new_row['loginDate'] = login_dt.strftime('%Y-%m-%d')
        filtered.append(new_row)

    return filtered


def apply_payout_wallet_comparison(data: List[Dict], query_text: str, requested_date: Optional[tuple],
                                   load_collection_data: Callable[[str, str], List[Dict]]) -> List[Dict]:
    """Apply payout-vs-wallet comparison using merchant wallet ledger balances within date range."""
    wallet_rows = load_collection_data('numoni_merchant', 'merchant_wallet_ledger') or []

    def _to_float(v: Any) -> Optional[float]:
        try:
            return None if v is None else float(v)
        except Exception:
            return None

    wallet_balance_by_merchant: Dict[str, float] = {}
    wallet_date_by_merchant: Dict[str, datetime] = {}
    wallet_trn_amount_by_merchant: Dict[str, Optional[float]] = {}
    for row in wallet_rows:
        merchant_id = str(row.get('merchantId', '')).strip()
        if not merchant_id:
            continue
        balance_val = _to_float(row.get('balance_amount'))
        if balance_val is None:
            continue
        row_dt = parse_record_date(row.get('createdDt') or row.get('updatedDt'))
        if requested_date and row_dt is not None:
            if not (requested_date[0].date() <= row_dt.date() <= requested_date[1].date()):
                continue
        prev_dt = wallet_date_by_merchant.get(merchant_id)
        if prev_dt is None or (row_dt is not None and row_dt >= prev_dt):
            wallet_balance_by_merchant[merchant_id] = balance_val
            wallet_date_by_merchant[merchant_id] = row_dt
            wallet_trn_amount_by_merchant[merchant_id] = _to_float(
                row.get('trn_in_amount') or row.get('amountByWallet') or row.get('amountBrandWallet')
            )

    comparator_op = detect_comparison_operator((query_text or '').lower())
    compared: List[Dict] = []
    for row in data:
        merchant_id = str(row.get('merchantId', '')).strip()
        payout_val = _to_float(row.get('payoutAmount'))
        if payout_val is None:
            payout_val = wallet_trn_amount_by_merchant.get(merchant_id)
        wallet_val = wallet_balance_by_merchant.get(merchant_id)
        if payout_val is None or wallet_val is None:
            continue

        if comparator_op == 'gt' and not (payout_val > wallet_val):
            continue
        if comparator_op == 'lt' and not (payout_val < wallet_val):
            continue
        if comparator_op == 'eq' and not (payout_val == wallet_val):
            continue
        if comparator_op == 'gte' and not (payout_val >= wallet_val):
            continue
        if comparator_op == 'lte' and not (payout_val <= wallet_val):
            continue
        if comparator_op == 'neq' and not (payout_val != wallet_val):
            continue

        new_row = dict(row)
        new_row['payoutAmount'] = payout_val
        new_row['walletBalance'] = wallet_val
        wallet_dt = wallet_date_by_merchant.get(merchant_id)
        if wallet_dt is not None:
            new_row['walletDate'] = wallet_dt.strftime('%Y-%m-%d')
        compared.append(new_row)

    return compared


def _extract_rank_filter(query_text: str) -> Optional[Dict[str, Any]]:
    """Parse top/bottom-N intent from query text."""
    query_lower = (query_text or '').lower()
    rank_match = re.search(
        r'\b(top|bottom)\s+(\d+)\b(?:\s+(?:\w+\s*){0,3})?(?:\b(?:by|based on)\b\s+([^,.;\n]+?))?(?=\s+(?:from|between|in|for|with|where|include|including)\b|$|[,.;])',
        query_lower,
    )
    if not rank_match:
        return None

    limit = int(rank_match.group(2))
    if limit <= 0:
        return None

    return {
        'direction': rank_match.group(1).lower(),
        'limit': limit,
        'metric_phrase': (rank_match.group(3) or '').strip(),
    }


def _to_float_safe(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        value_str = str(value).strip().replace(',', '')
        if not value_str:
            return None
        return float(value_str)
    except Exception:
        return None


def _pick_group_key(row: Dict[str, Any], query_text: str) -> str:
    """Pick stable entity grouping key from row based on query intent."""
    query_lower = (query_text or '').lower()
    row_keys = list(row.keys())
    keys_lower_map = {k.lower(): k for k in row_keys}

    if any(token in query_lower for token in ['customer', 'customers', 'user', 'users', 'login', 'logins']):
        for candidate in ['customer id', 'customerid', 'user id', 'userid', 'customer name', 'name', 'email']:
            if candidate in keys_lower_map:
                return keys_lower_map[candidate]

    if any(token in query_lower for token in ['merchant', 'merchants']):
        for candidate in ['merchant id', 'merchantid', 'merchant name', 'business name', 'name']:
            if candidate in keys_lower_map:
                return keys_lower_map[candidate]

    for candidate in ['customer id', 'merchant id', 'user id', 'customer name', 'merchant name', 'name']:
        if candidate in keys_lower_map:
            return keys_lower_map[candidate]

    return row_keys[0] if row_keys else ''


def _pick_metric_column(rows: List[Dict[str, Any]], requested_fields: Optional[List[str]], metric_phrase: str) -> Optional[str]:
    """Pick ranking metric column from requested fields and available row keys."""
    if not rows:
        return None

    available_cols = list(rows[0].keys())
    available_lower_map = {c.lower(): c for c in available_cols}
    requested_fields = requested_fields or []

    metric_keywords = ['amount', 'spent', 'purchase', 'sales', 'total', 'count', 'points', 'revenue']

    if metric_phrase:
        metric_phrase_lower = metric_phrase.lower().strip()
        for col in available_cols:
            col_low = col.lower()
            if metric_phrase_lower in col_low or col_low in metric_phrase_lower:
                return col

    for req in requested_fields:
        req_low = req.lower()
        if any(k in req_low for k in metric_keywords) and req_low in available_lower_map:
            return available_lower_map[req_low]

    for col in available_cols:
        col_low = col.lower()
        if any(k in col_low for k in metric_keywords):
            return col

    numeric_scores = []
    for col in available_cols:
        values = [_to_float_safe(r.get(col)) for r in rows[:200]]
        numeric_count = sum(v is not None for v in values)
        if numeric_count > 0:
            numeric_scores.append((numeric_count, col))
    if numeric_scores:
        numeric_scores.sort(reverse=True)
        return numeric_scores[0][1]

    return None


def _pick_reference_column(rows: List[Dict[str, Any]]) -> Optional[str]:
    if not rows:
        return None
    for col in rows[0].keys():
        low = col.lower()
        if 'reference' in low and 'id' in low:
            return col
    for col in rows[0].keys():
        if 'reference' in col.lower():
            return col
    return None


def _pick_date_column(rows: List[Dict[str, Any]]) -> Optional[str]:
    if not rows:
        return None
    preferred = ['transactionDate', 'createdDate', 'createdDt', 'updatedDt', 'updatedAt', 'date', 'createdAt', 'entryDate']
    row_keys = list(rows[0].keys())
    for p in preferred:
        if p in row_keys:
            return p
    for col in row_keys:
        if 'date' in col.lower() or 'time' in col.lower():
            return col
    return None


def apply_top_bottom_n_filter(display_rows: List[Dict[str, Any]], query_text: str,
                              requested_fields: Optional[List[str]] = None) -> tuple:
    """Apply generic top/bottom N by metric on projected rows.

    Returns:
        (rows, info_message)
    """
    rank_filter = _extract_rank_filter(query_text)
    if not rank_filter:
        return display_rows, None
    if not display_rows:
        return display_rows, f"{rank_filter['direction'].title()} {rank_filter['limit']} requested, but no rows to rank."

    metric_col = _pick_metric_column(display_rows, requested_fields, rank_filter.get('metric_phrase', ''))
    if not metric_col:
        return display_rows, f"{rank_filter['direction'].title()} {rank_filter['limit']} requested, but no numeric metric column was detected."

    date_col = _pick_date_column(display_rows)
    reference_col = _pick_reference_column(display_rows)

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in display_rows:
        group_col = _pick_group_key(row, query_text)
        raw_group_val = row.get(group_col) if group_col else None
        if raw_group_val is None:
            continue
        group_key = str(raw_group_val).strip()
        if not group_key or group_key.lower() in {'none', 'null', 'nan'}:
            continue

        metric_val = _to_float_safe(row.get(metric_col))
        if metric_val is None:
            metric_val = 0.0

        row_dt = parse_record_date(row.get(date_col)) if date_col else None
        entry = grouped.get(group_key)
        if entry is None:
            grouped[group_key] = {
                'metric_sum': metric_val,
                'first_row': row,
                'last_row': row,
                'last_dt': row_dt,
            }
        else:
            entry['metric_sum'] += metric_val
            prev_dt = entry.get('last_dt')
            if row_dt is not None and (prev_dt is None or row_dt > prev_dt):
                entry['last_row'] = row
                entry['last_dt'] = row_dt

    if not grouped:
        return [], f"{rank_filter['direction'].title()} {rank_filter['limit']} requested, but no groupable rows were found."

    rows_out: List[Dict[str, Any]] = []
    requested_fields = requested_fields or list(display_rows[0].keys())
    metric_col_lower = metric_col.lower()
    for _, entry in grouped.items():
        first_row = entry['first_row']
        last_row = entry['last_row']
        out_row = {}
        for req in requested_fields:
            req_low = req.lower()
            if req in first_row:
                if ('total' in req_low or 'spent' in req_low or 'purchase' in req_low or 'sales' in req_low or 'count' in req_low) and (
                    metric_col_lower in req_low or any(k in req_low for k in ['amount', 'spent', 'purchase', 'sales', 'count'])
                ):
                    out_row[req] = round(entry['metric_sum'], 2)
                elif 'last' in req_low and 'reference' in req_low and reference_col:
                    out_row[req] = last_row.get(reference_col)
                else:
                    out_row[req] = first_row.get(req)
            elif 'last' in req_low and 'reference' in req_low and reference_col:
                out_row[req] = last_row.get(reference_col)
            elif any(k in req_low for k in ['total', 'spent', 'purchase', 'sales', 'count', 'amount']):
                out_row[req] = round(entry['metric_sum'], 2)
            else:
                out_row[req] = first_row.get(req)
        rows_out.append(out_row)

    reverse = rank_filter['direction'] == 'top'
    rows_out.sort(key=lambda r: _to_float_safe(r.get(metric_col)) if metric_col in r else _to_float_safe(next((r.get(c) for c in r.keys() if any(k in c.lower() for k in ['total', 'spent', 'purchase', 'sales', 'count', 'amount'])), 0)) or 0.0, reverse=reverse)
    limited = rows_out[:rank_filter['limit']]

    info = f"Applied {rank_filter['direction']} {rank_filter['limit']} by '{metric_col}' across {len(rows_out)} grouped entities."
    return limited, info


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
