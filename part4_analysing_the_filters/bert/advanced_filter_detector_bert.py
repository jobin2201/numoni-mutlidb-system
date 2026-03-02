#!/usr/bin/env python
"""
Advanced Filter Detector - Part 4
Detects date, location, numeric, and complex filters from user queries
"""
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

# Current date reference (can be overridden)
CURRENT_DATE = datetime.now()


def detect_advanced_filters(user_query: str, current_date: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Detect advanced filters from user query
    
    Returns:
        {
            'date_filters': {...},
            'location_filters': {...},
            'numeric_filters': {...},
            'text_filters': {...},
            'has_advanced_filters': bool,
            'suggested_collection': str (optional)
        }
    """
    query_lower = user_query.lower()
    
    if current_date is None:
        current_date = CURRENT_DATE
    
    filters = {
        'date_filters': extract_date_filters(query_lower, current_date),
        'location_filters': extract_location_filters(query_lower),
        'numeric_filters': extract_numeric_filters(query_lower),
        'text_filters': extract_text_filters(query_lower),
        'has_advanced_filters': False,
        'suggested_collection': None,
        'collection_reason': None
    }
    
    # Check if any filters were detected
    filters['has_advanced_filters'] = any([
        filters['date_filters'],
        filters['location_filters'],
        filters['numeric_filters'],
        filters['text_filters']
    ])
    
    # Suggest appropriate collection for date queries
    if filters['date_filters']:
        suggestion = suggest_collection_for_date_query(query_lower)
        if suggestion:
            filters['suggested_collection'] = suggestion['collection']
            filters['collection_reason'] = suggestion['reason']
    
    return filters


def suggest_collection_for_date_query(query: str) -> Optional[Dict[str, str]]:
    """
    Suggest appropriate collection based on date query context
    For 'added/created' queries → use details collections
    For 'transactions' queries → use transaction_history
    """
    # Pattern 1: "customers added/created" → customerDetails
    if any(word in query for word in ['customer', 'customers', 'user']):
        if any(word in query for word in ['added', 'created', 'registered', 'signed up', 'joined']):
            return {
                'collection': 'customerDetails',
                'reason': 'Query about customers ADDED/CREATED - use customerDetails with createdDt field'
            }
        elif any(word in query for word in ['transaction', 'payment', 'transfer', 'received points']):
            return {
                'collection': 'transaction_history',
                'reason': 'Query about customer TRANSACTIONS - use transaction_history with transactionDate'
            }
    
    # Pattern 2: "merchants added/opened" → merchantDetails
    if any(word in query for word in ['merchant', 'merchants', 'store', 'business']):
        if any(word in query for word in ['added', 'created', 'opened', 'registered', 'joined']):
            return {
                'collection': 'merchantDetails',
                'reason': 'Query about merchants ADDED/CREATED - use merchantDetails with createdDt field'
            }
        elif any(word in query for word in ['transaction', 'payment', 'sale', 'deal']):
            return {
                'collection': 'transaction_history',
                'reason': 'Query about merchant TRANSACTIONS - use transaction_history with transactionDate'
            }
    
    # Pattern 3: "login/authentication" date queries → login_activities
    if any(word in query for word in ['login', 'signin', 'sign in', 'authentication', 'auth']):
        if any(word in query for word in ['activity', 'activities', 'attempt', 'events', 'history']):
            return {
                'collection': 'login_activities',
                'reason': 'Query about LOGIN ACTIVITIES - use login_activities with createdDt field'
            }
    
    # Pattern 4: "audit/audit trail" date queries → audit_trail
    if any(word in query for word in ['audit', 'audit trail', 'system activity', 'logs', 'action log']):
        return {
            'collection': 'audit_trail',
            'reason': 'Query about AUDIT TRAIL - use audit_trail with createdTime field'
        }
    
    # Pattern 5: "user sessions" date queries → user_sessions
    if any(word in query for word in ['session', 'sessions', 'user session', 'active session']):
        if any(word in query for word in ['start', 'created', 'active', 'open']):
            return {
                'collection': 'user_sessions',
                'reason': 'Query about USER SESSIONS - use user_sessions with sessionStartTime field'
            }
    
    return None


def extract_date_filters(query: str, current_date: datetime) -> Dict[str, Any]:
    """Extract date-based filters - supports seconds, minutes, hours, days, weeks, months, years"""
    date_filters = {}
    from calendar import monthrange
    
    # Pattern: "last year" / "this year"
    if 'last year' in query:
        date_filters['year'] = current_date.year - 1
        date_filters['type'] = 'specific_year'
    elif 'this year' in query:
        date_filters['year'] = current_date.year
        date_filters['type'] = 'specific_year'
    
    # Pattern: "last month" / "this month"
    if 'last month' in query:
        last_month = current_date.replace(day=1) - timedelta(days=1)
        date_filters['month'] = last_month.month
        date_filters['year'] = last_month.year
        date_filters['type'] = 'specific_month'
    elif 'this month' in query:
        date_filters['month'] = current_date.month
        date_filters['year'] = current_date.year
        date_filters['type'] = 'specific_month'
    
    # Pattern: "last week" / "this week" / "past week"
    if 'last week' in query or 'past week' in query:
        # Last week: 7 days ago to today
        date_filters['days_ago'] = 7
        date_filters['start_date'] = current_date - timedelta(days=7)
        date_filters['end_date'] = current_date
        date_filters['type'] = 'days_range'
    elif 'this week' in query:
        # This week: from Monday of current week to today
        monday = current_date - timedelta(days=current_date.weekday())
        date_filters['start_date'] = monday
        date_filters['end_date'] = current_date
        date_filters['type'] = 'days_range'
    
    # Pattern: "N weeks/week back/behind/ago" or "last N weeks"
    weeks_match = re.search(r'(?:last|past)\s+(\d+)\s+weeks?', query)
    if weeks_match:
        weeks = int(weeks_match.group(1))
        date_filters['days_ago'] = weeks * 7
        date_filters['start_date'] = current_date - timedelta(days=weeks * 7)
        date_filters['end_date'] = current_date
        date_filters['type'] = 'days_range'
    
    # Pattern: "N days/day back/behind/ago" or "last N days" / "past N days"
    days_match = re.search(r'(?:last|past)\s+(\d+)\s+days?(?:\s+ago)?', query)
    if days_match:
        days = int(days_match.group(1))
        date_filters['days_ago'] = days
        date_filters['start_date'] = current_date - timedelta(days=days)
        date_filters['end_date'] = current_date
        date_filters['type'] = 'days_range'
    
    # Pattern: "N hours/hour back/behind/ago"
    hours_match = re.search(r'(?:last|past)\s+(\d+)\s+hours?(?:\s+ago)?', query)
    if hours_match:
        hours = int(hours_match.group(1))
        date_filters['hours_ago'] = hours
        date_filters['start_date'] = current_date - timedelta(hours=hours)
        date_filters['end_date'] = current_date
        date_filters['type'] = 'hours_range'
    
    # Pattern: "N minutes/minute back/behind/ago"
    minutes_match = re.search(r'(?:last|past)\s+(\d+)\s+minutes?(?:\s+ago)?', query)
    if minutes_match:
        minutes = int(minutes_match.group(1))
        date_filters['minutes_ago'] = minutes
        date_filters['start_date'] = current_date - timedelta(minutes=minutes)
        date_filters['end_date'] = current_date
        date_filters['type'] = 'minutes_range'
    
    # Pattern: "N seconds/second back/behind/ago"
    seconds_match = re.search(r'(?:last|past)\s+(\d+)\s+seconds?(?:\s+ago)?', query)
    if seconds_match:
        seconds = int(seconds_match.group(1))
        date_filters['seconds_ago'] = seconds
        date_filters['start_date'] = current_date - timedelta(seconds=seconds)
        date_filters['end_date'] = current_date
        date_filters['type'] = 'seconds_range'
    
    # Pattern: "N months back/behind/ago" - SPECIFIC MONTH
    months_match = re.search(r'(\d+)\s+months?\s+(?:back|behind|ago)', query)
    if months_match:
        months = int(months_match.group(1))
        # Calculate the specific month that was N months ago
        year = current_date.year
        month = current_date.month - months
        while month <= 0:
            month += 12
            year -= 1
        
        # Set to first day of that specific month
        start_date = datetime(year, month, 1)
        # Set to last day of that specific month
        last_day = monthrange(year, month)[1]
        end_date = datetime(year, month, last_day, 23, 59, 59)
        
        date_filters['months_ago'] = months
        date_filters['month'] = month
        date_filters['year'] = year
        date_filters['start_date'] = start_date
        date_filters['end_date'] = end_date
        date_filters['type'] = 'specific_month'
    
    # Pattern: "N years/year back/behind/ago"
    years_match = re.search(r'(\d+)\s+years?\s+(?:back|behind|ago)', query)
    if years_match:
        years = int(years_match.group(1))
        target_year = current_date.year - years
        date_filters['year'] = target_year
        date_filters['type'] = 'specific_year'
    
    # Pattern: "newly added", "recent", "latest"
    if any(word in query for word in ['newly added', 'new', 'recent', 'latest']):
        # Default to last 30 days for "new" items
        date_filters['days_ago'] = 30
        date_filters['start_date'] = current_date - timedelta(days=30)
        date_filters['end_date'] = current_date
        date_filters['type'] = 'recent'
    
    # Pattern 5: "in YYYY" or "in 2025"
    year_match = re.search(r'\bin\s+(\d{4})\b', query)
    if year_match:
        date_filters['year'] = int(year_match.group(1))
        date_filters['type'] = 'specific_year'
    
    # Pattern 6: "added before/after date"
    if 'before' in query or 'after' in query:
        # Extract date patterns like "January 2025", "2025-01-01"
        month_year_match = re.search(r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})', query)
        if month_year_match:
            month_names = ['january', 'february', 'march', 'april', 'may', 'june', 
                          'july', 'august', 'september', 'october', 'november', 'december']
            month = month_names.index(month_year_match.group(1)) + 1
            year = int(month_year_match.group(2))
            
            if 'before' in query:
                date_filters['before_date'] = datetime(year, month, 1)
                date_filters['type'] = 'before'
            elif 'after' in query:
                date_filters['after_date'] = datetime(year, month, 1)
                date_filters['type'] = 'after'
    
    return date_filters


def extract_location_filters(query: str) -> Dict[str, Any]:
    """Extract location-based filters"""
    location_filters = {}
    
    # Pattern 1: "in [city/state/country]"
    in_match = re.search(r'\bin\s+([A-Z][a-zA-Z\s]+?)(?:\s+(?:city|state|country|area|region)|$)', query)
    if in_match:
        location = in_match.group(1).strip()
        location_filters['location'] = location
        
        # Detect location type
        if 'city' in query:
            location_filters['type'] = 'city'
        elif 'state' in query:
            location_filters['type'] = 'state'
        elif 'country' in query:
            location_filters['type'] = 'country'
        else:
            location_filters['type'] = 'any'  # Could be city, state, or country
    
    # Pattern 2: "from [location]"
    from_match = re.search(r'\bfrom\s+([A-Z][a-zA-Z\s]+?)(?:\s+(?:customers?|merchants?|area|region)|$)', query)
    if from_match and not location_filters:
        location = from_match.group(1).strip()
        location_filters['location'] = location
        location_filters['type'] = 'any'
    
    # Common Nigerian cities/states (for context)
    nigerian_locations = ['lagos', 'abuja', 'kano', 'ibadan', 'port harcourt', 
                         'benin', 'kaduna', 'enugu', 'nigeria']
    for loc in nigerian_locations:
        if loc in query and not location_filters:
            location_filters['location'] = loc.title()
            location_filters['type'] = 'any'
            break
    
    return location_filters


def extract_numeric_filters(query: str) -> List[Dict[str, Any]]:
    """Extract numeric comparison filters"""
    numeric_filters = []
    
    # Pattern 1: "more than X", "greater than X", "above X"
    more_than_patterns = [
        r'(?:more than|greater than|above|over|exceeding)\s+([\d,]+(?:\.\d+)?)',
        r'received\s+(?:more than|over)\s+([\d,]+(?:\.\d+)?)',
        r'have\s+(?:more than|over)\s+([\d,]+(?:\.\d+)?)'
    ]
    
    for pattern in more_than_patterns:
        match = re.search(pattern, query)
        if match:
            value = float(match.group(1).replace(',', ''))
            
            # Detect what field this applies to
            field = detect_numeric_field(query)
            
            numeric_filters.append({
                'field': field,
                'operator': '>',
                'value': value,
                'description': f'{field} > {value}'
            })
            break
    
    # Pattern 2: "less than X", "below X", "under X"
    less_than_patterns = [
        r'(?:less than|below|under)\s+([\d,]+(?:\.\d+)?)',
        r'received\s+(?:less than|under)\s+([\d,]+(?:\.\d+)?)'
    ]
    
    for pattern in less_than_patterns:
        match = re.search(pattern, query)
        if match:
            value = float(match.group(1).replace(',', ''))
            field = detect_numeric_field(query)
            
            numeric_filters.append({
                'field': field,
                'operator': '<',
                'value': value,
                'description': f'{field} < {value}'
            })
            break
    
    # Pattern 3: "exactly X", "equal to X"
    exact_patterns = [
        r'(?:exactly|equal to)\s+([\d,]+(?:\.\d+)?)',
        r'received\s+([\d,]+(?:\.\d+)?)\s+(?:points|amount)'
    ]
    
    for pattern in exact_patterns:
        match = re.search(pattern, query)
        if match:
            value = float(match.group(1).replace(',', ''))
            field = detect_numeric_field(query)
            
            numeric_filters.append({
                'field': field,
                'operator': '=',
                'value': value,
                'description': f'{field} = {value}'
            })
            break
    
    # Pattern 4: "between X and Y"
    between_match = re.search(r'between\s+([\d,]+(?:\.\d+)?)\s+and\s+([\d,]+(?:\.\d+)?)', query)
    if between_match:
        min_val = float(between_match.group(1).replace(',', ''))
        max_val = float(between_match.group(2).replace(',', ''))
        field = detect_numeric_field(query)
        
        numeric_filters.append({
            'field': field,
            'operator': 'between',
            'min_value': min_val,
            'max_value': max_val,
            'description': f'{min_val} <= {field} <= {max_val}'
        })
    
    return numeric_filters


def detect_numeric_field(query: str) -> str:
    """Detect which numeric field the filter applies to"""
    query_lower = query.lower()
    
    # Points-related
    if any(word in query_lower for word in ['points', 'numoni points', 'reward', 'bonus']):
        return 'points'
    
    # Amount-related
    if any(word in query_lower for word in ['amount', 'money', 'payment', 'total']):
        return 'amount'
    
    # Balance-related
    if any(word in query_lower for word in ['balance', 'wallet']):
        return 'balance'
    
    # Transaction count
    if any(word in query_lower for word in ['transactions', 'orders', 'purchases']):
        return 'count'
    
    # Default
    return 'amount'


def extract_text_filters(query: str) -> Dict[str, Any]:
    """Extract text-based filters including merchant/business names"""
    text_filters = {}
    temporal_stopwords = {'last', 'this', 'next', 'past', 'previous', 'current', 'today', 'yesterday', 'week', 'month', 'year'}
    
    # Pattern 0: Extract merchant/business name from "of [Name]" - more flexible
    # Matches: "of chicken republic", "of Chicken Republic", "of chicken republic last month", etc.
    of_match = re.search(r'of\s+([a-z][a-z0-9\s&-\.\']+?)(?:\s+(?:last|this|next|in|from|transactions?|customers?|data|records|month|week|day|year)|\s*$)', query)
    if of_match:
        merchant_name = of_match.group(1).strip()
        # Clean up trailing/leading spaces and normalize
        merchant_name = re.sub(r'\s+', ' ', merchant_name)
        merchant_name_lower = merchant_name.lower()
        # Ignore temporal-only captures like "of last year"
        if merchant_name_lower and merchant_name_lower not in temporal_stopwords:
            merchant_name = merchant_name.title()  # Capitalize first letter of each word
            text_filters['merchant_name'] = merchant_name
    
    # Pattern 0b: Extract "received points from [someone]" or "received [amount] points from [someone]"
    # Matches: "received 1000 nuMoni points from someone", "received points from John"
    received_points_match = re.search(r'received\s+(?:(\d+)\s+)?(?:numoni\s+)?points?\s+from\s+([a-z\s]+?)(?:\s+(?:in|on|at|during|when|if|where)|\s*$)', query)
    if received_points_match:
        amount = received_points_match.group(1)  # Could be None if no amount specified
        from_person = received_points_match.group(2).strip()
        
        # Set flags for this special query type
        text_filters['is_received_points'] = True
        if amount:
            text_filters['received_points_amount'] = int(amount)
        if from_person:
            text_filters['received_points_from'] = from_person
        else:
            text_filters['received_points_from'] = 'someone'  # Generic "someone else" case
    
    # Pattern 1: Name contains
    if 'name contains' in query or 'names containing' in query:
        match = re.search(r'(?:name|names)\s+contain(?:s|ing)\s+["\']?(\w+)["\']?', query)
        if match:
            text_filters['name_contains'] = match.group(1)
    
    # Pattern 2: Status filters
    status_keywords = ['active', 'inactive', 'pending', 'completed', 'failed', 'success', 'successful']
    for status in status_keywords:
        if status in query:
            text_filters['status'] = status.upper()
            break
    
    # Pattern 3: Type filters
    if 'type' in query:
        type_match = re.search(r'type\s+(?:is|=|:)\s+(\w+)', query)
        if type_match:
            text_filters['type'] = type_match.group(1).upper()
    
    # Pattern 4: Detect if looking for edited/modified/changed items for updatedDt
    if any(word in query for word in ['edited', 'modified', 'changed', 'updated']):
        text_filters['use_updated_date'] = True
    
    return text_filters


if __name__ == "__main__":
    print("=" * 80)
    print("Advanced Filter Detector - Test Mode")
    print("=" * 80)
    
    test_queries = [
        "show me customers added last year",
        "who were the newly added customers",
        "customers from Lagos",
        "which customers have received more than 1000 numoni points",
        "merchants in Nigeria",
        "customers added in last 30 days",
        "show me transactions from this month",
        "customers with balance between 500 and 2000",
        "merchants added after January 2025"
    ]
    
    for query in test_queries:
        print(f"\nQuery: '{query}'")
        filters = detect_advanced_filters(query)
        
        if filters['has_advanced_filters']:
            if filters['date_filters']:
                print(f"  Date: {filters['date_filters']}")
            if filters['location_filters']:
                print(f"  Location: {filters['location_filters']}")
            if filters['numeric_filters']:
                print(f"  Numeric: {filters['numeric_filters']}")
            if filters['text_filters']:
                print(f"  Text: {filters['text_filters']}")
        else:
            print("  No advanced filters detected")
    
    print("\n" + "=" * 80)
