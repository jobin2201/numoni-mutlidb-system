#!/usr/bin/env python
"""
Action Detector - Analyzes user query to determine operations to perform
Detects: COUNT, LIST, SUM, AVG, TOP N, FILTER, COMPARE, etc.
"""
import re
from typing import Dict, List, Any, Optional, Tuple
from sentence_transformers import SentenceTransformer, util


class ActionType:
    """Define action types"""
    COUNT = "count"
    LIST = "list"
    SUM = "sum"
    AVERAGE = "average"
    MAX = "max"
    MIN = "min"
    TOP_N = "top_n"
    BOTTOM_N = "bottom_n"
    FILTER = "filter"
    COMPARE = "compare"
    GROUP_BY = "group_by"
    DISTINCT = "distinct"
    SEARCH = "search"


# -----------------------------
# BERT Model Initialization
# -----------------------------
print("🤖 Loading BERT model for action detection...")
try:
    BERT_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    print("✅ BERT model loaded successfully!")
except Exception as e:
    print(f"⚠️ Warning: Could not load BERT model: {e}")
    print("   Falling back to keyword matching only.")
    BERT_MODEL = None

EMBEDDING_CACHE = {}


ACTION_CONTEXT = {
    ActionType.COUNT: "count number total how many quantity", 
    ActionType.LIST: "list show display get all retrieve records", 
    ActionType.SUM: "sum total amount add up aggregate", 
    ActionType.AVERAGE: "average mean avg typical", 
    ActionType.MAX: "maximum highest largest biggest top", 
    ActionType.MIN: "minimum lowest smallest least", 
    ActionType.TOP_N: "top best highest ranked largest first", 
    ActionType.BOTTOM_N: "bottom lowest last least worst", 
    ActionType.FILTER: "filter where with conditions matching", 
    ActionType.COMPARE: "compare versus difference between", 
    ActionType.GROUP_BY: "group by per for each categorize", 
    ActionType.DISTINCT: "distinct unique different", 
    ActionType.SEARCH: "search find look for query"
}


def get_embedding(text: str):
    if text in EMBEDDING_CACHE:
        return EMBEDDING_CACHE[text]

    embedding = BERT_MODEL.encode(text, convert_to_tensor=True)
    EMBEDDING_CACHE[text] = embedding
    return embedding


def semantic_best_action(query: str) -> Tuple[Optional[str], float]:
    if BERT_MODEL is None:
        return None, 0.0

    try:
        query_emb = get_embedding(query)
        scores = {}
        for action, context in ACTION_CONTEXT.items():
            context_emb = get_embedding(context)
            scores[action] = util.cos_sim(query_emb, context_emb).item()

        best_action = max(scores, key=scores.get)
        return best_action, float(scores[best_action])
    except Exception:
        return None, 0.0


def detect_action(user_query: str) -> Dict[str, Any]:
    """
    Detect what action(s) to perform based on user query
    
    Returns:
        {
            "primary_action": str,  # Main action
            "secondary_actions": list,  # Additional actions
            "fields": list,  # Fields mentioned
            "filters": dict,  # Filter conditions
            "limit": int,  # TOP N limit
            "sort_by": str,  # Field to sort by
            "sort_order": str,  # asc/desc
            "group_by": str,  # Field to group by
            "aggregation": str,  # count/sum/avg
            "comparison": dict,  # Comparison details
        }
    """
    query = user_query.lower()
    
    result = {
        "primary_action": None,
        "secondary_actions": [],
        "fields": [],
        "filters": {},
        "limit": None,
        "sort_by": None,
        "sort_order": "desc",
        "group_by": None,
        "aggregation": None,
        "comparison": None,
        "raw_query": user_query
    }
    
    # 1. Detect COUNT operations
    if any(pattern in query for pattern in [
        'how many', 'count', 'total number', 'number of'
    ]):
        result["primary_action"] = ActionType.COUNT
        result["aggregation"] = "count"
    
    # 2. Detect LIST operations
    elif any(pattern in query for pattern in [
        'list', 'show', 'display', 'get all', 'find all', 'give me'
    ]):
        result["primary_action"] = ActionType.LIST
    
    # 3. Detect SUM operations
    elif any(pattern in query for pattern in [
        'total amount', 'sum of', 'total', 'add up'
    ]):
        result["primary_action"] = ActionType.SUM
        result["aggregation"] = "sum"
    
    # 4. Detect AVERAGE operations
    elif any(pattern in query for pattern in [
        'average', 'avg', 'mean'
    ]):
        result["primary_action"] = ActionType.AVERAGE
        result["aggregation"] = "avg"
    
    # 5. Detect TOP N operations
    top_match = re.search(r'top\s+(\d+)', query)
    if top_match:
        result["primary_action"] = ActionType.TOP_N
        result["limit"] = int(top_match.group(1))
        result["sort_order"] = "desc"
    
    # 6. Detect BOTTOM N operations
    bottom_match = re.search(r'(?:bottom|last|lowest)\s+(\d+)', query)
    if bottom_match:
        result["primary_action"] = ActionType.BOTTOM_N
        result["limit"] = int(bottom_match.group(1))
        result["sort_order"] = "asc"
    
    # 7. Detect MAX operations
    elif any(pattern in query for pattern in [
        'maximum', 'max', 'highest', 'largest', 'biggest'
    ]):
        result["primary_action"] = ActionType.MAX
        result["aggregation"] = "max"
    
    # 8. Detect MIN operations
    elif any(pattern in query for pattern in [
        'minimum', 'min', 'lowest', 'smallest'
    ]):
        result["primary_action"] = ActionType.MIN
        result["aggregation"] = "min"
    
    # 9. Detect DISTINCT operations
    if any(pattern in query for pattern in ['unique', 'distinct', 'different']):
        if result["primary_action"] is None:
            result["primary_action"] = ActionType.DISTINCT
        else:
            result["secondary_actions"].append(ActionType.DISTINCT)
    
    # 10. Detect COMPARISON operations
    if any(pattern in query for pattern in [
        'compare', 'comparison', 'versus', 'vs', 'difference between'
    ]):
        result["comparison"] = extract_comparison(query)
        if result["primary_action"] is None:
            result["primary_action"] = ActionType.COMPARE
    
    # 11. Detect GROUP BY operations
    if any(pattern in query for pattern in [
        'by', 'per', 'for each', 'group by', 'grouped by'
    ]):
        group_field = extract_group_by_field(query)
        if group_field:
            result["group_by"] = group_field
            if result["primary_action"] == ActionType.LIST:
                result["primary_action"] = ActionType.GROUP_BY
    
    # 12. Extract FILTERS
    result["filters"] = extract_filters(query)
    
    # 13. Extract FIELDS mentioned
    result["fields"] = extract_fields(query)
    
    # 14. Extract SORT field
    result["sort_by"] = extract_sort_field(query)
    
    # 15. Extract LIMIT if specified
    if result["limit"] is None:
        limit_match = re.search(r'(?:limit|first|show)\s+(\d+)', query)
        if limit_match:
            result["limit"] = int(limit_match.group(1))
    
    # If LIST was chosen but semantic intent is strong, switch to semantic action
    if result["primary_action"] == ActionType.LIST:
        semantic_action, semantic_score = semantic_best_action(query)
        if semantic_action and semantic_score >= 0.72 and semantic_action != ActionType.LIST:
            result["primary_action"] = semantic_action
            result["semantic_action"] = semantic_action
            result["semantic_score"] = semantic_score

    # Default to LIST if no action detected
    if result["primary_action"] is None:
        result["primary_action"] = ActionType.LIST
    
    return result


def extract_filters(query: str) -> Dict[str, Any]:
    """Extract filter conditions from query"""
    filters = {}
    query_lower = query.lower()
    
    # Status filters
    status_values = ['pending', 'successful', 'failed', 'active', 'inactive', 'completed']
    for status in status_values:
        if status in query:
            filters['status'] = status.upper()
    
    # Type filters
    type_match = re.search(r'type\s+(?:is|=|:)\s+(\w+)', query)
    if type_match:
        filters['type'] = type_match.group(1).upper()
    
    # Date/time filters
    if any(word in query for word in ['today', 'yesterday', 'last week', 'last month', 'last year']):
        filters['time_period'] = extract_time_period(query)
    
    # Amount filters
    amount_match = re.search(r'(?:amount|balance|total)\s*(?:>|<|>=|<=|=)\s*(\d+)', query)
    if amount_match:
        filters['amount_operator'] = re.search(r'(>|<|>=|<=|=)', query).group(1)
        filters['amount_value'] = float(amount_match.group(1))
    
    # "with" filters (e.g., "customers with errors")
    with_match = re.search(r'with\s+(\w+)', query)
    if with_match:
        filters['has_field'] = with_match.group(1)
    
    # NEW: Text-based filters
    
    # 1. "starting with X" filter
    starts_match = re.search(r'starting\s+with\s+([a-zA-Z])', query_lower)
    if starts_match:
        filters['starts_with'] = starts_match.group(1).upper()
    
    # 2. "similar names" filter
    if 'similar' in query_lower and any(word in query_lower for word in ['name', 'names', 'merchant', 'customer']):
        filters['similar_names'] = True
    
    # 3. "contains text" filter
    contains_match = re.search(r'(?:contains?|having|with)\s+["\'](.+?)["\']', query_lower)
    if contains_match:
        filters['contains_text'] = contains_match.group(1)
    
    return filters


def extract_fields(query: str) -> List[str]:
    """Extract field names mentioned in query"""
    fields = []
    
    # Common field patterns
    field_keywords = [
        'name', 'email', 'phone', 'address', 'amount', 'balance', 'status',
        'type', 'date', 'time', 'id', 'customerId', 'merchantId', 
        'transactionId', 'accountNumber', 'bankName', 'location'
    ]
    
    for field in field_keywords:
        # Match field name or camelCase variations
        if field.lower() in query:
            fields.append(field)
    
    # Look for "show X and Y" patterns
    show_match = re.search(r'show\s+([\w\s,]+?)(?:\s+(?:for|from|in|where)|$)', query)
    if show_match:
        items = show_match.group(1).split(',')
        for item in items:
            item = item.strip()
            if item and item not in ['all', 'the', 'a', 'an']:
                fields.append(item.replace(' ', ''))
    
    return list(set(fields))  # Remove duplicates


def extract_sort_field(query: str) -> Optional[str]:
    """Extract field to sort by"""
    # "by amount", "by date", "sorted by"
    sort_match = re.search(r'(?:sort(?:ed)?\s+by|by)\s+(\w+)', query)
    if sort_match:
        return sort_match.group(1)
    
    # Common implicit sorting
    if 'amount' in query or 'balance' in query or 'total' in query:
        return 'amount'
    elif 'date' in query or 'recent' in query or 'latest' in query:
        return 'date'
    
    return None


def extract_group_by_field(query: str) -> Optional[str]:
    """Extract field to group by"""
    # "by customer", "per merchant", "for each status"
    patterns = [
        r'(?:by|per|for each)\s+(customer|merchant|status|type|date|location)',
        r'group(?:ed)?\s+by\s+(\w+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, query)
        if match:
            return match.group(1)
    
    return None


def extract_time_period(query: str) -> Dict[str, str]:
    """Extract time period from query"""
    if 'today' in query:
        return {'period': 'today'}
    elif 'yesterday' in query:
        return {'period': 'yesterday'}
    elif 'last week' in query:
        return {'period': 'last_week'}
    elif 'last month' in query:
        return {'period': 'last_month'}
    elif 'last year' in query or 'previous year' in query:
        return {'period': 'last_year'}
    elif 'this year' in query:
        return {'period': 'this_year'}
    
    return {}


def extract_comparison(query: str) -> Dict[str, Any]:
    """Extract comparison details"""
    comparison = {
        'enabled': True,
        'entities': [],
        'metric': None
    }
    
    # Find "A vs B" or "A and B" patterns
    vs_match = re.search(r'(\w+)\s+(?:vs|versus|and)\s+(\w+)', query)
    if vs_match:
        comparison['entities'] = [vs_match.group(1), vs_match.group(2)]
    
    # Determine comparison metric
    if 'amount' in query or 'total' in query:
        comparison['metric'] = 'amount'
    elif 'count' in query or 'number' in query:
        comparison['metric'] = 'count'
    
    return comparison


if __name__ == "__main__":
    # Test cases
    test_queries = [
        "How many customers have share money transactions?",
        "List top 10 merchants by transaction amount",
        "Show average balance for active customers",
        "Count transactions by status",
        "Get all customers with pending payments",
        "What is the total amount of successful transactions?",
        "Compare customer and merchant wallet balances",
        "Show me the last 5 transactions",
        "List unique transaction types",
        "Find highest transaction amount",
        "Group transactions by type and count them",
    ]
    
    print("=" * 80)
    print("🎯 ACTION DETECTOR TEST")
    print("=" * 80)
    
    for query in test_queries:
        print(f"\n❓ Query: '{query}'")
        action = detect_action(query)
        print(f"   Primary Action: {action['primary_action']}")
        if action['secondary_actions']:
            print(f"   Secondary: {action['secondary_actions']}")
        if action['aggregation']:
            print(f"   Aggregation: {action['aggregation']}")
        if action['filters']:
            print(f"   Filters: {action['filters']}")
        if action['limit']:
            print(f"   Limit: {action['limit']}")
        if action['group_by']:
            print(f"   Group By: {action['group_by']}")
        if action['fields']:
            print(f"   Fields: {action['fields'][:5]}")
