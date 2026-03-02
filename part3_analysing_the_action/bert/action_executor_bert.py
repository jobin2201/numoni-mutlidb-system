#!/usr/bin/env python
"""
Action Executor - Executes detected actions on MongoDB collections
Performs actual database operations based on action metadata
"""
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from sentence_transformers import SentenceTransformer, util
from action_detector_bert import ActionType


# File is in part3_analysing_the_action/bert/; go up to numoni_final
BASE_DIR = Path(__file__).resolve().parents[2]
DATABASES_PATH = BASE_DIR / "databases"

# -----------------------------
# BERT Model Initialization
# -----------------------------
print("🤖 Loading BERT model for action execution...")
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


def load_collection_data(database_name: str, collection_name: str) -> List[Dict]:
    """Load data from a JSON collection file"""
    db_path = DATABASES_PATH / database_name / f"{collection_name}.json"
    
    if not db_path.exists():
        return []
    
    try:
        with open(db_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception as e:
        print(f"Error loading {db_path}: {e}")
        return []


def execute_action(
    action_metadata: Dict[str, Any],
    database_name: str,
    collection_name: str,
    alternative_collections: Optional[List[Dict]] = None,
    advanced_filters: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute action on the selected collection(s)
    
    Args:
        action_metadata: Output from action_detector
        database_name: Which database (numoni_customer/numoni_merchant/authentication)
        collection_name: Primary collection to query
        alternative_collections: List of alternative collections with similar scores
        advanced_filters: Part 4 advanced filters (date, location, numeric)
    
    Returns:
        {
            "action": str,
            "collection": str,
            "result": Any,
            "result_count": int,
            "alternative_results": list,  # If multiple collections have same score
            "summary": str,
            "visualization_type": str  # For UI hints
        }
    """
    
    # Load primary collection data
    data = load_collection_data(database_name, collection_name)
    
    if not data:
        return {
            "action": action_metadata["primary_action"],
            "collection": collection_name,
            "result": None,
            "error": f"Collection '{collection_name}' is empty or not found",
            "summary": f"No data available in {collection_name}"
        }
    
    # Apply basic filters first (from Part 3)
    filtered_data = apply_filters(data, action_metadata["filters"])
    
    # Apply advanced filters (from Part 4)
    filter_messages = []
    if advanced_filters and advanced_filters.get('has_advanced_filters'):
        # Import here to avoid circular dependency
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent / "part4_analysing_the_filters"))
        from advanced_filter_executor import apply_advanced_filters
        
        filtered_data, filter_messages = apply_advanced_filters(
            filtered_data, 
            advanced_filters, 
            collection_name
        )
    
    # Execute primary action
    primary_result = execute_single_action(
        filtered_data,
        action_metadata,
        collection_name
    )
    
    # Execute on alternative collections if their scores are same/close
    alternative_results = []
    if alternative_collections:
        for alt in alternative_collections[:2]:  # Top 2 alternatives
            alt_collection = alt.get("collection")
            alt_data = load_collection_data(database_name, alt_collection)
            if alt_data:
                alt_filtered = apply_filters(alt_data, action_metadata["filters"])
                alt_result = execute_single_action(
                    alt_filtered,
                    action_metadata,
                    alt_collection
                )
                alternative_results.append({
                    "collection": alt_collection,
                    "result": alt_result
                })
    
    return {
        "action": action_metadata["primary_action"],
        "collection": collection_name,
        "result": primary_result,
        "alternative_results": alternative_results,
        "filters_applied": action_metadata["filters"],
        "filter_messages": filter_messages,  # NEW: validation messages from Part 4
        "summary": format_summary(primary_result, action_metadata, collection_name),
        "visualization_type": suggest_visualization(action_metadata)
    }


def execute_single_action(
    data: List[Dict],
    action_metadata: Dict[str, Any],
    collection_name: str
) -> Any:
    """Execute action on a single dataset"""
    
    action = action_metadata["primary_action"]
    
    if action == ActionType.COUNT:
        return execute_count(data, action_metadata)
    
    elif action == ActionType.LIST:
        return execute_list(data, action_metadata)
    
    elif action == ActionType.SUM:
        return execute_sum(data, action_metadata)
    
    elif action == ActionType.AVERAGE:
        return execute_average(data, action_metadata)
    
    elif action == ActionType.MAX:
        return execute_max(data, action_metadata)
    
    elif action == ActionType.MIN:
        return execute_min(data, action_metadata)
    
    elif action == ActionType.TOP_N:
        return execute_top_n(data, action_metadata)
    
    elif action == ActionType.BOTTOM_N:
        return execute_bottom_n(data, action_metadata)
    
    elif action == ActionType.GROUP_BY:
        return execute_group_by(data, action_metadata)
    
    elif action == ActionType.DISTINCT:
        return execute_distinct(data, action_metadata)
    
    else:
        return execute_list(data, action_metadata)


def apply_filters(data: List[Dict], filters: Dict[str, Any]) -> List[Dict]:
    """Apply filter conditions to data"""
    if not filters:
        return data
    
    filtered = data
    
    # Status filter
    if 'status' in filters:
        filtered = [r for r in filtered if r.get('status', '').upper() == filters['status']]
    
    # Type filter
    if 'type' in filters:
        filtered = [r for r in filtered if r.get('type', '').upper() == filters['type']]
    
    # Amount filter
    if 'amount_operator' in filters and 'amount_value' in filters:
        op = filters['amount_operator']
        val = filters['amount_value']
        
        for field in ['amount', 'totalAmount', 'transactionAmount', 'balance']:
            if any(field in r for r in filtered):
                if op == '>':
                    filtered = [r for r in filtered if float(r.get(field, 0)) > val]
                elif op == '<':
                    filtered = [r for r in filtered if float(r.get(field, 0)) < val]
                elif op == '>=':
                    filtered = [r for r in filtered if float(r.get(field, 0)) >= val]
                elif op == '<=':
                    filtered = [r for r in filtered if float(r.get(field, 0)) <= val]
                elif op == '=':
                    filtered = [r for r in filtered if float(r.get(field, 0)) == val]
                break
    
    # Text-based filters (NEW)
    if 'starts_with' in filters:
        # Find name fields (businessName, merchantName, name, userName, etc.)
        starts_with_value = filters['starts_with'].upper()
        name_fields = ['businessName', 'merchantName', 'name', 'userName', 'customerName', 'fullName']
        
        for field in name_fields:
            # Check if this field exists in data
            if any(field in r for r in filtered):
                filtered = [r for r in filtered 
                           if r.get(field, '').upper().startswith(starts_with_value)]
                break
    
    if 'similar_names' in filters:
        # Find similar names using BERT semantic similarity
        name_fields = ['businessName', 'merchantName', 'name', 'userName', 'customerName', 'fullName']
        
        # Find which name field exists
        name_field = None
        for field in name_fields:
            if any(field in r for r in filtered):
                name_field = field
                break
        
        if name_field:
            # Extract all names
            names = [r.get(name_field, '') for r in filtered if r.get(name_field)]
            
            # Find similar names (semantic similarity > 0.70)
            similar_groups = {}
            for name in names:
                # Check against all other names
                for other_name in names:
                    if name != other_name:
                        similarity = semantic_similarity(name.lower(), other_name.lower())
                        if similarity > 0.70:
                            # Group similar names
                            if name not in similar_groups:
                                similar_groups[name] = []
                            similar_groups[name].append(other_name)
            
            # Filter to only records with similar names
            if similar_groups:
                similar_name_set = set()
                for name, similar in similar_groups.items():
                    similar_name_set.add(name)
                    similar_name_set.update(similar)
                
                filtered = [r for r in filtered if r.get(name_field, '') in similar_name_set]
    
    if 'contains_text' in filters:
        # Search for text in any string field
        search_text = filters['contains_text'].lower()
        
        filtered = [
            r for r in filtered
            if any(
                search_text in str(v).lower()
                for v in r.values()
                if isinstance(v, (str, int, float))
            )
        ]
    
    return filtered


def clean_mongodb_formatting(data: List[Dict]) -> List[Dict]:
    """
    Clean MongoDB object formatting from data for display
    - Removes MongoDB ObjectId strings like {"$oid":"..."}
    - Removes internal _class field
    - Converts MongoDB date format to readable strings
    """
    if not data:
        return data
    
    cleaned_data = []
    for record in data:
        cleaned_record = {}
        for field, value in record.items():
            # Skip internal MongoDB fields
            if field in ['_class']:
                continue
            
            # Skip pure MongoDB ObjectId strings
            if isinstance(value, str) and value.startswith('{"$oid"'):
                continue
            
            # Handle MongoDB date format
            if isinstance(value, dict) and '$date' in value:
                try:
                    from datetime import datetime
                    # Extract ISO string and convert
                    date_str = value['$date']
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    cleaned_record[field] = dt.strftime('%Y-%m-%d')
                except:
                    cleaned_record[field] = value
            else:
                cleaned_record[field] = value
        
        if cleaned_record:
            cleaned_data.append(cleaned_record)
    
    return cleaned_data


def execute_count(data: List[Dict], metadata: Dict) -> Dict:
    """Execute COUNT operation"""
    if metadata.get("group_by"):
        return execute_group_by(data, metadata)
    
    return {
        "count": len(data),
        "total_records": len(data)
    }


def filter_important_columns(data: List[Dict], collection_name: str = "") -> List[Dict]:
    """Filter to show only important columns for readability"""
    if not data:
        return data
    
    # Define important fields per collection type
    important_fields = {
        'default': ['name', 'businessName', 'merchantName', 'userName', 'customerName', 
                   'email', 'phoneNumber', 'createdDt', 'createdDate', 'date', 
                   'amount', 'totalAmount', 'balance', 'status', 'type',
                   'currency', 'merchantFee', 'payoutAmount', 'fee', 'commission',
                   'accountNumber', 'accountName', 'bankName', 'bankCode', 'reference'],
        'transaction': ['customerId', 'merchantId', 'transactionName', 'totalAmountPaid',
                       'balanceBefore', 'balanceAfter', 'transactionType', 
                       'transactionDate', 'createdDate', 'status', 'currency', 'fee',
                       'accountNumber', 'accountName', 'reference'],
        'payout': ['type', 'currency', 'merchantFee', 'payoutAmount', 'status', 
                  'createdDt', 'merchantId', 'customerId', 'fee', 'commission',
                  'accountNumber', 'accountName', 'bankName', 'bankCode', 'reference'],
        'notification': ['type', 'currency', 'merchantFee', 'payoutAmount', 'status', 
                        'createdDt', 'message', 'title', 'fee',
                        'accountNumber', 'accountName', 'bankName', 'bankCode', 'reference'],
        'details': ['name', 'businessName', 'userName', 'customerName', 'merchantName',
                   'email', 'phoneNumber', 'createdDt', 'isEmailVerified', 'isPhoneVerified',
                   'accountNumber', 'accountName', 'bankName', 'bankCode'],
        'location': ['street', 'address', 'city', 'country', 'homeNumber', 'isDefault', 'createdDt'],
        'geography': ['regionName', 'region', 'state', 'country', 'lga', 'locations'],
        'region': ['regionName', 'name', 'state', 'country', 'lga', 'code'],
        'deals': ['heading', 'description', 'dealName', 'merchantName', 'amount', 'discount',
                 'startDate', 'endDate', 'status', 'type', 'createdDt', 'image'],
    }
    
    # Determine which field set to use
    collection_lower = collection_name.lower()
    if 'payout' in collection_lower:
        fields_to_keep = important_fields['payout']
    elif 'notification' in collection_lower:
        fields_to_keep = important_fields['notification']
    elif 'transaction' in collection_lower:
        fields_to_keep = important_fields['transaction']
    elif 'location' in collection_lower or 'customer_location' in collection_lower:
        fields_to_keep = important_fields['location']
    elif 'geography' in collection_lower:
        fields_to_keep = important_fields['geography']
    elif 'region' in collection_lower:
        fields_to_keep = important_fields['region']
    elif 'deals' in collection_lower or 'deal' in collection_lower:
        fields_to_keep = important_fields['deals']
    elif 'details' in collection_lower:
        fields_to_keep = important_fields['details']
    else:
        fields_to_keep = important_fields['default']
    
    # Filter each record and clean MongoDB ObjectIds
    filtered_data = []
    for record in data:
        filtered_record = {}
        for field in fields_to_keep:
            if field in record:
                value = record[field]
                # Clean MongoDB ObjectId format
                if isinstance(value, str) and value.startswith('{"$oid"'):
                    continue  # Skip MongoDB ObjectId fields
                filtered_record[field] = value
        
        # Always keep some fields if they exist
        if filtered_record:
            filtered_data.append(filtered_record)
        else:
            # If no important fields found, keep original but clean ObjectIds
            cleaned_record = {}
            for field, value in record.items():
                # Skip MongoDB ObjectId fields that are pure IDs
                if isinstance(value, str) and value.startswith('{"$oid"'):
                    continue
                # Skip internal MongoDB _class field
                if field == '_class':
                    continue
                cleaned_record[field] = value
            
            if cleaned_record:
                filtered_data.append(cleaned_record)
    
    return filtered_data


def sort_by_name(data: List[Dict], ascending: bool = True) -> List[Dict]:
    """Sort data by name field (ascending by default)"""
    if not data:
        return data
    
    # Find name field
    name_fields = ['name', 'businessName', 'merchantName', 'userName', 'customerName', 'fullName']
    name_field = None
    
    for field in name_fields:
        if field in data[0]:
            name_field = field
            break
    
    if name_field:
        return sorted(data, key=lambda x: str(x.get(name_field, '')).lower(), reverse=not ascending)
    
    return data


def execute_list(data: List[Dict], metadata: Dict) -> List[Dict]:
    """Execute LIST operation"""
    limit = metadata.get("limit", 100)  # Default limit to avoid huge responses
    
    # Sort by name field (ascending) if no explicit sort specified
    if not metadata.get("sort_by"):
        data = sort_by_name(data, ascending=True)
    elif metadata.get("sort_by"):
        data = sort_data(data, metadata["sort_by"], metadata.get("sort_order", "desc"))
    
    # Filter to important columns only
    data = filter_important_columns(data[:limit])
    
    return data


def execute_sum(data: List[Dict], metadata: Dict) -> Dict:
    """Execute SUM operation"""
    # Find numeric fields to sum
    numeric_fields = find_numeric_fields(data)
    
    results = {}
    for field in numeric_fields:
        total = sum(float(r.get(field, 0)) for r in data if r.get(field))
        results[field] = round(total, 2)
    
    return {
        "sums": results,
        "record_count": len(data)
    }


def execute_average(data: List[Dict], metadata: Dict) -> Dict:
    """Execute AVERAGE operation"""
    numeric_fields = find_numeric_fields(data)
    
    results = {}
    for field in numeric_fields:
        values = [float(r.get(field, 0)) for r in data if r.get(field)]
        if values:
            results[field] = round(sum(values) / len(values), 2)
    
    return {
        "averages": results,
        "record_count": len(data)
    }


def execute_max(data: List[Dict], metadata: Dict) -> Dict:
    """Execute MAX operation"""
    numeric_fields = find_numeric_fields(data)
    
    results = {}
    for field in numeric_fields:
        values = [float(r.get(field, 0)) for r in data if r.get(field)]
        if values:
            max_val = max(values)
            results[field] = {
                "max": max_val,
                "record": next(r for r in data if float(r.get(field, 0)) == max_val)
            }
    
    return results


def execute_min(data: List[Dict], metadata: Dict) -> Dict:
    """Execute MIN operation"""
    numeric_fields = find_numeric_fields(data)
    
    results = {}
    for field in numeric_fields:
        values = [float(r.get(field, 0)) for r in data if r.get(field)]
        if values:
            min_val = min(values)
            results[field] = {
                "min": min_val,
                "record": next(r for r in data if float(r.get(field, 0)) == min_val)
            }
    
    return results


def execute_top_n(data: List[Dict], metadata: Dict) -> List[Dict]:
    """Execute TOP N operation"""
    limit = metadata.get("limit", 10)
    sort_by = metadata.get("sort_by") or find_primary_sort_field(data)
    
    sorted_data = sort_data(data, sort_by, "desc")
    return sorted_data[:limit]


def execute_bottom_n(data: List[Dict], metadata: Dict) -> List[Dict]:
    """Execute BOTTOM N operation"""
    limit = metadata.get("limit", 10)
    sort_by = metadata.get("sort_by") or find_primary_sort_field(data)
    
    sorted_data = sort_data(data, sort_by, "asc")
    return sorted_data[:limit]


def execute_group_by(data: List[Dict], metadata: Dict) -> Dict:
    """Execute GROUP BY operation"""
    group_field = metadata.get("group_by")
    
    if not group_field:
        return {"error": "No group_by field specified"}
    
    # Group data
    groups = {}
    for record in data:
        key = record.get(group_field, "Unknown")
        if key not in groups:
            groups[key] = []
        groups[key].append(record)
    
    # Apply aggregation
    aggregation = metadata.get("aggregation", "count")
    
    results = {}
    for key, records in groups.items():
        if aggregation == "count":
            results[key] = len(records)
        elif aggregation == "sum":
            numeric_fields = find_numeric_fields(records)
            if numeric_fields:
                field = numeric_fields[0]
                results[key] = sum(float(r.get(field, 0)) for r in records)
        elif aggregation == "avg":
            numeric_fields = find_numeric_fields(records)
            if numeric_fields:
                field = numeric_fields[0]
                values = [float(r.get(field, 0)) for r in records if r.get(field)]
                results[key] = sum(values) / len(values) if values else 0
    
    return {
        "grouped_by": group_field,
        "results": results,
        "total_groups": len(results)
    }


def execute_distinct(data: List[Dict], metadata: Dict) -> Dict:
    """Execute DISTINCT operation"""
    fields = metadata.get("fields", [])
    
    if not fields:
        # Find key fields automatically
        if data:
            fields = [k for k in data[0].keys() if not k.startswith('_')][:5]
    
    distinct_values = {}
    for field in fields:
        values = set()
        for record in data:
            val = record.get(field)
            if val is not None:
                values.add(str(val))
        distinct_values[field] = sorted(list(values))[:20]  # Limit to 20
    
    return {
        "distinct_values": distinct_values,
        "fields_analyzed": fields
    }


# Helper functions
def find_numeric_fields(data: List[Dict]) -> List[str]:
    """Find numeric fields in data"""
    if not data:
        return []
    
    numeric_fields = []
    sample = data[0]
    
    # Known numeric field names
    numeric_field_names = [
        'amount', 'totalAmount', 'transactionAmount', 'totalAmountPaid',
        'balance', 'balanceBefore', 'balanceAfter', 'walletBalance',
        'points', 'bonusAmount', 'rewardPoints', 'count', 'quantity',
        'price', 'total', 'subtotal', 'tax', 'fee', 'commission'
    ]
    
    for key, value in sample.items():
        if key.startswith('_'):
            continue
        
        # Check if field name is known numeric
        if key in numeric_field_names:
            numeric_fields.append(key)
            continue
        
        # Check if value is numeric type
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            # Exclude ID fields except specific ones
            if 'id' not in key.lower():
                numeric_fields.append(key)
    
    return numeric_fields


def find_primary_sort_field(data: List[Dict]) -> str:
    """Find the most appropriate field to sort by"""
    numeric_fields = find_numeric_fields(data)
    
    # Prefer amount-related fields
    for field in ['amount', 'totalAmount', 'balance', 'transactionAmount']:
        if field in numeric_fields:
            return field
    
    # Fallback to first numeric field
    return numeric_fields[0] if numeric_fields else list(data[0].keys())[0]


def sort_data(data: List[Dict], sort_by: str, order: str = "desc") -> List[Dict]:
    """Sort data by a field"""
    try:
        # Try numeric sort first
        sorted_data = sorted(
            data,
            key=lambda x: float(x.get(sort_by, 0)) if x.get(sort_by) else 0,
            reverse=(order == "desc")
        )
    except (ValueError, TypeError):
        # Fall back to string sort
        sorted_data = sorted(
            data,
            key=lambda x: str(x.get(sort_by, '')),
            reverse=(order == "desc")
        )
    
    return sorted_data


def format_summary(result: Any, metadata: Dict, collection: str) -> str:
    """Format a human-readable summary"""
    action = metadata["primary_action"]
    
    if action == ActionType.COUNT:
        if isinstance(result, dict) and "count" in result:
            return f"Found {result['count']} records in {collection}"
        elif isinstance(result, dict) and "results" in result:
            total = sum(result["results"].values())
            return f"Total {total} records grouped by {result.get('grouped_by', 'field')}"
    
    elif action == ActionType.LIST:
        count = len(result) if isinstance(result, list) else 0
        return f"Showing {count} records from {collection}"
    
    elif action == ActionType.SUM:
        if isinstance(result, dict) and "sums" in result:
            return f"Sum calculated for {len(result['sums'])} fields"
    
    elif action == ActionType.AVERAGE:
        if isinstance(result, dict) and "averages" in result:
            return f"Average calculated for {len(result['averages'])} fields"
    
    elif action == ActionType.TOP_N:
        count = len(result) if isinstance(result, list) else 0
        return f"Top {count} records from {collection}"
    
    return f"Action {action} executed on {collection}"


def suggest_visualization(metadata: Dict) -> str:
    """Suggest visualization type for UI"""
    action = metadata["primary_action"]
    
    if action == ActionType.COUNT:
        return "number"
    elif action == ActionType.LIST:
        return "table"
    elif action in [ActionType.SUM, ActionType.AVERAGE]:
        return "bar_chart"
    elif action == ActionType.GROUP_BY:
        return "grouped_bar_chart"
    elif action in [ActionType.TOP_N, ActionType.BOTTOM_N]:
        return "ranked_list"
    else:
        return "table"
