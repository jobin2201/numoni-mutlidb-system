#!/usr/bin/env python
"""
Ranking Query Handler - Proper NLP-based ranking with field matching
Handles: "rank X by Y", "rank X based on Y", "rank X by count of Y", etc.
"""
import re
from typing import List, Dict, Any, Tuple
from collections import defaultdict, Counter
from sentence_transformers import SentenceTransformer, util

print("🤖 Loading BERT model for ranking handler...")
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

def similarity(a: str, b: str) -> float:
    """Calculate string similarity"""
    if not a or not b or BERT_MODEL is None:
        return 0.0

    try:
        emb1 = get_embedding(a.lower().strip())
        emb2 = get_embedding(b.lower().strip())
        return float(util.cos_sim(emb1, emb2).item())
    except Exception:
        return 0.0

def find_best_match_field(needle: str, record: Dict, threshold: float = 0.6) -> Tuple[str, Any]:
    """Find the best matching field in a record for given needle"""
    needle_lower = needle.lower()
    
    # Direct match first
    for key, value in record.items():
        if needle_lower in key.lower() or (isinstance(value, str) and needle_lower in value.lower()):
            return (key, value)
    
    # Semantic match
    best_score = 0
    best_key = None
    best_value = None
    
    for key, value in record.items():
        score = similarity(needle, key)
        if isinstance(value, str):
            score = max(score, similarity(needle, value))
        
        if score > best_score and score >= threshold:
            best_score = score
            best_key = key
            best_value = value
    
    return (best_key, best_value) if best_key else (None, None)

def match_entity_to_collection(rank_entity: str, metadata: Dict[str, Any]) -> Tuple[str, float]:
    """
    Intelligently match a rank_entity (from user query) to the best collection in metadata.
    Handles: "customer error" → "customerError", "transactions" → "transaction_history", etc.
    
    Returns: (collection_name, confidence_score)
    """
    rank_entity_lower = rank_entity.lower()
    
    # Priority mappings for common queries
    entity_to_collection = {
        # Customer entities
        'customer error': ['customerError', 'customer_error', 'error'],
        'customer errors': ['customerError', 'customer_error', 'error'],
        'error': ['customerError', 'customer_error'],
        'customer location': ['customer_location', 'customerLocation', 'location'],
        'customer session': ['customer_session', 'customerSession', 'session'],
        'customer transaction': ['transaction_history', 'transactions', 'transaction'],
        'favorite deal': ['favourite_deals', 'favoritedeals', 'favorites_deals', 'favorite_deal'],
        'favourite deal': ['favourite_deals', 'favoritedeals', 'favorites_deals', 'favorite_deal'],
        'wallet': ['wallet', 'wallets'],
        'transaction': ['transaction_history', 'transactions', 'transaction'],
        'transfer': ['transaction_history', 'transactions'],
        
        # Merchant entities
        'pos': ['pos', 'posTerminals', 'pos_terminals', 'terminal'],
        'merchant location': ['merchant_location', 'merchantLocation', 'location'],
        'geography': ['geography', 'geographies', 'region', 'regions'],
        'region': ['geography', 'geographies', 'regions'],
        'regions': ['geography', 'geographies', 'regions'],
        'state': ['geography', 'geographies', 'regions'],
    }
    
    # Check for exact entity match in priority mapping
    for entity_pattern, preferred_collections in entity_to_collection.items():
        if entity_pattern in rank_entity_lower:
            # Try to find any of the preferred collections in metadata
            for pref_coll in preferred_collections:
                for coll_name in metadata.keys():
                    if pref_coll.lower() == coll_name.lower():
                        return (coll_name, 0.95)  # High confidence for priority match
    
    # Fallback: semantic match against all collections
    best_collection = None
    best_score = 0
    
    for coll_name in metadata.keys():
        score = similarity(rank_entity, coll_name)
        
        # Boost score for substring matches
        if rank_entity_lower in coll_name.lower():
            score = min(1.0, score + 0.2)
        if coll_name.lower() in rank_entity_lower:
            score = min(1.0, score + 0.15)
        
        if score > best_score:
            best_score = score
            best_collection = coll_name
    
    return (best_collection, best_score) if best_collection else (None, 0.0)

def parse_ranking_query(query: str) -> Dict[str, Any]:
    """
    Parse ANY ranking query flexibly using NLP approach
    Handles: "rank X by Y", "rank X in Z based on Y", "rank X for Y", etc.
    
    Examples:
    - "rank transactions by business name" → entity: transactions, by: business name
    - "rank regions in nigeria based on state" → entity: regions, by: state
    - "rank customer error by retries" → entity: customer error, by: retries
    - "rank merchants for status" → entity: merchants, by: status
    """
    query_lower = query.lower().strip()
    
    # Check if this is a ranking query
    if 'rank' not in query_lower:
        return None
    
    # Remove "rank" and work with the rest
    query_clean = re.sub(r'^rank\s+', '', query_lower)
    
    rank_entity = None
    rank_by = None
    
    # Pattern separators (ordered by priority)
    separators = [
        (' based on ', 'based on'),
        (' by ', 'by'),
        (' for ', 'for'),
        (' according to ', 'according to'),
    ]
    
    # Find separator and split
    for sep, sep_name in separators:
        if sep in query_clean:
            parts = query_clean.split(sep, 1)
            rank_entity = parts[0].strip()
            rank_by = parts[1].strip() if len(parts) > 1 else None
            break
    
    # Fallback: if no separator found, try to split by space
    if not rank_entity or not rank_by:
        return None
    
    # Clean rank_entity: remove location/scope modifiers (in, from, within, across, of)
    # "regions in nigeria" → "regions"
    # "customers from database" → "customers"
    rank_entity = re.sub(r'\s+(in|from|within|across|of)\s+.+?(?=\s+by|\s+based|\s+for|$)', ' ', rank_entity).strip()
    rank_entity = re.sub(r'\s+', ' ', rank_entity).strip()  # Clean whitespace
    
    # Clean rank_by: remove metric prefixes
    # "count of retries" → "retries"
    # "total transactions" → "transactions"
    # "number of errors" → "errors"
    rank_by = re.sub(r'(?:count of |number of |total |sum of |average |avg of )', '', rank_by).strip()
    rank_by = re.sub(r'\s+', ' ', rank_by).strip()  # Clean whitespace
    
    # Detect metric from query if mentioned
    rank_metric = 'count'  # default
    if any(x in query_lower for x in ['sum of', 'total']):
        rank_metric = 'sum'
    elif any(x in query_lower for x in ['average', 'avg']):
        rank_metric = 'avg'
    elif any(x in query_lower for x in ['maximum', 'max']):
        rank_metric = 'max'
    elif any(x in query_lower for x in ['minimum', 'min']):
        rank_metric = 'min'
    
    # Validate extraction
    if not rank_entity or not rank_by or len(rank_entity) < 2 or len(rank_by) < 2:
        return None
    
    return {
        'rank_entity': rank_entity,
        'rank_by': rank_by,
        'rank_metric': rank_metric,
        'query_type': 'flexible_nhlp'
    }

def execute_ranking(data: List[Dict], 
                   rank_entity: str, 
                   rank_by: str,
                   rank_metric: str = 'count',
                   collection_name: str = '') -> Dict[str, Any]:
    """Execute ranking query on data with intelligent field matching"""
    
    if not data:
        return {'error': 'No data to rank', 'ranked': []}
    
    # Find all field names and their similarities to rank_entity and rank_by
    all_fields = set()
    for record in data:
        all_fields.update(record.keys())
    
    # STEP 1: Find field to group by (what to rank)
    # IMPORTANT: When user says "rank X based on Y", Y is what we GROUP BY
    # So prioritize matching rank_by first
    
    grouping_field = None
    grouping_score = 0
    
    # Strategy 1: Try to match rank_by first (this is usually what user wants to group by)
    # e.g., "rank customer location based on city" → group by "city" field
    # e.g., "rank transactions by business name" → group by "businessName" field
    
    for field in all_fields:
        score = similarity(rank_by, field)
        
        # Boost score for exact word matches
        if rank_by.lower() in field.lower() or field.lower() in rank_by.lower():
            score = min(1.0, score + 0.3)
        
        # Penalize ID fields unless user specifically asks for them
        if 'id' in field.lower() and 'id' not in rank_by.lower():
            score *= 0.3
        
        if score > grouping_score:
            grouping_score = score
            grouping_field = field
    
    # Strategy 2: If rank_by didn't match well, try rank_entity
    # (user might say "rank regions" meaning group by region field)
    if not grouping_field or grouping_score < 0.6:
        for field in all_fields:
            score = similarity(rank_entity, field)
            
            # Boost score for exact word matches
            if rank_entity.lower() in field.lower() or field.lower() in rank_entity.lower():
                score = min(1.0, score + 0.2)
            
            # Penalize ID fields
            if 'id' in field.lower():
                score *= 0.3
            
            if score > grouping_score:
                grouping_score = score
                grouping_field = field
    
    if not grouping_field:
        # Last resort: use the first non-id field
        for field in sorted(all_fields):
            if 'id' not in field.lower() and '_' not in field[:1]:
                grouping_field = field
                break
        
        if not grouping_field:
            return {'error': f'Could not find suitable field for "{rank_entity}"', 'ranked': []}
    
    # STEP 2: Find field to rank by (e.g., retries count, amount, etc.)
    ranking_field = None
    ranking_score = 0
    
    # Try to match rank_by to numeric fields first
    for field in all_fields:
        score = similarity(rank_by, field)
        if rank_by.lower() in field.lower() or field.lower() in rank_by.lower():
            score = min(1.0, score + 0.2)
        
        if score > ranking_score:
            ranking_score = score
            ranking_field = field
    
    # If no good match, look for any numeric field
    if not ranking_field or ranking_score < 0.5:
        for field in all_fields:
            # Check if field contains mostly numeric values
            numeric_count = 0
            for record in data[:min(10, len(data))]:
                if field in record and isinstance(record[field], (int, float)):
                    numeric_count += 1
            
            if numeric_count > 0:
                if rank_metric in ['sum', 'avg', 'max', 'min']:
                    ranking_field = field
                    break
    
    # Group and rank
    grouped = defaultdict(list)
    grouped_by_value = defaultdict(lambda: {'count': 0, 'sum': 0, 'max': 0, 'min': float('inf'), 'values': []})
    
    for record in data:
        group_key = record.get(grouping_field, 'N/A')
        grouped[group_key].append(record)
        
        # Track numeric field values for aggregation
        if ranking_field and ranking_field in record:
            value = record[ranking_field]
            if isinstance(value, (int, float)):
                grouped_by_value[group_key]['count'] += 1
                grouped_by_value[group_key]['sum'] += value
                grouped_by_value[group_key]['max'] = max(grouped_by_value[group_key]['max'], value)
                grouped_by_value[group_key]['min'] = min(grouped_by_value[group_key]['min'], value)
                grouped_by_value[group_key]['values'].append(value)
    
    # Rank by metric
    if rank_metric == 'count':
        ranked = sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True)
    elif rank_metric in ['sum', 'total'] and ranking_field and grouped_by_value:
        ranked = sorted(grouped_by_value.items(), key=lambda x: x[1]['sum'], reverse=True)
    elif rank_metric in ['avg', 'average'] and ranking_field and grouped_by_value:
        ranked = sorted(grouped_by_value.items(), 
                       key=lambda x: (x[1]['sum'] / x[1]['count']) if x[1]['count'] > 0 else 0, 
                       reverse=True)
    elif rank_metric == 'max' and ranking_field and grouped_by_value:
        ranked = sorted(grouped_by_value.items(), key=lambda x: x[1]['max'], reverse=True)
    else:
        ranked = sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True)
    
    return {
        'ranked': ranked,
        'grouping_field': grouping_field,
        'ranking_field': ranking_field,
        'rank_metric': rank_metric,
        'total_groups': len(ranked),
        'grouping_score': grouping_score,
        'ranking_score': ranking_score
    }

def format_ranking_results(ranking_result: Dict,
                          collection_name: str,
                          limit: int = 20) -> List[Dict]:
    """Format ranking results for display"""
    
    if 'error' in ranking_result:
        return {'error': ranking_result['error']}
    
    ranked = ranking_result.get('ranked', [])[:limit]
    grouping_field = ranking_result.get('grouping_field', '')
    ranking_field = ranking_result.get('ranking_field', '')
    rank_metric = ranking_result.get('rank_metric', 'count')
    
    # Important fields map
    important_map = {
        'pos': ['posName', 'terminalId', 'serialNumber', 'status', 'model', 'merchantId', 'merchantName'],
        'customerror': ['errorType', 'errorMessage', 'retries', 'status', 'customerId', 'createdDt'],
        'transaction': ['transactionName', 'amount', 'status', 'merchantName', 'customerName', 'senderBankName'],
        'customer': ['name', 'email', 'phoneNumber', 'status', 'balance', 'createdDt'],
        'merchant': ['businessName', 'name', 'email', 'phoneNumber', 'status', 'balance', 'category'],
        'location': ['street', 'address', 'city', 'country', 'homeNumber', 'isDefault'],
        'geography': ['regionName', 'region', 'state', 'country', 'lga'],
        'region': ['regionName', 'name', 'state', 'country', 'lga'],
        'default': ['name', 'businessName', 'status', 'amount', 'email', 'phoneNumber']
    }
    
    def get_important_fields(coll_name: str, record: Dict):
        """Get important fields from record based on collection, cleaning MongoDB formatting"""
        key = None
        for k in important_map.keys():
            if k in coll_name.lower():
                key = k
                break
        
        if not key:
            # Try to guess from fields
            record_keys = set(record.keys())
            if any(x in record_keys for x in ['errorType', 'retries']):
                key = 'customerror'
            elif any(x in record_keys for x in ['posName', 'terminalId']):
                key = 'pos'
            elif any(x in record_keys for x in ['transactionName', 'senderBankName']):
                key = 'transaction'
            elif any(x in record_keys for x in ['businessName']):
                key = 'merchant'
            elif any(x in record_keys for x in ['street', 'address', 'homeNumber']):
                key = 'location'
            elif any(x in record_keys for x in ['regionName', 'lga', 'state']):
                key = 'geography'
            else:
                key = 'default'
        
        fields = important_map.get(key, important_map['default'])
        cleaned_fields = {}
        
        for k, v in record.items():
            if k in fields and v is not None:
                # Skip MongoDB ObjectId formatting
                if isinstance(v, str) and v.startswith('{"$oid"'):
                    continue
                # Clean MongoDB date format
                if isinstance(v, dict) and '$date' in v:
                    try:
                        from datetime import datetime
                        date_str = v['$date']
                        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        cleaned_fields[k] = dt.strftime('%Y-%m-%d')
                    except:
                        cleaned_fields[k] = v
                else:
                    cleaned_fields[k] = v
        
        return cleaned_fields
    
    # Format results
    formatted = []
    for rank, (group_key, group_data) in enumerate(ranked, 1):
        record_count = len(group_data) if isinstance(group_data, list) else 1
        
        # Get sample important fields
        sample_record = group_data[0] if isinstance(group_data, list) and group_data else group_data
        if isinstance(sample_record, dict):
            important = get_important_fields(collection_name, sample_record)
        else:
            important = {}
        
        result = {
            'Rank': rank,
            'Name': str(group_key)[:60] if group_key != 'N/A' else 'Unknown',
            'Count': record_count,
            **{f'_{k}': v for k, v in list(important.items())[:4]}
        }
        
        formatted.append(result)
    
    return formatted
