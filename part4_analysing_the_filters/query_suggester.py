from keywords_lookup import KEYWORDS_DB
from difflib import SequenceMatcher

# Build reverse index: keyword -> (database, table)
KEYWORD_INDEX = {}

for db_name in ['CUSTOMER_DB', 'MERCHANT_DB']:
    for table_name, fields in KEYWORDS_DB[db_name].items():
        
        # Index table name variations
        keywords_to_index = [table_name]
        
        # Add field names  
        keywords_to_index.extend(fields)
        
        # Add field name variations (e.g., customerId -> customer, id)
        for field in fields:
            parts = field.replace('_', ' ').lower().split()
            keywords_to_index.extend(parts)
        
        for kw in keywords_to_index:
            kw_lower = kw.lower()
            if kw_lower not in KEYWORD_INDEX:
                KEYWORD_INDEX[kw_lower] = []
            KEYWORD_INDEX[kw_lower].append((db_name, table_name))

def search_keywords(query_word, limit=10):
    """Find tables/fields matching a query word with fuzzy matching"""
    query_word_lower = query_word.lower()
    results = {}
    
    for kw, tables in KEYWORD_INDEX.items():
        if query_word_lower == kw:
            score = 1.0
        elif query_word_lower in kw or kw in query_word_lower:
            score = 0.8
        else:
            score = SequenceMatcher(None, query_word_lower, kw).ratio()
        
        if score >= 0.6:
            for db, table in tables:
                key = (db, table)
                if key not in results:
                    results[key] = score
                else:
                    results[key] = max(results[key], score)
    
    # Sort by score and return unique tables
    sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)
    unique_tables = {}
    for (db, table), score in sorted_results:
        if table not in unique_tables:
            unique_tables[table] = (db, score)
    
    return {t: s[0] for t, s in list(unique_tables.items())[:limit]}

def get_query_suggestions(query):
    """Get table suggestions based on query keywords"""
    words = query.lower().replace(',', '').split()
    suggestions = {}
    
    for word in words:
        if len(word) > 2:
            tables = search_keywords(word, limit=5)
            for table, db in tables.items():
                key = table
                if key not in suggestions:
                    suggestions[key] = (db, 1)
                else:
                    suggestions[key] = (suggestions[key][0], suggestions[key][1] + 1)
    
    # Sort by frequency
    sorted_sugg = sorted(suggestions.items(), key=lambda x: x[1][1], reverse=True)
    return [{'table': t, 'database': s[0], 'score': s[1]} for t, s in sorted_sugg[:5]]
