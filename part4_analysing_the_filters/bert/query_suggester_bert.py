import sys
from pathlib import Path

try:
    from keywords_lookup import KEYWORDS_DB
except ModuleNotFoundError:
    # Allow direct execution/import from bert folder
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from keywords_lookup import KEYWORDS_DB
from sentence_transformers import SentenceTransformer, util

print("🤖 Loading BERT model for query suggestions...")
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
    if not text1 or not text2:
        return 0.0

    if BERT_MODEL is None:
        return 0.0

    try:
        emb1 = get_embedding(text1)
        emb2 = get_embedding(text2)
        return float(util.cos_sim(emb1, emb2).item())
    except Exception:
        return 0.0

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
    """Find tables/fields matching a query word with BERT semantic matching"""
    query_word_lower = query_word.lower()
    results = {}
    
    for kw, tables in KEYWORD_INDEX.items():
        if query_word_lower == kw:
            score = 1.0
        elif query_word_lower in kw or kw in query_word_lower:
            score = 0.8
        else:
            score = semantic_similarity(query_word_lower, kw)
        
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
