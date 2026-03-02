import re

# Test the field spec removal
query = "show me payout notification. I want merchant fee"

field_spec_patterns = [
    r'[,\.]\s*i\s+(?:only\s+)?want\s+.+$',
    r'[,\.]\s*i\s+need\s+.+$',  
    r'[,\.]\s*looking\s+for\s+.+$',
    r'[,\.]\s*with\s+[a-z\s,]+$'
]

query_for_collection = query
for pattern in field_spec_patterns:
    query_for_collection = re.sub(pattern, '', query_for_collection, flags=re.IGNORECASE)
query_for_collection = query_for_collection.strip()

print(f"Original: {query}")
print(f"Cleaned:  {query_for_collection}")
print(f"✅ Field spec removed: {query != query_for_collection}")
