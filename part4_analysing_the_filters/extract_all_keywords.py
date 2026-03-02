import json
import os
from collections import defaultdict

def extract_keywords_from_json(filepath):
    """Extract field names from a JSON file"""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list) and len(data) > 0:
            return list(data[0].keys()) if isinstance(data[0], dict) else []
        elif isinstance(data, dict):
            return list(data.keys())
        return []
    except:
        return []

db_path = "f:\\WORK\\adventure_dataset\\numoni_final\\databases"
keywords = {
    'CUSTOMER_DB': {},
    'MERCHANT_DB': {}
}

# Process CUSTOMER tables
customer_dir = os.path.join(db_path, 'numoni_customer')
for file in os.listdir(customer_dir):
    if file.endswith('.json'):
        table_name = file.replace('.json', '')
        filepath = os.path.join(customer_dir, file)
        fields = extract_keywords_from_json(filepath)
        keywords['CUSTOMER_DB'][table_name] = fields

# Process MERCHANT tables
merchant_dir = os.path.join(db_path, 'numoni_merchant')
for file in os.listdir(merchant_dir):
    if file.endswith('.json'):
        table_name = file.replace('.json', '')
        filepath = os.path.join(merchant_dir, file)
        fields = extract_keywords_from_json(filepath)
        keywords['MERCHANT_DB'][table_name] = fields

# Save to Python file
output = "KEYWORDS_FROM_TABLES = " + str(keywords)
with open('comprehensive_keywords.py', 'w') as f:
    f.write(output)

print(f"✓ Customer tables: {len(keywords['CUSTOMER_DB'])}")
print(f"✓ Merchant tables: {len(keywords['MERCHANT_DB'])}")
print(f"✓ Total tables: {len(keywords['CUSTOMER_DB']) + len(keywords['MERCHANT_DB'])}")
print("\nFile saved: comprehensive_keywords.py")
