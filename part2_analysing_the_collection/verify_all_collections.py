#!/usr/bin/env python
"""Verify all collections are analyzed properly"""
import json
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Load metadata
with open(BASE_DIR / 'numoni_customer_collections_metadata.json') as f:
    customer_meta = json.load(f)

with open(BASE_DIR / 'numoni_merchant_collections_metadata.json') as f:
    merchant_meta = json.load(f)

print("=" * 70)
print("COLLECTION ANALYSIS VERIFICATION")
print("=" * 70)

print(f"\nCustomer Collections: {len(customer_meta)}")
print("-" * 70)
for name, data in sorted(customer_meta.items()):
    status = "[EMPTY]" if data.get('status') == 'empty' else f"{data['total_records']:,} records"
    fields = len(data.get('fields', []))
    print(f"  {name:40} {status:20} {fields} fields")

print(f"\nMerchant Collections: {len(merchant_meta)}")
print("-" * 70)
for name, data in sorted(merchant_meta.items()):
    status = "[EMPTY]" if data.get('status') == 'empty' else f"{data['total_records']:,} records"
    fields = len(data.get('fields', []))
    print(f"  {name:40} {status:20} {fields} fields")

# Count empties
customer_empty = sum(1 for d in customer_meta.values() if d.get('status') == 'empty')
merchant_empty = sum(1 for d in merchant_meta.values() if d.get('status') == 'empty')

print("\n" + "=" * 70)
print(f"SUMMARY:")
print(f"  Total Customer Collections: {len(customer_meta)} ({customer_empty} empty)")
print(f"  Total Merchant Collections: {len(merchant_meta)} ({merchant_empty} empty)")
print(f"  Total Collections Analyzed: {len(customer_meta) + len(merchant_meta)}")
print("=" * 70)

# Verify token counts
import json
customer_str = json.dumps(customer_meta)
merchant_str = json.dumps(merchant_meta)

customer_tokens = len(customer_str.split()) * 1.3
merchant_tokens = len(merchant_str.split()) * 1.3

print(f"\nToken Estimates:")
print(f"  Customer metadata: ~{int(customer_tokens)} tokens")
print(f"  Merchant metadata: ~{int(merchant_tokens)} tokens")
print(f"  Both under 2000 token limit: {'YES' if customer_tokens < 2000 and merchant_tokens < 2000 else 'NO'}")
