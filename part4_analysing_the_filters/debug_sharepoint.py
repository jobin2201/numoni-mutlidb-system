#!/usr/bin/env python
"""Debug customer_sharepoint_requests collection"""
import sys
import os
from pprint import pprint

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part3_analysing_the_action'))

from action_executor import load_collection_data

data = load_collection_data("numoni_customer", "customer_sharepoint_requests")
print(f"Total records: {len(data)}")
print(f"\nFirst 3 records:")
for i, record in enumerate(data[:3]):
    print(f"\n{i+1}. ")
    pprint(record)

# Check all unique fields
print(f"\nAll fields in collection:")
all_fields = set()
for record in data:
    all_fields.update(record.keys())
print(sorted(all_fields))

# Check for "received" and "points" patterns in smsText
print(f"\nRecords with 'smsText' field:")
sms_count = 0
matching = 0
for record in data:
    sms_text = record.get('smsText', '')
    if sms_text:
        sms_count += 1
        if 'received' in str(sms_text).lower() and 'points' in str(sms_text).lower():
            matching += 1
            print(f"  ✓ {sms_text[:80]}")

print(f"\nTotal with smsText: {sms_count}")
print(f"Matching 'received' AND 'points': {matching}")
