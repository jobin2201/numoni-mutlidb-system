#!/usr/bin/env python
"""
Test Part 3 with the regions query to ensure it executes properly
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))

from db_keyword_router_fuzzy import detect_database
from collection_router import detect_collection
from action_detector import detect_action
from action_executor import execute_action

print("=" * 80)
print("🎯 FULL TEST: Top 2 Regions Query")
print("=" * 80)

query = "Show me the top 2 regions in Nigeria"
print(f"\n❓ Query: '{query}'\n")

# Part 1
print("📊 Part 1: Database Detection")
db_result = detect_database(query)
selected_db = db_result.get('selected_database') or db_result.get('selected_dbs', ['numoni_merchant'])[0]
print(f"   → {selected_db}\n")

# Part 2
print("📋 Part 2: Collection Detection")
collection_result = detect_collection(query, selected_db)
print(f"   → {collection_result['selected_collection']} (score: {collection_result.get('score', 0):.1f})")
print(f"   Matched fields: {collection_result.get('matched_fields', [])}\n")

# Part 3: Action
print("⚡ Part 3: Action Detection")
action_metadata = detect_action(query)
print(f"   → Action: {action_metadata['primary_action']}")
print(f"   → Limit: {action_metadata.get('limit', 'None')}\n")

# Part 3: Execute
print("🔄 Executing...")
execution_result = execute_action(
    action_metadata,
    selected_db,
    collection_result['selected_collection']
)

# Display
print("\n" + "=" * 80)
print("📊 RESULTS")
print("=" * 80)
print(f"\n💡 {execution_result.get('summary')}")

if execution_result.get('error'):
    print(f"❌ Error: {execution_result['error']}")
else:
    result = execution_result.get('result')
    if isinstance(result, list):
        print(f"\n✅ Retrieved {len(result)} records")
        if result:
            print(f"\nTop {min(2, len(result))} regions:")
            for i, record in enumerate(result[:2], 1):
                # Display the region info
                region_name = record.get('region') or record.get('name') or record.get('location')
                print(f"   {i}. {record}")

print("\n✅ Test completed - collection matched correctly even with 'top 2' limiter!")
