#!/usr/bin/env python
"""
Quick Test - Execute a simple query end-to-end
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

# Test query
query = "How many customers have share money type transactions?"

print("=" * 80)
print("🧪 QUICK TEST - End-to-End")
print("=" * 80)
print(f"\n❓ Query: '{query}'")

# Part 1
print("\n📊 Part 1: Database")
db_result = detect_database(query)
selected_db = db_result.get('selected_database') or db_result.get('selected_dbs', ['unknown'])[0]
print(f"   → {selected_db}")
print(f"   Confidence: {db_result.get('confidence', 'N/A')}")

# Part 2
print("\n📋 Part 2: Collection")
collection_result = detect_collection(query, selected_db)
print(f"   → {collection_result['selected_collection']} (score: {collection_result.get('score', 0):.1f})")

# Part 3: Action
print("\n⚡ Part 3: Action")
action_metadata = detect_action(query)
print(f"   → {action_metadata['primary_action']}")

# Part 3: Execute
print("\n🔄 Executing...")
execution_result = execute_action(
    action_metadata,
    selected_db,
    collection_result['selected_collection']
)

# Display
print("\n" + "=" * 80)
print("📊 RESULT")
print("=" * 80)
print(f"\n{execution_result.get('summary')}")

if execution_result.get('error'):
    print(f"❌ Error: {execution_result['error']}")
else:
    result = execution_result.get('result')
    if isinstance(result, dict) and 'count' in result:
        print(f"\n✅ Count: {result['count']:,} records")
    elif isinstance(result, list):
        print(f"\n✅ Found {len(result)} records")
        if result:
            print(f"\nSample record fields: {list(result[0].keys())[:10]}")

print("\n✅ Test completed!")
