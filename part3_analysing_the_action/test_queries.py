#!/usr/bin/env python
"""Test the 3 specific queries user reported issues with"""
import sys
import json
from full_pipeline import full_pipeline

test_queries = [
    "which customer received the highest numoni points",
    "what are the similar merchants names in deals",
    "show me any merchant starting with J"
]

print("=" * 80)
print("TESTING 3 FAILING QUERIES")
print("=" * 80)

for i, query in enumerate(test_queries, 1):
    print(f"\n{'=' * 80}")
    print(f"Query {i}: {query}")
    print("=" * 80)
    
    try:
        result = full_pipeline(query)
        
        # Show key information
        print(f"\nDatabase: {result.get('selected_database', 'N/A')}")
        print(f"Collection: {result.get('selected_collection', 'N/A')}")
        print(f"Confidence: {result.get('collection_confidence', 0)}")
        print(f"Action: {result.get('primary_action', 'N/A')}")
        
        # Show collection reason
        if 'collection_reason' in result:
            print(f"Why: {result['collection_reason'][:150]}")
        
        # Show results if available
        if 'action_result' in result:
            action_result = result['action_result']
            if 'result' in action_result:
                res = action_result['result']
                if isinstance(res, list):
                    print(f"\nResults: {len(res)} records")
                    if len(res) > 0:
                        print(f"Sample: {json.dumps(res[0], default=str)[:200]}")
                elif isinstance(res, dict):
                    print(f"\nResult data: {json.dumps(res, default=str)[:300]}")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

print(f"\n{'=' * 80}")
print("TESTING COMPLETE")
print("=" * 80)
