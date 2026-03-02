#!/usr/bin/env python
"""
Test the specific issue: regions query with and without "top N"
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection"))

from collection_router import detect_collection

print("=" * 80)
print("🧪 TEST: Regions Query - With and Without 'Top N'")
print("=" * 80)

test_cases = [
    {
        "query": "Show me the regions in Nigeria",
        "expected": "nigeria_regions",
        "note": "Without TOP N limiter"
    },
    {
        "query": "Show me the top 2 regions in Nigeria",
        "expected": "nigeria_regions",
        "note": "With TOP 2 limiter"
    },
    {
        "query": "Show me the top 10 regions in Nigeria",
        "expected": "nigeria_regions",
        "note": "With TOP 10 limiter"
    },
    {
        "query": "List all regions",
        "expected": "nigeria_regions",
        "note": "Simple regions query"
    },
    {
        "query": "Get top 5 regions",
        "expected": "nigeria_regions",
        "note": "Top 5 regions"
    }
]

print("\n📊 Testing with MERCHANT database:\n")

for i, test in enumerate(test_cases, 1):
    print(f"{i}. Query: '{test['query']}'")
    print(f"   Note: {test['note']}")
    print(f"   Expected: {test['expected']}")
    
    result = detect_collection(test['query'], "numoni_merchant")
    
    selected = result['selected_collection']
    score = result.get('score', 0)
    confidence = result['confidence']
    
    if selected == test['expected']:
        print(f"   ✅ PASS: {selected} (score: {score:.1f}, confidence: {confidence:.2f})")
    else:
        print(f"   ❌ FAIL: Got {selected} (score: {score:.1f})")
        print(f"      Reason: {result['reason'][:100]}")
    
    # Show alternatives
    if result.get('alternatives'):
        print(f"   Alternatives:")
        for j, alt in enumerate(result['alternatives'][:3], 1):
            indicator = "⚠️" if alt['collection'] == test['expected'] else "  "
            print(f"      {indicator} {j}. {alt['collection']}: {alt['score']:.1f} pts")
    
    print()

print("=" * 80)
print("✅ Test Complete")
print("=" * 80)
