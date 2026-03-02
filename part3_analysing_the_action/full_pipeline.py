#!/usr/bin/env python
"""
Integrated Full Pipeline: Part 1 + Part 2 + Part 3
Database Detection → Collection Detection → Action Execution
"""
import sys
import os
from pathlib import Path

# Add parent directories to path
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))

from db_keyword_router_fuzzy import detect_database
from collection_router import detect_collection
from action_detector import detect_action
from action_executor import execute_action


def full_pipeline(user_query: str) -> dict:
    """
    Complete pipeline from query to results
    
    Returns:
        {
            "query": str,
            "part1_database": {...},
            "part2_collection": {...},
            "part3_action": {...},
            "final_results": {...}
        }
    """
    
    print("=" * 80)
    print("🚀 FULL PIPELINE EXECUTION")
    print("=" * 80)
    print(f"\n❓ Query: '{user_query}'")
    
    # PART 1: Detect Database
    print("\n" + "─" * 80)
    print("📊 PART 1: Database Detection")
    print("─" * 80)
    
    db_result = detect_database(user_query)
    print(f"✅ Database: {db_result['selected_database']}")
    print(f"   Confidence: {db_result['confidence']}")
    print(f"   Reason: {db_result['reason'][:100]}")
    
    # PART 2: Detect Collection
    print("\n" + "─" * 80)
    print("📋 PART 2: Collection Detection")
    print("─" * 80)
    
    collection_result = detect_collection(user_query, db_result['selected_database'])
    print(f"✅ Collection: {collection_result['selected_collection']}")
    print(f"   Confidence: {collection_result['confidence']}")
    print(f"   Score: {collection_result.get('score', 0):.1f} points")
    
    if collection_result.get('matched_fields'):
        print(f"   Matched Fields: {', '.join(collection_result['matched_fields'][:5])}")
    
    # Show alternatives
    alternatives = collection_result.get('alternatives', [])
    if alternatives:
        print(f"   Alternatives:")
        for i, alt in enumerate(alternatives[:3], 1):
            print(f"      {i}. {alt['collection']}: {alt['score']:.1f} pts (conf: {alt['confidence']:.2f})")
    
    # PART 3: Detect and Execute Action
    print("\n" + "─" * 80)
    print("⚡ PART 3: Action Detection & Execution")
    print("─" * 80)
    
    action_metadata = detect_action(user_query)
    print(f"✅ Action: {action_metadata['primary_action']}")
    
    if action_metadata.get('aggregation'):
        print(f"   Aggregation: {action_metadata['aggregation']}")
    if action_metadata.get('filters'):
        print(f"   Filters: {action_metadata['filters']}")
    if action_metadata.get('limit'):
        print(f"   Limit: {action_metadata['limit']}")
    if action_metadata.get('group_by'):
        print(f"   Group By: {action_metadata['group_by']}")
    
    # Check if alternatives have same/similar score (within 10%)
    primary_score = collection_result.get('score', 0)
    close_alternatives = []
    
    for alt in alternatives[:2]:
        if alt['score'] >= primary_score * 0.9:  # Within 90% of primary score
            close_alternatives.append(alt)
    
    # Execute action
    print("\n🔄 Executing action...")
    execution_result = execute_action(
        action_metadata,
        db_result['selected_database'],
        collection_result['selected_collection'],
        close_alternatives if close_alternatives else None
    )
    
    # Display Results
    print("\n" + "=" * 80)
    print("📊 RESULTS")
    print("=" * 80)
    
    display_results(execution_result, action_metadata)
    
    # Compile complete response
    response = {
        "query": user_query,
        "part1_database": {
            "database": db_result['selected_database'],
            "confidence": db_result['confidence'],
            "reason": db_result['reason']
        },
        "part2_collection": {
            "collection": collection_result['selected_collection'],
            "confidence": collection_result['confidence'],
            "score": collection_result.get('score', 0),
            "alternatives": alternatives[:3]
        },
        "part3_action": {
            "action": action_metadata['primary_action'],
            "metadata": action_metadata
        },
        "final_results": execution_result
    }
    
    return response


def display_results(execution_result: dict, action_metadata: dict):
    """Display execution results in a user-friendly format"""
    
    action = action_metadata['primary_action']
    result = execution_result.get('result')
    collection = execution_result.get('collection')
    
    print(f"\n📌 Collection: {collection}")
    print(f"💡 Summary: {execution_result.get('summary', 'Done')}")
    
    if execution_result.get('error'):
        print(f"\n❌ Error: {execution_result['error']}")
        return
    
    # Display based on action type
    if action == 'count':
        if isinstance(result, dict) and 'count' in result:
            print(f"\n   🔢 Total Count: {result['count']:,}")
        elif isinstance(result, dict) and 'results' in result:
            print(f"\n   📊 Grouped Results:")
            for key, value in sorted(result['results'].items(), key=lambda x: x[1], reverse=True):
                print(f"      • {key}: {value:,}")
    
    elif action == 'list':
        if isinstance(result, list):
            print(f"\n   📋 Records ({len(result)}):")
            for i, record in enumerate(result[:10], 1):  # Show first 10
                # Display key fields
                display_fields = ['name', 'customerId', 'merchantId', 'amount', 'status', 'type']
                displayed = {k: v for k, v in record.items() if k in display_fields and v is not None}
                
                if not displayed:
                    # Show first 3 fields
                    displayed = {k: v for k, v in list(record.items())[:3]}
                
                print(f"      {i}. {displayed}")
            
            if len(result) > 10:
                print(f"      ... and {len(result) - 10} more records")
    
    elif action in ['sum', 'average']:
        if isinstance(result, dict):
            results_key = 'sums' if action == 'sum' else 'averages'
            if results_key in result:
                print(f"\n   📈 Results:")
                for field, value in result[results_key].items():
                    print(f"      • {field}: {value:,.2f}")
    
    elif action in ['max', 'min']:
        if isinstance(result, dict):
            print(f"\n   🎯 Results:")
            for field, data in result.items():
                if isinstance(data, dict) and 'max' in data:
                    print(f"      • {field}: {data['max']:,.2f}")
                elif isinstance(data, dict) and 'min' in data:
                    print(f"      • {field}: {data['min']:,.2f}")
    
    elif action in ['top_n', 'bottom_n']:
        if isinstance(result, list):
            print(f"\n   🏆 Top Results ({len(result)}):")
            for i, record in enumerate(result, 1):
                # Find the most relevant field
                amount_fields = ['amount', 'totalAmount', 'balance', 'transactionAmount']
                amount_val = next((record.get(f) for f in amount_fields if record.get(f)), None)
                
                key_field = record.get('name') or record.get('customerId') or record.get('merchantId') or 'Record'
                print(f"      {i}. {key_field}: {amount_val if amount_val else '...'}")
    
    # Display alternative results if available
    if execution_result.get('alternative_results'):
        print(f"\n   📊 Alternative Results from Similar Collections:")
        for alt in execution_result['alternative_results']:
            alt_collection = alt['collection']
            alt_result = alt['result']
            print(f"\n   → {alt_collection}:")
            
            if isinstance(alt_result, dict) and 'count' in alt_result:
                print(f"      Count: {alt_result['count']:,}")
            elif isinstance(alt_result, list):
                print(f"      Records: {len(alt_result)}")


if __name__ == "__main__":
    # Test queries
    test_queries = [
        "How many customers have share money type transactions?",
        "List top 10 customers by wallet balance",
        "Show total transaction amount for successful payments",
        "Count transactions by status",
        "What is the average load money amount?",
        "List all merchants in Lagos",
        "Show me customer errors",
        "Get the highest transaction amount",
    ]
    
    print("\n" + "=" * 80)
    print("🧪 TESTING FULL PIPELINE")
    print("=" * 80)
    
    for i, query in enumerate(test_queries[:3], 1):  # Test first 3
        print(f"\n\n{'=' * 80}")
        print(f"TEST {i}/{len(test_queries[:3])}")
        print('=' * 80)
        
        try:
            result = full_pipeline(query)
            print("\n✅ Pipeline completed successfully")
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
        
        print("\n" + "─" * 80)
        input("Press Enter for next test...")
