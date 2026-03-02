#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Integrated Router: Database + Collection Detection
Combines Part 1 (DB detection) and Part 2 (Collection detection)
"""
import sys
import os
from pathlib import Path

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add part1 to path - use absolute path
part1_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'part1_analysing_the_db'))
if part1_path not in sys.path:
    sys.path.insert(0, part1_path)

try:
    from db_keyword_router_fuzzy import detect_database
    from collection_router import detect_collection
except ImportError as e:
    print(f"❌ Import error: {e}")
    print(f"   Searching in: {part1_path}")
    print(f"   File exists: {os.path.exists(os.path.join(part1_path, 'db_keyword_router_fuzzy.py'))}")
    sys.exit(1)


def full_query_routing(user_query):
    """
    Complete routing: Database -> Collection
    
    Returns:
        dict with database_info and collection_info
    """
    
    # Step 1: Detect database
    db_result = detect_database(user_query)
    
    selected_dbs = db_result.get("selected_dbs", [])
    
    if not selected_dbs or selected_dbs == ["unknown"]:
        return {
            "database_info": db_result,
            "collection_info": None,
            "status": "error",
            "message": "Could not determine which database to use"
        }
    
    # Step 2: Detect collection for each database
    collection_results = {}
    
    for db_name in selected_dbs:
        if db_name in ["numoni_customer", "numoni_merchant", "authentication"]:
            collection_result = detect_collection(user_query, db_name)
            collection_results[db_name] = collection_result
    
    # If only one DB, return its collection
    if len(selected_dbs) == 1:
        db_name = selected_dbs[0]
        return {
            "database": db_name,
            "database_info": db_result,
            "collection_info": collection_results.get(db_name),
            "status": "success"
        }
    
    # If multiple DBs, return all
    return {
        "databases": selected_dbs,
        "database_info": db_result,
        "collection_info": collection_results,
        "status": "success_multiple"
    }


if __name__ == "__main__":
    print("=" * 80)
    print("🚀 INTEGRATED ROUTER: Database + Collection Detection")
    print("=" * 80)
    
    test_queries = [
        # Customer queries
        "Show me all customer details",
        "How many customers have errors?",
        "List customer wallet balances",
        "Get customer transaction history",
        "Show customer locations in Lagos",
        "customer load money records",
        "OTP verification status",
        
        # Merchant queries
        "Show merchant business information",
        "List all merchant bank accounts",
        "Get merchant locations",
        "Show all deals",
        "merchant wallet transactions",
        "POS terminal details",
        "merchant payout history",
        "merchant reviews and ratings",
        
        # Bank/business names
        "MONIEPOINT MICROFINANCE details",
        "Show Chrisllar Global transactions",
        
        # Ambiguous (could be both)
        "Show all transactions",
        "wallet balance",
    ]
    
    for query in test_queries:
        print(f"\n{'=' * 80}")
        print(f"❓ Query: '{query}'")
        print("-" * 80)
        
        result = full_query_routing(query)
        
        if result["status"] == "error":
            print(f"❌ Error: {result['message']}")
            print(f"   Reason: {result['database_info'].get('reason')}")
        
        elif result["status"] == "success":
            db_name = result["database"]
            collection_info = result["collection_info"]
            
            print(f"✅ Database: {db_name}")
            print(f"   Reason: {result['database_info']['reason']}")
            
            if collection_info:
                print(f"\n📊 Collection: {collection_info['selected_collection']}")
                print(f"   Confidence: {collection_info['confidence']} ({collection_info.get('score', 0)} points)")
                print(f"   Reason: {collection_info['reason']}")
                print(f"   Total Records: {collection_info.get('total_records', 0):,}")
                
                if collection_info.get('matched_fields'):
                    print(f"   Key Fields: {', '.join(collection_info['matched_fields'][:5])}")
                
                if collection_info.get('alternatives'):
                    print(f"\n   📌 Alternatives:")
                    for alt in collection_info['alternatives'][:2]:
                        print(f"      - {alt['collection']} (confidence: {alt['confidence']})")
        
        elif result["status"] == "success_multiple":
            print(f"✅ Databases: {', '.join(result['databases'])}")
            print(f"   Reason: {result['database_info']['reason']}")
            
            print(f"\n📊 Collections per Database:")
            for db_name, collection_info in result["collection_info"].items():
                if collection_info and collection_info['selected_collection']:
                    print(f"\n   {db_name}:")
                    print(f"      Collection: {collection_info['selected_collection']}")
                    print(f"      Confidence: {collection_info['confidence']}")
                    print(f"      Records: {collection_info.get('total_records', 0):,}")
    
    print(f"\n{'=' * 80}")
    print("✅ Integrated routing complete!")
    print("=" * 80)
