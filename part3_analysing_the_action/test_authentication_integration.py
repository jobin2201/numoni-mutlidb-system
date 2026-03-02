#!/usr/bin/env python
"""
Test Part 3 Integration with Authentication Database
Verifies that action_executor works with authentication collections
"""
import sys
import os
import json
from pathlib import Path

# Add paths
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))

from db_keyword_router_fuzzy import detect_database
from collection_router import detect_collection
from action_detector import detect_action
from action_executor import execute_action

def test_authentication_pipeline():
    """Test authentication database support throughout pipeline"""
    
    print("=" * 70)
    print("TESTING PART 3 AUTHENTICATION INTEGRATION")
    print("=" * 70)
    
    test_cases = [
        {
            "query": "Show me user authentication details",
            "expected_db": "authentication",
            "expected_collection": "authuser"
        },
        {
            "query": "List all login activities",
            "expected_db": "authentication",
            "expected_collection": "login_activities"
        },
        {
            "query": "Show user sessions",
            "expected_db": "authentication",
            "expected_collection": "user_sessions"
        },
        {
            "query": "Display OTP records",
            "expected_db": "authentication",
            "expected_collection": "otp"
        },
        {
            "query": "Show audit trail events",
            "expected_db": "authentication",
            "expected_collection": "audit_trail"
        }
    ]
    
    passed = 0
    failed = 0
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n[TEST {i}] Query: {test_case['query']}")
        print("-" * 70)
        
        try:
            # PART 1: Database Detection
            print("  Part 1: Detecting database...")
            db_result = detect_database(test_case['query'])
            detected_db = db_result.get('selected_database') or db_result.get('selected_dbs', ['unknown'])[0]
            db_confidence = db_result.get('confidence', 0)
            
            print(f"    ✓ Detected DB: {detected_db} (confidence: {db_confidence:.0%})")
            
            # PART 2: Collection Detection
            print("  Part 2: Detecting collection...")
            collection_result = detect_collection(test_case['query'], detected_db)
            detected_collection = collection_result['selected_collection']
            coll_confidence = collection_result['confidence']
            
            print(f"    ✓ Detected Collection: {detected_collection} (confidence: {coll_confidence:.0%})")
            
            # PART 3: Action Detection & Execution
            print("  Part 3: Detecting action...")
            action_metadata = detect_action(test_case['query'])
            action = action_metadata['primary_action']
            
            print(f"    ✓ Detected Action: {action}")
            
            # Execute action
            print("  Executing action on authentication database...")
            result = execute_action(
                action_metadata,
                detected_db,
                detected_collection,
                alternative_collections=collection_result.get('alternatives'),
                advanced_filters={}
            )
            
            print(f"    ✓ Action executed successfully")
            print(f"    Result Count: {result.get('result_count', 'N/A')}")
            
            # Validation
            is_correct_db = detected_db == test_case['expected_db']
            is_correct_coll = detected_collection == test_case['expected_collection']
            
            if is_correct_db and is_correct_coll:
                print(f"  ✅ TEST PASSED")
                passed += 1
            else:
                print(f"  ❌ TEST FAILED")
                if not is_correct_db:
                    print(f"     Expected DB: {test_case['expected_db']}, Got: {detected_db}")
                if not is_correct_coll:
                    print(f"     Expected Collection: {test_case['expected_collection']}, Got: {detected_collection}")
                failed += 1
        
        except Exception as e:
            print(f"  ❌ TEST FAILED WITH ERROR: {e}")
            failed += 1
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total Tests: {len(test_cases)}")
    print(f"Passed: {passed} ✅")
    print(f"Failed: {failed} ❌")
    print(f"Success Rate: {(passed/len(test_cases)*100):.1f}%")
    print("=" * 70)
    
    return failed == 0

if __name__ == "__main__":
    success = test_authentication_pipeline()
    sys.exit(0 if success else 1)
