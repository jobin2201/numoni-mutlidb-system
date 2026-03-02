#!/usr/bin/env python
"""Simple content search across all collections"""
import sys
import os
import json
import re
from typing import List, Dict, Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part3_analysing_the_action'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'part2_analysing_the_collection'))

from action_executor import load_collection_data
from collection_router import load_metadata

def search_in_record(record: Dict, search_term: str) -> bool:
    """Check if search term appears in any field of the record"""
    search_lower = search_term.lower()
    for value in record.values():
        if isinstance(value, str) and search_lower in value.lower():
            return True
        elif isinstance(value, dict):  # Handle nested dicts
            for v in value.values():
                if isinstance(v, str) and search_lower in str(v).lower():
                    return True
    return False

def get_important_fields(record: Dict) -> Dict:
    """Extract only important fields from a record"""
    important = ['name', 'businessName', 'merchantName', 'customerName', 'bankName', 
                'description', 'email', 'phoneNumber', 'accountNumber', 'status',
                'amount', 'balance', 'terminalId', 'serialNumber', 'model']
    
    result = {}
    for key, value in record.items():
        if key in important or any(imp in key.lower() for imp in ['name', 'bank', 'account', 'number']):
            result[key] = value
    
    return result

def search_all_collections(search_terms: List[str], max_results_per_collection: int = 5):
    """Search all collections for the given terms"""
    results = {}
    
    databases = ["numoni_customer", "numoni_merchant"]
    
    for db in databases:
        print(f"\n{'='*80}")
        print(f"Searching in: {db}")
        print('='*80)
        
        metadata = load_metadata(db)
        
        for collection_name in metadata.keys():
            try:
                data = load_collection_data(db, collection_name)
                if not data:
                    continue
                
                matches = []
                for record in data:
                    for term in search_terms:
                        if search_in_record(record, term):
                            matches.append(record)
                            break  # Found one term, no need to check others
                
                if matches:
                    print(f"\n📁 {collection_name}: Found {len(matches)} matches")
                    
                    # Show first few matches with important fields only
                    for i, match in enumerate(matches[:max_results_per_collection], 1):
                        important = get_important_fields(match)
                        if important:
                            print(f"  {i}. {important}")
                    
                    if len(matches) > max_results_per_collection:
                        print(f"  ... and {len(matches) - max_results_per_collection} more")
                    
                    results[f"{db}.{collection_name}"] = len(matches)
            
            except Exception as e:
                continue
    
    return results

if __name__ == "__main__":
    print("="*80)
    print("SIMPLE CONTENT SEARCH")
    print("="*80)
    
    # Search 1: Moniepoint
    print("\n\n🔍 SEARCHING FOR: Moniepoint")
    search_all_collections(["Moniepoint", "monie point"])
    
    # Search 2: Banks
    print("\n\n🔍 SEARCHING FOR: STERLING / ZENITH Banks")
    search_all_collections(["STERLING", "sterling bank", "ZENITH", "ZENITH BANK"])
