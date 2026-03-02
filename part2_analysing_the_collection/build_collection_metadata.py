#!/usr/bin/env python
"""
Build lightweight collection metadata for fast NLP-based collection detection
Keeps metadata under 2000 tokens for small model usage
"""
import os
import json
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATABASES_PATH = BASE_DIR / "databases"
OUTPUT_PATH = Path(__file__).parent


def extract_collection_metadata(json_file_path, max_records=100, max_unique_values=4):
    """Extract key metadata from a collection JSON file
    
    Args:
        max_records: How many records to scan for diverse values (100)
        max_unique_values: Maximum unique values to store per field (REDUCED to 4 for token efficiency)
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Include empty collections too
        if not isinstance(data, list):
            return None
        
        if len(data) == 0:
            return {
                "total_records": 0,
                "fields": [],
                "sample_values": {},
                "status": "empty"
            }
        
        # Sample more records to get diverse values
        sample_records = data[:min(max_records, len(data))]
        
        # Extract all unique field names
        all_fields = set()
        # Store MULTIPLE unique values per field (as list)
        field_values_collector = {}
        
        for record in sample_records:
            if isinstance(record, dict):
                for key, value in record.items():
                    all_fields.add(key)
                    
                    # Store sample values (only important simple types, skip internal fields)
                    if key.startswith('_') or key in ['createdDt', 'updatedDt', '_class']:
                        continue
                    
                    if value is None:
                        continue
                    
                    # Collect multiple unique values per field
                    if isinstance(value, (str, int, float, bool)):
                        if key not in field_values_collector:
                            field_values_collector[key] = set()
                        
                        # Add value (truncated if string)
                        value_str = str(value)[:50]
                        if len(field_values_collector[key]) < max_unique_values:
                            field_values_collector[key].add(value_str)
        
        # Convert sets to sorted lists for JSON serialization
        sample_values = {
            key: sorted(list(values)) for key, values in field_values_collector.items()
        }
        
        # Keep only first 12 most relevant fields (REDUCED from 15 to save tokens)
        relevant_fields = [f for f in sorted(all_fields) if not f.startswith('_')][:12]
        
        return {
            "total_records": len(data),
            "fields": relevant_fields,
            "sample_values": sample_values
        }
    
    except Exception as e:
        print(f"Error processing {json_file_path}: {e}")
        return None


def build_collection_keywords():
    """Define keywords and patterns for collection detection"""
    return {
        # Customer collections
        "customerDetails": {
            "keywords": ["customer", "user", "person", "profile", "name", "email", "phone"],
            "fields": ["name", "email", "phoneNo", "userId"],
            "description": "Main customer profile information"
        },
        "customer_wallet_ledger": {
            "keywords": ["wallet", "balance", "ledger", "transaction", "money", "amount"],
            "fields": ["balance", "amount", "transactionType"],
            "description": "Customer wallet transactions and balance"
        },
        "customerError": {
            "keywords": ["error", "issue", "problem", "failure", "exception"],
            "fields": ["errorMessage", "errorCode"],
            "description": "Customer-related system errors"
        },
        "customerlocation": {
            "keywords": ["location", "address", "city", "state", "region", "place"],
            "fields": ["address", "city", "state"],
            "description": "Customer location and address details"
        },
        "customer_load_money": {
            "keywords": ["load", "topup", "top up", "add money", "deposit", "fund"],
            "fields": ["amount", "status"],
            "description": "Customer money loading/top-up records"
        },
        "customer_points_ledger": {
            "keywords": ["points", "rewards", "loyalty", "earned", "redeemed"],
            "fields": ["points", "pointsBalance"],
            "description": "Customer reward points ledger"
        },
        "customer_share_money": {
            "keywords": ["share", "send", "transfer", "give money"],
            "fields": ["amount", "recipientId"],
            "description": "Customer money sharing/sending records"
        },
        "transaction_history": {
            "keywords": ["transaction", "history", "payment", "purchase", "order"],
            "fields": ["transactionId", "amount", "status"],
            "description": "Transaction history records"
        },
        "wallet": {
            "keywords": ["wallet", "balance", "account"],
            "fields": ["balance", "walletId"],
            "description": "Wallet account information"
        },
        "tokens": {
            "keywords": ["token", "auth", "authentication", "session"],
            "fields": ["token", "userId"],
            "description": "Authentication tokens"
        },
        "payment_otp_verification": {
            "keywords": ["otp", "verification", "code", "verify"],
            "fields": ["otp", "verified"],
            "description": "OTP verification for payments"
        },
        "invoice": {
            "keywords": ["invoice", "bill", "receipt", "payment"],
            "fields": ["invoiceId", "amount"],
            "description": "Invoice records"
        },
        "favourite_deal": {
            "keywords": ["favourite", "favorite", "liked", "saved", "bookmarked"],
            "fields": ["dealId", "customerId"],
            "description": "Customer favorite deals"
        },
        "sponsored_deals": {
            "keywords": ["sponsored", "promotion", "ad", "advertisement"],
            "fields": ["dealId", "sponsor"],
            "description": "Sponsored deal promotions"
        },

        # Authentication collections
        "authuser": {
            "keywords": ["auth user", "authentication user", "login user", "user account"],
            "fields": ["username", "email", "phone", "status"],
            "description": "Authentication users and credentials"
        },
        "login_activities": {
            "keywords": ["login activity", "login activities", "login history", "signin activity"],
            "fields": ["userId", "ip", "device", "status"],
            "description": "User login activity records"
        },
        "signin_records": {
            "keywords": ["signin", "sign in", "signin records", "sign in records"],
            "fields": ["userId", "status", "createdAt"],
            "description": "User sign-in records"
        },
        "user_sessions": {
            "keywords": ["user sessions", "sessions", "active session"],
            "fields": ["userId", "sessionId", "status"],
            "description": "User session records"
        },
        "userDeviceDetail": {
            "keywords": ["user device", "device detail", "device info"],
            "fields": ["userId", "deviceId", "deviceName"],
            "description": "User device details"
        },
        "otp": {
            "keywords": ["otp", "one time password", "verification code"],
            "fields": ["otp", "userId", "status"],
            "description": "OTP and verification codes"
        },
        "refresh_token_record": {
            "keywords": ["refresh token", "token refresh", "refresh record"],
            "fields": ["userId", "token", "expiresAt"],
            "description": "Refresh token records"
        },
        "refreshtoken": {
            "keywords": ["refresh token", "token", "session token"],
            "fields": ["userId", "token", "expiresAt"],
            "description": "Refresh tokens"
        },
        "roles": {
            "keywords": ["role", "roles", "permission", "permissions", "access"],
            "fields": ["name", "permissions"],
            "description": "Role and permission definitions"
        },
        "audit_trail": {
            "keywords": ["audit", "audit trail", "activity log", "logs"],
            "fields": ["action", "userId", "createdAt"],
            "description": "Audit trail events"
        },
        "account_deletion_request": {
            "keywords": ["account deletion", "delete account", "deactivation"],
            "fields": ["userId", "status", "reason"],
            "description": "Account deletion requests"
        },
        "id_seq": {
            "keywords": ["sequence", "id seq", "counter"],
            "fields": ["name", "seq"],
            "description": "ID sequence counters"
        },
        
        # Merchant collections
        "merchantDetails": {
            "keywords": ["merchant", "business", "shop", "store", "vendor", "seller"],
            "fields": ["businessName", "brandName", "email"],
            "description": "Main merchant business profile"
        },
        "bankInformation": {
            "keywords": ["bank", "account", "banking", "account number", "bank code"],
            "fields": ["bankname", "accountNo", "bankcode"],
            "description": "Merchant bank account information"
        },
        "merchantlocation": {
            "keywords": ["location", "address", "city", "state", "region"],
            "fields": ["address", "city", "state"],
            "description": "Merchant business location"
        },
        "merchant_wallet_ledger": {
            "keywords": ["wallet", "ledger", "balance", "transaction"],
            "fields": ["balance", "amount"],
            "description": "Merchant wallet transactions"
        },
        "merchant_reward_points_ledger": {
            "keywords": ["points", "rewards", "loyalty"],
            "fields": ["points", "balance"],
            "description": "Merchant reward points"
        },
        "merchant_payout": {
            "keywords": ["payout", "withdrawal", "settlement", "disbursement"],
            "fields": ["amount", "status"],
            "description": "Merchant payout/withdrawal records"
        },
        "deals": {
            "keywords": ["deal", "offer", "discount", "promotion", "sale"],
            "fields": ["dealId", "title", "description"],
            "description": "Merchant deals and offers"
        },
        "reviews": {
            "keywords": ["review", "rating", "feedback", "comment"],
            "fields": ["rating", "comment"],
            "description": "Merchant reviews and ratings"
        },
        "pos": {
            "keywords": ["pos", "terminal", "device", "machine"],
            "fields": ["posId", "terminalId"],
            "description": "POS terminal devices"
        },
        "notifications": {
            "keywords": ["notification", "alert", "message", "notify"],
            "fields": ["message", "type"],
            "description": "Merchant notifications"
        },
        "category": {
            "keywords": ["category", "type", "classification"],
            "fields": ["categoryName", "categoryId"],
            "description": "Business categories"
        },
        "rewards": {
            "keywords": ["rewards", "incentive", "bonus"],
            "fields": ["rewardId", "amount"],
            "description": "Reward programs"
        }
    }


def generate_metadata_files():
    """Generate lightweight metadata files for all databases"""
    
    for db_name in ["numoni_customer", "numoni_merchant", "authentication"]:
        db_path = DATABASES_PATH / db_name
        
        if not db_path.exists():
            print(f"Database not found: {db_path}")
            continue
        
        print(f"\n{'='*70}")
        print(f"Processing: {db_name}")
        print(f"{'='*70}")
        
        collections_metadata = {}
        
        # Process each JSON file
        for json_file in sorted(db_path.glob("*.json")):
            collection_name = json_file.stem
            print(f"  - {collection_name}...", end=" ")
            
            metadata = extract_collection_metadata(json_file)
            
            if metadata:
                collections_metadata[collection_name] = metadata
                if metadata.get('status') == 'empty':
                    print(f"✓ (0 records, empty collection)")
                else:
                    print(f"✓ ({metadata['total_records']} records, {len(metadata['fields'])} fields)")
            else:
                print("✗ (invalid format)")
        
        # Save metadata
        output_file = OUTPUT_PATH / f"{db_name}_collections_metadata.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(collections_metadata, f, indent=2)
        
        print(f"\n✅ Saved: {output_file}")
        print(f"   Collections: {len(collections_metadata)}")
        
        # Calculate token estimate (rough)
        metadata_str = json.dumps(collections_metadata)
        estimated_tokens = len(metadata_str.split()) * 1.3  # Rough estimate
        print(f"   Estimated tokens: {int(estimated_tokens)}")
    
    # Save keyword mapping
    keywords_file = OUTPUT_PATH / "collection_keywords.json"
    with open(keywords_file, 'w', encoding='utf-8') as f:
        json.dump(build_collection_keywords(), f, indent=2)
    
    print(f"\n✅ Saved collection keywords: {keywords_file}")


if __name__ == "__main__":
    print("🔧 Building Collection Metadata for NLP-based Detection")
    print(f"Base directory: {BASE_DIR}")
    generate_metadata_files()
    print("\n✅ All metadata files generated!")
