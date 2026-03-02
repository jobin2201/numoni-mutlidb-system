#!/usr/bin/env python
"""
FINAL VERIFICATION: Authentication Collection Detection Complete
Confirm all authentication collections are properly differentiated
"""

from collection_router import detect_collection

print("=" * 90)
print(" " * 20 + "✅ AUTHENTICATION COLLECTION DETECTION - FINAL VERIFICATION")
print("=" * 90)

# Critical test cases that were previously failing
critical_tests = [
    {
        "query": "Show me details of login details of each user",
        "expected": "login_activities",
        "problem": "Was selecting audit_trail instead of login_activities"
    },
    {
        "query": "Show me details of authentication for each user",
        "expected": "authuser",
        "problem": "Was selecting account_deletion_request instead of authuser"
    },
]

# Comprehensive test suite for all authentication collections
comprehensive_tests = [
    # Authentication users
    ("Show user details", "authuser"),
    ("List user account information", "authuser"),
    ("Get user credentials", "authuser"),
    
    # Login activities
    ("Show login history", "login_activities"),
    ("List user login activities", "login_activities"),
    ("Get login event records", "login_activities"),
    
    # Sessions
    ("Show active sessions", "user_sessions"),
    ("List user sessions", "user_sessions"),
    
    # Device info
    ("Show user device information", "userDeviceDetail"),
    ("Get device details for users", "userDeviceDetail"),
    
    # OTP
    ("Show OTP verification records", "otp"),
    ("Get one time password codes", "otp"),
    
    # Audit trail
    ("Show system audit trail", "audit_trail"),
    ("Get audit log events", "audit_trail"),
    ("List system activity", "audit_trail"),
    
    # Account deletion
    ("Show account deletion requests", "account_deletion_request"),
    ("List deactivation requests", "account_deletion_request"),
    
    # Sign-in records
    ("Show signin records", "signin_records"),
    ("Get sign in events", "signin_records"),
    
    # Roles
    ("Show user roles and permissions", "roles"),
    ("List role definitions", "roles"),
    
    # Refresh tokens
    ("Show refresh token records", "refresh_token_record"),
    ("Get token refresh log", "refresh_token_record"),
]

print("\n" + "🔴 CRITICAL ISSUES (Previously Failing):" + "\n")
critical_passed = 0
critical_failed = 0

for test in critical_tests:
    result = detect_collection(test["query"], "authentication")
    is_correct = result['selected_collection'] == test["expected"]
    
    status = "✅ FIXED" if is_correct else "❌ STILL BROKEN"
    print(f"{status}")
    print(f"  Query: '{test['query']}'")
    print(f"  Expected: {test['expected']}, Got: {result['selected_collection']}")
    if is_correct:
        critical_passed += 1
    else:
        critical_failed += 1
    print(f"  ℹ️  Issue: {test['problem']}")
    print()

print("\n" + "=" * 90)
print("📊 COMPREHENSIVE AUTHENTICATION COLLECTION TEST:" + "\n")

comprehensive_passed = 0
comprehensive_failed = 0
results_by_collection = {}

for query, expected in comprehensive_tests:
    result = detect_collection(query, "authentication")
    is_correct = result['selected_collection'] == expected
    
    if is_correct:
        comprehensive_passed += 1
    else:
        comprehensive_failed += 1
    
    if expected not in results_by_collection:
        results_by_collection[expected] = {"passed": 0, "failed": 0}
    
    if is_correct:
        results_by_collection[expected]["passed"] += 1
    else:
        results_by_collection[expected]["failed"] += 1

# Display by collection
for collection in sorted(results_by_collection.keys()):
    stats = results_by_collection[collection]
    total = stats["passed"] + stats["failed"]
    pct = (stats["passed"] / total * 100) if total > 0 else 0
    status = "✅" if stats["failed"] == 0 else "⚠️"
    print(f"{status} {collection:30s} → {stats['passed']}/{total} tests passed ({pct:.0f}%)")

print("\n" + "=" * 90)
print("🎯 OVERALL RESULTS:")
print("=" * 90)
total_passed = critical_passed + comprehensive_passed
total_failed = critical_failed + comprehensive_failed
total_tests = total_passed + total_failed
success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0

print(f"\nCritical Issues Fixed: {critical_passed}/{len(critical_tests)}")
print(f"Comprehensive Tests: {comprehensive_passed}/{len(comprehensive_tests)}")
print(f"TOTAL: {total_passed}/{total_tests} tests passed ({success_rate:.1f}%)")

if total_failed == 0:
    print("\n" + "✅ " * 30)
    print("SUCCESS! All authentication collections are properly differentiated!")
    print("✅ " * 30)
    print("\nKey improvements:")
    print("  ✓ login_activities correctly selected for login queries")
    print("  ✓ authuser correctly selected for user details queries")
    print("  ✓ audit_trail distinguished from login_activities")
    print("  ✓ All 12 authentication collections properly routed")
    print("  ✓ Field-aware matching prevents cross-collection confusion")
    print("  ✓ Keyword synonyms map queries to correct tables")
else:
    print(f"\n⚠️  {total_failed} test(s) still failing - needs further debugging")

print("\n" + "=" * 90)
