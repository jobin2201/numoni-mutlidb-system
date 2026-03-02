#!/usr/bin/env python
"""
Test authentication collection detection
Verify that login queries select login_activities, not audit_trail
"""

from collection_router import detect_collection

print("=" * 80)
print("🔐 AUTHENTICATION COLLECTION DETECTION TEST")
print("=" * 80)

# Test queries that should map to specific authentication collections
test_cases = [
    {
        "query": "Show me details of login details of each user",
        "expected": "login_activities",
        "description": "Login activity history"
    },
    {
        "query": "Show me details of authentication for each user",
        "expected": "authuser",
        "description": "User authentication details"
    },
    {
        "query": "Show me login history",
        "expected": "login_activities",
        "description": "Login history"
    },
    {
        "query": "Get user login information",
        "expected": "login_activities",
        "description": "User login info"
    },
    {
        "query": "Show me user details",
        "expected": "authuser",
        "description": "User account details"
    },
    {
        "query": "Show me user profile information",
        "expected": "authuser",
        "description": "User profile"
    },
    {
        "query": "Show me active sessions",
        "expected": "user_sessions",
        "description": "User sessions"
    },
    {
        "query": "Show me audit trail",
        "expected": "audit_trail",
        "description": "System audit events"
    },
    {
        "query": "Show me system activity log",
        "expected": "audit_trail",
        "description": "Activity log"
    },
    {
        "query": "Show me OTP records",
        "expected": "otp",
        "description": "OTP verification"
    },
    {
        "query": "Show me account deletion requests",
        "expected": "account_deletion_request",
        "description": "Account deletion"
    },
    {
        "query": "Show me signin records",
        "expected": "signin_records",
        "description": "Sign-in events"
    },
]

passed = 0
failed = 0

print("\n🧪 Running Authentication Collection Tests:\n")

for test in test_cases:
    query = test["query"]
    expected = test["expected"]
    description = test["description"]
    
    # Try with authentication database name
    result = detect_collection(query, "authentication")
    
    selected = result['selected_collection']
    score = result.get('score', 0)
    confidence = result.get('confidence', 0)
    
    # Check if correct
    is_correct = selected == expected
    status = "✅ PASS" if is_correct else "❌ FAIL"
    
    if is_correct:
        passed += 1
    else:
        failed += 1
    
    print(f"{status} | {description}")
    print(f"     Query: '{query}'")
    print(f"     Expected: {expected}, Got: {selected}")
    print(f"     Score: {score:.1f} | Confidence: {confidence:.2f}")
    if result.get('matched_fields'):
        print(f"     Fields: {', '.join(result['matched_fields'][:3])}")
    print()

print("=" * 80)
print(f"📊 RESULTS: {passed} passed, {failed} failed out of {len(test_cases)} tests")
print("=" * 80)

if failed == 0:
    print("\n✅ All tests PASSED! Authentication collection routing is working correctly.")
else:
    print(f"\n⚠️  {failed} test(s) FAILED. Collection routing needs improvement.")
    print("\nFailed collections need better keyword differentiation:")
    print("  - login_activities vs audit_trail (both have 'activity')")
    print("  - authuser vs account_deletion_request (both user-related)")
    print("  - user_sessions vs user_sessions (session keywords)")

print("\n" + "=" * 80)
