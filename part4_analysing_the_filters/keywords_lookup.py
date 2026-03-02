from difflib import SequenceMatcher

KEYWORDS_DB = {
    'CUSTOMER_DB': {
        'brand_wallet': [],
        'customerDetails': ['name', 'email', 'userId', 'phoneNumber', 'qrCode', 'isEmailVerified'],
        'customerError': ['errorTypeEnum', 'errorMessage', 'retries', 'resolved', 'customerId'],
        'customerlocation': ['street', 'address', 'city', 'country', 'latitude', 'longitude', 'homeNumber'],
        'customer_load_money': ['transactionAmount', 'fee', 'bonusAmount', 'status', 'bankName', 'accountNumber'],
        'customer_points_ledger': [],
        'customer_sharepoint_requests': ['referCode', 'phoneNumber', 'referId'],
        'customer_share_money': ['transactionAmount', 'phoneNumber', 'sentUserId', 'receiveUserId'],
        'customer_wallet_ledger': ['transactionNo', 'senderName', 'senderBankName', 'fee', 'status', 'trn_amount'],
        'favourite_deal': ['dealId', 'category'],
        'initiative_orders': ['dealId', 'invoiceAmount', 'tipAmount', 'totalAmount', 'quantity'],
        'invoice': [],
        'merchant_payment_details': [],
        'order_seqNo': ['seqNo', 'prefix'],
        'payment_otp_verification': ['otp', 'phoneNumber', 'status', 'token'],
        'pay_on_us_notifications': ['transactionAmount', 'merchantFee', 'status', 'accountNumber'],
        'sponsored_deals': ['heading', 'description', 'dealId', 'isActive'],
        'tokens': ['tokenType', 'token', 'expiresAt'],
        'top_up_status': ['status', 'onusReferenceId'],
        'transaction_history': ['customerId', 'transactionReferenceId', 'transactionName', 'totalAmountPaid', 'senderName', 'receiverName', 'transactionDate', 'status'],
        'transaction_session': ['transactionId', 'sessionId', 'invoiceNo', 'type'],
        'wallet': ['amount', 'amountOnHold', 'balance'],
        'wallet_adjust_management': ['walletId', 'adjustAmount', 'reason', 'type']
    },
    'MERCHANT_DB': {
        'adjustmentpointandbalance': ['balance', 'reason', 'walletType'],
        'bankInformation': ['bankname', 'bankcode', 'accountNo', 'accountHolderName', 'primary'],
        'businessimage': ['image', 'userId'],
        'category': ['name', 'tip_amount'],
        'dealimage': ['dealId', 'imagePath'],
        'deals': ['userId', 'name', 'description', 'discount', 'newPrice', 'category', 'startDate', 'endDate'],
        'deal_status': ['dealId', 'status'],
        'file_mapping': ['originalFileName', 'contentType', 'fileSize'],
        'merchantDetails': ['email', 'businessName', 'brandName', 'category', 'description', 'phoneNumber'],
        'merchantlocation': ['userId', 'address', 'street', 'city', 'country', 'storeNo'],
        'merchant_payout': ['merchantId', 'payoutDate', 'status'],
        'merchant_payout_initiatives': ['merchantId', 'amount', 'beneficiaryAccountName', 'email'],
        'merchant_reward_points_ledger': [],
        'merchant_wallet_ledger': ['merchantId', 'invoiceNo', 'transactionNo', 'status', 'amount'],
        'nigeria_regions': ['region', 'state', 'lga'],
        'notifications': ['title', 'description', 'read', 'userId'],
        'payout_retry_records': ['merchantId', 'retryCount', 'transactionId'],
        'payout_scheduler_entry': ['merchantId', 'scheduledPayoutDate'],
        'payout_scheduler_process_entry': ['entryDate'],
        'pay_out_notification': ['transactionAmount', 'status', 'accountNumber'],
        'pos': ['posId', 'posName', 'merchantId', 'bankName', 'accountNo', 'location', 'status'],
        'reviews': ['merchantId', 'customerId', 'rating', 'comment'],
        'rewards': ['merchantId', 'rewardType', 'rewardCap', 'status', 'startDate'],
        'scheduler_locks': [],
        'shedLock': [],
        'tokens': ['tokenType', 'token'],
        'transaction_history': ['merchantId', 'transactionType', 'amount', 'status', 'merchantName', 'customerName', 'transactionDate'],
        'wallet': ['amount', 'balance']
    }
}

def fuzzy_match_keyword(keyword, candidates, threshold=0.6):
    """Find best matching keyword from candidates"""
    keyword_lower = keyword.lower()
    best_match = None
    best_score = 0
    
    for candidate in candidates:
        candidate_lower = candidate.lower()
        score = SequenceMatcher(None, keyword_lower, candidate_lower).ratio()
        
        if keyword_lower in candidate_lower or candidate_lower in keyword_lower:
            score = max(score, 0.8)
        
        if score > best_score:
            best_score = score
            best_match = candidate
    
    return best_match if best_score >= threshold else None

def get_matching_tables(query_keywords, db_type="BOTH"):
    """Find tables with matching fields"""
    matches = {'CUSTOMER_DB': {}, 'MERCHANT_DB': {}}
    
    for db in ['CUSTOMER_DB', 'MERCHANT_DB']:
        if db_type != "BOTH" and db_type not in db:
            continue
            
        for table, fields in KEYWORDS_DB[db].items():
            if not fields:
                continue
            
            matched_fields = 0
            for keyword in query_keywords:
                for field in fields:
                    if fuzzy_match_keyword(keyword, [field], 0.6):
                        matched_fields += 1
                        break
            
            if matched_fields > 0:
                matches[db][table] = {'matched': matched_fields, 'total_fields': len(fields)}
    
    return matches

def get_table_fields(table_name, db_name):
    """Get all fields for a specific table"""
    if db_name in KEYWORDS_DB and table_name in KEYWORDS_DB[db_name]:
        return KEYWORDS_DB[db_name][table_name]
    return []
