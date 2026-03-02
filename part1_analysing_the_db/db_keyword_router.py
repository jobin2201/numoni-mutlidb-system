# db_keyword_router.py
# ----------------------------------------------------------
# Purpose:
# Detect whether user query belongs to:
#   - numoni_customer
#   - numoni_merchant
#   - both (relationship/comparison between customer & merchant)
# ----------------------------------------------------------


DB_KEYWORDS = {
    "numoni_customer": [
        # General customer terms
        "customer", "user", "buyer", "client", "account holder",
        "customer details", "customer profile", "customer info",
        "customer phone", "customer email", "customer name",

        # Customer collections (file names)
        "customerdetails", "customererror", "customerlocation",
        "wallet", "customer_wallet", "customer_wallet_ledger",
        "customer_wallet_ledger.json", "wallet_adjust_management",
        "transaction_session", "transaction_history",

        # Wallet / money / balance related
        "wallet balance", "balance", "amount", "amountonhold",
        "load money", "topup", "top up", "customer_load_money",
        "bonus", "fee",

        # Customer sharing money
        "share money", "send money", "receive money",
        "customer_share_money",

        # OTP / authentication
        "otp", "verification", "payment otp", "payment_otp_verification",

        # Orders / initiatives
        "initiative", "initiative_orders", "order", "order_seqno",

        # Deals / favourites
        "favourite deal", "favourite_deal", "sponsored_deals",

        # Notifications (customer side)
        "pay on us", "pay_on_us_notifications",

        # Points ledger
        "points", "ledger", "customer_points_ledger",

        # Tokens / authentication
        "tokens", "token"
    ],


    "numoni_merchant": [
        # General merchant terms
        "merchant", "shop", "store", "vendor", "seller", "business",
        "merchant details", "merchant profile", "merchant info",
        "merchant phone", "merchant email", "brand", "brandname",

        # Merchant collections (file names)
        "merchantdetails", "merchantlocation",
        "bankinformation", "pos", "terminal", "posid",
        "merchant_wallet_ledger", "merchant_reward_points_ledger",
        "merchant_payout", "merchant_payout_initiatives",
        "payout_retry_records", "payout_scheduler_entry",
        "payout_scheduler_process_entry",

        # Deals / rewards
        "deals", "dealimage", "deal_status",
        "reviews", "rating", "comment",
        "rewards", "reward", "rewardcap", "rewarddistributed",

        # Bank / payout related
        "bank", "bankname", "bankcode", "accountno",
        "account holder", "beneficiary",
        "payout", "settlement", "withdrawal",

        # File storage mapping
        "file_mapping", "businessimage", "imagepath",

        # Merchant notifications
        "notifications", "pay_out_notification",

        # Nigeria regions
        "nigeria_regions", "state", "lga", "region",

        # Merchant wallet / adjustment
        "wallet", "merchant wallet", "adjustmentpointandbalance",

        # Tokens / authentication
        "tokens", "token"
    ],


    "both": [
        # comparison / relationship terms
        "compare", "comparison", "difference", "diff",
        "merchant vs customer", "customer vs merchant",
        "relationship", "relation", "linked", "mapping",
        "foreign key", "join", "match",

        # transaction/payment flow between customer and merchant
        "payment", "paid", "received", "sent",
        "customer paid merchant", "merchant received from customer",
        "invoice", "invoice number", "invoice ref",
        "transaction", "transaction id", "transaction history",
        "transaction reference", "paymentreferencetoken",

        # important cross-db keys present in your collections
        "merchantid", "customerid", "userid",
        "merchantuserid", "customeruserid",

        # initiative_orders contains both merchantId and customerId
        "initiative_orders",

        # reviews contains merchantId and customerId
        "reviews"
    ]
}


def detect_database(user_query: str):
    """
    Returns:
        ["numoni_customer"]
        ["numoni_merchant"]
        ["numoni_customer", "numoni_merchant"]
    """
    query = user_query.lower()

    customer_score = sum(1 for kw in DB_KEYWORDS["numoni_customer"] if kw in query)
    merchant_score = sum(1 for kw in DB_KEYWORDS["numoni_merchant"] if kw in query)
    both_score = sum(1 for kw in DB_KEYWORDS["both"] if kw in query)

    # If query has relationship/comparison/payment flow -> use both
    if both_score > 0:
        return ["numoni_customer", "numoni_merchant"]

    # Decide based on score
    if customer_score > merchant_score:
        return ["numoni_customer"]

    if merchant_score > customer_score:
        return ["numoni_merchant"]

    # fallback if unclear
    return ["numoni_customer", "numoni_merchant"]


if __name__ == "__main__":
    while True:
        q = input("\nAsk something (type exit to stop): ")
        if q.lower() == "exit":
            break

        dbs = detect_database(q)
        print("➡️ Suggested DB(s):", dbs)
