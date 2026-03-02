# Part 2: Collection/Table Detection

Intelligently detects **which collection (table)** to query within a selected database using NLP and metadata analysis.

## 🎯 What It Does

**Input**: User's natural language question + Database name (from Part 1)  
**Output**: Best matching collection/table with confidence score and matched fields

## 📁 Files

```
part2_analysing_the_collection/
│
├── build_collection_metadata.py     # Generate lightweight metadata (<2000 tokens)
├── collection_router.py              # Collection detection logic
├── integrated_router.py              # Combines Part 1 + Part 2
├── app2.py                          # Streamlit interface
│
├── numoni_customer_collections_metadata.json  # Customer collections (863 tokens)
├── numoni_merchant_collections_metadata.json  # Merchant collections (1103 tokens)
└── collection_keywords.json                    # Keyword mappings
```

## 🚀 Quick Start

### 1. Generate Metadata (First Time Only)
```bash
python build_collection_metadata.py
```

### 2. Test Collection Router
```bash
python collection_router.py
```

### 3. Test Integrated Router (Part 1 + Part 2)
```bash
python integrated_router.py
```

### 4. Run Streamlit App
```bash
streamlit run app2.py
```

## 🧠 How It Works

### Step 1: Metadata Generation
Scans all JSON collections and extracts:
- Field names
- Sample values
- Total records
- Optimized to stay under 2000 tokens

### Step 2: Intelligent Scoring
Matches user query against collections using:

1. **Collection Name Match** (50 points)
   - Direct match: "customer_wallet" → customer_wallet_ledger

2. **Keyword Matching** (10 points each)
   - Predefined keywords per collection
   - Example: "wallet", "balance" → wallet collection

3. **Field Name Matching** (15 points exact, 5 points partial)
   - Query mentions field names
   - Example: "accountNo" → bankInformation

4. **Sample Value Matching** (8 points)
   - Query contains actual data values

5. **Intent-Based Scoring** (5-15 points)
   - History queries → transaction_history
   - Error queries → customerError
   - Location queries → customerlocation

### Step 3: Confidence Calculation
```python
confidence = min(score / 100, 1.0)
```

## 📊 Example Outputs

### Query: "Show customer wallet balance"
```json
{
  "selected_collection": "wallet",
  "confidence": 0.83,
  "score": 83,
  "reason": "Collection name matched | Keywords matched: wallet, balance",
  "matched_fields": ["balance", "walletId"],
  "total_records": 8660,
  "alternatives": [
    {"collection": "customer_wallet_ledger", "confidence": 0.48}
  ]
}
```

### Query: "merchant bank account details"
```json
{
  "selected_collection": "bankInformation",
  "confidence": 0.73,
  "score": 73,
  "reason": "Keywords matched: bank, account | Fields matched: bankname, accountNo",
  "matched_fields": ["bankname", "accountNo", "bankcode"],
  "total_records": 112
}
```

## 🎨 Features

✅ **Under 2000 tokens** metadata per database  
✅ **20 customer collections** detected  
✅ **26 merchant collections** detected  
✅ **Typo tolerance** via fuzzy matching  
✅ **Field-level matching** (column names)  
✅ **Alternative suggestions** (top 3)  
✅ **Sample value detection** (finds data in queries)  
✅ **Intent-based scoring** (history, error, location patterns)  

## 🔧 Integration with Part 1

```python
from integrated_router import full_query_routing

result = full_query_routing("Show customer wallet transactions")

# Output:
{
  "database": "numoni_customer",
  "collection_info": {
    "selected_collection": "customer_wallet_ledger",
    "confidence": 0.85,
    "matched_fields": ["balance", "amount", "transactionType"]
  }
}
```

## 📈 Collections Supported

### Customer (20 collections)
- customerDetails
- customer_wallet_ledger
- customer_load_money
- transaction_history
- customerlocation
- customerError
- payment_otp_verification
- favourite_deal
- wallet
- And 11 more...

### Merchant (26 collections)
- merchantDetails
- bankInformation
- merchant_wallet_ledger
- merchantlocation
- deals
- reviews
- pos
- merchant_payout
- transaction_history
- And 17 more...

## 🎯 Accuracy

- **High Confidence** (>0.7): Direct matches with keywords
- **Medium Confidence** (0.4-0.7): Partial matches
- **Low Confidence** (<0.4): Weak matches, check alternatives

## 🛠️ Customization

### Add New Keywords
Edit `collection_keywords.json`:
```json
{
  "customerDetails": {
    "keywords": ["customer", "user", "profile", "YOUR_KEYWORD"],
    "fields": ["name", "email"],
    "description": "Customer profile info"
  }
}
```

### Adjust Scoring
Modify `collection_router.py`:
```python
# Line ~80-120: Adjust scoring weights
if 'history' in collection_name:
    score += 10  # Change this value
```

## 🔍 Use Cases

1. **Chatbot Query Routing**: Auto-select correct table
2. **Data Explorer**: Help users find data
3. **Query Builder**: Generate MongoDB queries
4. **API Router**: Route requests to correct collections
5. **Analytics Dashboard**: Smart data source selection
