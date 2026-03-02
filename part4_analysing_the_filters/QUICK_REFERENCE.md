# Quick Reference - Part 4 App Usage

## Accessing the App

**Streamlit URL**: http://localhost:8501 (or check terminal output)

**Command to Start**:
```bash
conda activate numoni_env
cd f:\WORK\adventure_dataset\numoni_final\part4_analysing_the_filters
streamlit run app_4part_pipeline.py
```

## What You'll See

### Part 1: Database Detection
- Shows which database (CUSTOMER/MERCHANT)
- Confidence percentage
- Score
- Reason for selection

### Part 2: Collection Detection
- Shows selected collection
- Confidence & score
- **Reason** explaining the match
- Matched fields and values
- Top 5 alternative collections

### Part 3: Action Detection
- Action type (LIST, COUNT, etc.)
- Aggregation/Limit info
- Number of filters detected
- Full action metadata (expandable)

### Part 4: Advanced Filters
- **Date filters**: Type, year, month, days ago
- **Location filters**: Location name, type
- **Numeric filters**: Field, operator, value
- **Smart suggestions**: When wrong collection detected

### Validation Messages
- ✅ Green: Success messages
- ⚠️ Yellow: Warnings & suggestions

### Results
- Record count
- Sample data (up to 50 records)
- Formatted dates (YYYY-MM-DD)
- Download CSV button

## Example Queries

### Date Queries (Smart Detection)
```
show me customers added last year
→ Will suggest customer_details if transaction_history selected

newly added merchants
→ Will suggest merchantDetails

transactions from last month
→ Correctly uses transaction_history
```

### Numeric Queries
```
customers who received more than 3000 numoni points
→ Selects customer_sharepoint_requests
→ Extracts points from smsText field
```

### Location Queries
```
merchants in Lagos
→ Searches city, state, businessAddress fields
```

### Combined Queries
```
Lagos merchants added last year
→ Applies both location AND date filters
```

## Understanding Validation Messages

### ✅ Success Messages
```
✓ Date filter applied using field: createdDt
```
→ Everything working correctly!

### ⚠️ Warning Messages
```
⚠️ Current collection 'transaction_history' has no date fields. 
Consider using 'customer_details' instead 
(Query about customers ADDED/CREATED - use customer_details with createdDt field)
```
→ Wrong collection selected, but Part 4 caught it and suggests the right one!

### 💡 Collection Suggestions
When Part 4 detects a date query mismatch:
```
💡 Suggestion: Query about customers ADDED/CREATED - 
use customer_details with createdDt field
```

## What Makes Part 4 Smart

### Context-Aware Collection Suggestions

| Query Pattern | Suggested Collection | Reason |
|---------------|---------------------|---------|
| "customers **added**" | `customer_details` | Has `createdDt` field |
| "customers **transactions**" | `transaction_history` | Has `transactionDate` field |
| "merchants **added**" | `merchantDetails` | Has `createdDt` field |
| "merchants **sales**" | `transaction_history` | Has transaction data |

### Date Field Priority
1. `createdDt` - When entity was created
2. `createdDate` - Alternative creation field
3. `transactionDate` - When transaction occurred
4. `date` - Generic date field

### Intelligent Fallback
- If collection has no date fields → Clear warning
- If wrong collection selected → Suggests correct one
- If ambiguous query → Shows what was detected

## Testing Individual Features

### Test Date Detection:
```bash
python test_date_detection.py
```

### Test Query:
Open Streamlit app and try:
1. "show me customers added last year"
2. Look for ⚠️ warning message
3. Note the suggested collection

## Token Efficiency ✅

All components stay under 2000 tokens:
- Part 1: Database router
- Part 2: Collection router (Customer: 1643, Merchant: 1926)
- Part 3: Action executor
- Part 4: Advanced filters (~1230 tokens)

## Troubleshooting

### No Date Fields Found
→ Check if you're querying the right database
→ Customer details in `customer_details`
→ Merchant details in `merchantDetails`

### Wrong Results
→ Look at validation messages
→ Check if Part 4 suggested a different collection
→ Try the suggested collection name

### Timezone Errors
→ Already fixed! All dates converted to naive datetime

## Files Structure

```
part4_analysing_the_filters/
├── advanced_filter_detector.py      # Detects date/location/numeric filters
├── advanced_filter_executor.py      # Applies filters + validation
├── complete_pipeline.py             # 4-part integration
├── app_4part_pipeline.py           # Streamlit UI (THIS IS THE MAIN APP)
├── test_date_detection.py          # Test script
├── PART4_ENHANCEMENTS.md           # Detailed documentation
└── QUICK_REFERENCE.md              # This file
```

## Next Steps

1. ✅ App is running at http://localhost:8501
2. ✅ All improvements implemented
3. ✅ All tests passing
4. ✅ Token counts under 2000

**Try these queries in the app:**
- "show me customers added last year"
- "customers who received more than 3000 numoni points"
- "transactions from last month"
- "merchants in Lagos"

Watch for the ⚠️ warnings and 💡 suggestions!
