# Field Filter Integration Guide

## Quick Usage

```python
from field_filter import apply_field_filters

# After getting results from ranking/search
query = "get Customer ID, Sender Name on 16th of Feb"
result = apply_field_filters(data, query)

print(result['filtered'])  # Filtered data with selected columns only
print(result['total_records'])  # Count after filtering
```

## Supported Patterns

### Field Selection
- `get Customer ID, Sender Name`
- `show me customerId, merchantName`
- `display totalAmountPaid, transactionReferenceId`
- `with columns: field1, field2`
- `these fields: field1, field2`

### Date Filtering
- `on 16th of Feb`
- `on February 16`
- `on 2025-02-16`
- `for 2025-02-16`

## Combined Example

```python
# Single query with both field and date filtering
query = "get Customer ID, Sender Name, Total Amount on 16th of Feb"
result = apply_field_filters(data, query)

# Returns:
# {
#   'filtered': [{'customerId': '123', 'senderName': 'John', 'totalAmountPaid': 5000}, ...],
#   'fields_selected': True,
#   'selected_fields': ['Customer ID', 'Sender Name', 'Total Amount'],
#   'date_filtered': True,
#   'total_records': 15
# }
```

## Features
- 4 field extraction patterns
- Fuzzy field name matching (handles "Customer ID" → "customerId")
- 3 date parsing formats
- Auto-detects date field (transactionDate, createdDate, createdDt)
- Handles MongoDB date format: ISO strings and {"$date": "..."} format
- Returns metadata about filtering applied
