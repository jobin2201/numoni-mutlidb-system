from field_filter import extract_field_names, extract_date_range

# Test 1: Field extraction
query1 = "get Customer ID, Sender Name on 16th of Feb"
fields = extract_field_names(query1)
print(f"Test 1 - Fields: {fields}")

# Test 2: Date extraction
dates = extract_date_range(query1)
print(f"Test 2 - Dates: {dates}")

# Test 3: Another pattern
query2 = "show me totalAmountPaid, transactionReferenceId on 16th of Feb"
fields2 = extract_field_names(query2)
print(f"Test 3 - Fields: {fields2}")

# Test 4: "with columns" pattern
query3 = "with columns: customerId, merchantName on 2025-02-16"
fields3 = extract_field_names(query3)
print(f"Test 4 - Fields: {fields3}")
dates3 = extract_date_range(query3)
print(f"Test 4 - Dates: {dates3}")
