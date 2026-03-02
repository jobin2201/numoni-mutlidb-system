# WHICH Handler Workflow (True vs False)

This document explains exactly what happens in `app_4part_pipeline.py` when a query is entered.

---

## Example query used

**Query:** `Which users have active sessions but no login activity record?`

---

## 6-step mini flow (high level)

```mermaid
flowchart TD
    A[1. User enters query] --> B[2. app_4part_pipeline calls handle_which_query(query, load_collection_data)]
    B --> C{3. which_result.handled ?}
    C -->|TRUE| D[4A. Use which_result database/collections/action/filters]
    D --> E[5A. Show WHICH result rows in table]
    E --> F[6A. st.stop() -> normal pipeline is skipped]

    C -->|FALSE| G[4B. Continue normal pipeline]
    G --> H[5B. Detect DB -> detect collection -> detect action -> detect advanced filters]
    H --> I[6B. execute_action + display result]
```

---

## What happens when `which_result.handled == TRUE`

This is the **WHICH path** (shortcut path).

1. App calls:
   - `which_result = handle_which_query(query, load_collection_data)`
2. If `which_result.get('handled')` is true:
   - PART 1 UI uses `which_result['database']`
   - PART 2 UI uses `which_result['collections']`
   - PART 3 UI uses `which_result['action']`
   - PART 4 UI uses `which_result['filters']`
   - Result table uses `which_result['rows']`
3. Then app calls `st.stop()`.

✅ Meaning: once true, **nothing below runs** (no normal DB detector, no normal collection detector).

---

## What happens when `which_result.handled == FALSE`

This is the **normal 4-part path**.

1. App does **not** stop.
2. It continues through special modes and normal pipeline checks:
   - data-retrieval mode / ranking mode / comparison mode / info-search mode (if matching)
3. If none of those early modes stop execution, then it runs the standard flow:
   - **PART 1:** `detect_database(...)`
   - **PART 2:** `detect_collection(...)` (with transaction priority rules in some cases)
   - **PART 3:** `detect_action(...)`
   - **PART 4:** `detect_advanced_filters(...)`
   - Execute with `execute_action(...)`
4. UI shows the result from `execute_action`.

✅ Meaning: false = app falls back to the normal routing logic.

---

## For your exact example query

Query: **Which users have active sessions but no login activity record?**

Typical WHICH-path output object shape:

- `handled`: `True`
- `database`: `authentication`
- `collections`: `['user_sessions', 'login_activities', 'authuser']`
- `action`: `list`
- `filters`: `{'present_in': 'user_sessions', 'absent_in': 'login_activities'}`
- `rows`: list of matched rows

So in UI it shows those values in PART 1/2/3/4 and stops there.

---

## Simple memory rule

- **True** -> "WHICH handler owns the query" -> show WHICH result -> stop.
- **False** -> "WHICH handler skipped" -> run normal app pipeline.
