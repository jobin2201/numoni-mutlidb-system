#!/usr/bin/env python
"""
Streamlit App - Complete 4-Part Pipeline
Shows: Database Detection → Collection Detection → Action Execution → Advanced Filters
"""
import streamlit as st
import sys
from pathlib import Path
import pandas as pd
import json
import re

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action"))
sys.path.insert(0, str(BASE_DIR / "part4_analysing_the_filters"))

from db_keyword_router_fuzzy import detect_database
from collection_router import detect_collection, load_metadata
from action_detector import detect_action
from action_executor import execute_action, load_collection_data
from advanced_filter_detector import detect_advanced_filters
from ranking_handler import parse_ranking_query, execute_ranking, format_ranking_results, similarity, match_entity_to_collection
from keywords_lookup import get_matching_tables, get_table_fields, KEYWORDS_DB
from table_linker import enrich_requested_fields_with_links
from which_query_handler import handle_which_query
from compare_keywords_handler import handle_compare_keyword_query
from optional_json_linker import render_optional_json_linker_section
import os
from datetime import datetime
from groq import Groq
from difflib import SequenceMatcher

JSON_LINKER_PATH = str(BASE_DIR / "part4_analysing_the_filters" / "summarised" / "collection_usage_linker.json")


@st.cache_data(show_spinner=False)
def load_unique_filter_tokens_from_databases() -> set:
    """Load all distinct column names from authentication, numoni_merchant, numoni_customer."""
    db_root = BASE_DIR / "databases"
    target_dbs = ["authentication", "numoni_merchant", "numoni_customer"]
    tokens = set()

    def add_key_variants(key: str):
        key_lower = key.lower().strip()
        if not key_lower:
            return
        tokens.add(key_lower)
        tokens.add(key_lower.replace("_", ""))
        tokens.add(key_lower.replace("_", " "))

    for db_name in target_dbs:
        db_path = db_root / db_name
        if not db_path.exists():
            continue

        for json_file in db_path.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as file:
                    content = json.load(file)
            except Exception:
                continue

            if isinstance(content, list):
                for record in content:
                    if isinstance(record, dict):
                        for key in record.keys():
                            if isinstance(key, str):
                                add_key_variants(key)
            elif isinstance(content, dict):
                for key in content.keys():
                    if isinstance(key, str):
                        add_key_variants(key)

    return tokens

st.set_page_config(page_title="Numoni Query System - 4-Part Pipeline", page_icon="🎯", layout="wide")

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .part-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        font-size: 1.3rem;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">🎯 Numoni Query System</div>', unsafe_allow_html=True)
st.markdown("**4-Part Intelligent Pipeline:** Database → Collection → Action → Advanced Filters")

# Sidebar with examples
with st.sidebar:
    st.header("💡 Example Queries")
    
    st.subheader("Date Filters")
    st.code("show me customers added last year", language=None)
    st.code("newly added merchants", language=None)
    st.code("transactions from this month", language=None)
    st.code("user logins from last week", language=None)
    
    st.subheader("Numeric Filters")
    st.code("customers who received more than 1000 numoni points", language=None)
    st.code("transactions above 5000", language=None)
    
    st.subheader("Location Filters")
    st.code("merchants in Lagos", language=None)
    st.code("customers from Nigeria", language=None)
    
    st.subheader("Authentication")
    st.code("show login activities from last month", language=None)
    st.code("list audit trail events", language=None)

# Main query input
st.markdown("---")
query = st.text_input(
    "🔍 Enter your query:", 
    placeholder="e.g., Show me customers added last year"
)

if st.button("🚀 Execute Query", type="primary") or query:
    
    if not query:
        st.warning("Please enter a query")
    else:
        query_lower = query.lower()
        from field_filter import extract_field_names, extract_date_range, normalize_field_name
        requested_fields_hint = extract_field_names(query)

        # Generic WHICH-query handler (isolated path; does not affect non-which queries)
        which_result = handle_which_query(query, load_collection_data)
        if which_result.get('handled'):
            st.markdown('<div class="part-header">📊 PART 1: Database Detection</div>', unsafe_allow_html=True)
            st.success(f"**Selected Database:** {(which_result.get('database') or 'N/A').replace('numoni_', '').upper()}")

            st.markdown('<div class="part-header">🗂️ PART 2: Collection Detection</div>', unsafe_allow_html=True)
            collections_used = which_result.get('collections', [])
            st.success(f"**Selected Collection(s):** {', '.join(collections_used) if collections_used else 'N/A'}")

            st.markdown('<div class="part-header">⚡ PART 3: Action Detection</div>', unsafe_allow_html=True)
            st.info(f"**Action:** {(which_result.get('action') or 'list').upper()}")

            st.markdown('<div class="part-header">🔍 PART 4: Filters</div>', unsafe_allow_html=True)
            filters_used = which_result.get('filters', {})
            if filters_used:
                st.json(filters_used, expanded=False)
            else:
                st.info("No filters detected")

            st.markdown('<div class="part-header">🎯 WHICH Query Result</div>', unsafe_allow_html=True)
            rows = which_result.get('rows', [])
            if rows:
                if which_result.get('single'):
                    st.success("✅ Returning the single requested result")
                else:
                    st.success(f"✅ Found {len(rows):,} {which_result.get('target', 'results')}")
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            else:
                st.info("No matching results found for this WHICH query")
            st.stop()

        # TARGETED INTENT HANDLERS (only for specific business questions)
        is_customers_with_payout_merchants = (
            ('customer' in query_lower or 'customers' in query_lower)
            and 'interact' in query_lower
            and 'merchant' in query_lower
            and 'payout' in query_lower
        )
        is_highest_wallet_activity_merchants = (
            ('merchant' in query_lower or 'merchants' in query_lower)
            and 'wallet ledger' in query_lower
            and ('activity' in query_lower or 'highest' in query_lower)
            and ('customer' in query_lower or 'customers' in query_lower)
        )

        if is_customers_with_payout_merchants:
            st.markdown('<div class="part-header">🎯 Intent-Specific Result</div>', unsafe_allow_html=True)
            with st.spinner("Finding customers who interacted with payout merchants..."):
                payout_data = load_collection_data('numoni_merchant', 'merchant_payout')
                if not payout_data:
                    payout_data = load_collection_data('numoni_merchant', 'merchant_payout_initiatives')

                merchant_ids = {
                    str(r.get('merchantId')).strip()
                    for r in payout_data
                    if r.get('merchantId') is not None and str(r.get('merchantId')).strip()
                }

                tx_data = load_collection_data('numoni_merchant', 'transaction_history')
                customer_map = {}

                for row in tx_data:
                    merchant_id = str(row.get('merchantId', '')).strip()
                    if merchant_id not in merchant_ids:
                        continue

                    customer_id = str(row.get('customerId', '')).strip()
                    customer_name = str(row.get('customerName', '')).strip()

                    if not customer_id and not customer_name:
                        continue

                    key = customer_id or customer_name
                    if key not in customer_map:
                        customer_map[key] = {
                            'Customer ID': customer_id or 'N/A',
                            'Customer Name': customer_name or 'N/A',
                            'Interactions': 0,
                            'Unique Merchants': set()
                        }

                    customer_map[key]['Interactions'] += 1
                    customer_map[key]['Unique Merchants'].add(merchant_id)

                output_rows = []
                for rec in customer_map.values():
                    output_rows.append({
                        'Customer ID': rec['Customer ID'],
                        'Customer Name': rec['Customer Name'],
                        'Interactions': rec['Interactions'],
                        'Payout Merchants Interacted': len(rec['Unique Merchants'])
                    })

                output_rows = sorted(output_rows, key=lambda x: x['Interactions'], reverse=True)

                if output_rows:
                    st.success(f"✅ Found {len(output_rows):,} customers who interacted with merchants that received payouts")
                    st.dataframe(pd.DataFrame(output_rows), use_container_width=True, hide_index=True)
                else:
                    st.info("No matching customers found")
            st.stop()

        if is_highest_wallet_activity_merchants:
            st.markdown('<div class="part-header">🎯 Intent-Specific Result</div>', unsafe_allow_html=True)
            with st.spinner("Ranking merchants by wallet ledger activity from customers..."):
                tx_data = load_collection_data('numoni_merchant', 'transaction_history')

                merchant_stats = {}
                for row in tx_data:
                    source_table = str(row.get('sourceTable', '')).lower()
                    if 'wallet_ledger' not in source_table and 'wallet ledger' not in source_table:
                        continue

                    merchant_id = str(row.get('merchantId', '')).strip()
                    merchant_name = str(row.get('merchantName', '')).strip()
                    customer_id = str(row.get('customerId', '')).strip()

                    if not merchant_id and not merchant_name:
                        continue
                    if not customer_id:
                        continue

                    key = merchant_id or merchant_name
                    if key not in merchant_stats:
                        merchant_stats[key] = {
                            'Merchant ID': merchant_id or 'N/A',
                            'Merchant Name': merchant_name or 'N/A',
                            'Wallet Ledger Activity Count': 0,
                            'Unique Customers': set()
                        }

                    merchant_stats[key]['Wallet Ledger Activity Count'] += 1
                    merchant_stats[key]['Unique Customers'].add(customer_id)

                output_rows = []
                for rec in merchant_stats.values():
                    output_rows.append({
                        'Merchant ID': rec['Merchant ID'],
                        'Merchant Name': rec['Merchant Name'],
                        'Wallet Ledger Activity Count': rec['Wallet Ledger Activity Count'],
                        'Unique Customers': len(rec['Unique Customers'])
                    })

                output_rows = sorted(output_rows, key=lambda x: x['Wallet Ledger Activity Count'], reverse=True)

                if output_rows:
                    st.success(f"✅ Ranked {len(output_rows):,} merchants by wallet ledger activity from customers")
                    st.dataframe(pd.DataFrame(output_rows), use_container_width=True, hide_index=True)
                else:
                    st.info("No matching merchant wallet ledger activity found")
            st.stop()

        has_field_intent = any(phrase in query_lower for phrase in [
            'i want', 'only want', 'show me only', 'display only', 'just the',
            'i need', 'need these', 'these fields', 'these columns', 'following fields', 'following columns',
            'field', 'column', 'include', 'including'
        ])
        
        # CHECK 0a: Is this a DATA RETRIEVAL query with specific fields requested?
        # Only trigger for queries that explicitly ask for field selection, NOT generic "show me"
        is_data_query = (bool(requested_fields_hint) and has_field_intent) or (
            any(phrase in query_lower for phrase in [
            'get me all the data', 'only want', 'i want these', 'show me only', 'display only',
            'just the', 'these fields', 'these columns', 'following fields', 'following columns', 'include', 'including'
            ]) and (any(phrase in query_lower for phrase in ['field', 'column']) or ',' in query_lower)
        )
        
        if is_data_query:
            st.markdown('<div class="part-header">📋 Data Retrieval Mode</div>', unsafe_allow_html=True)
            
            with st.spinner("Processing data retrieval query..."):
                # Extract requested fields (may be None)
                requested_fields = requested_fields_hint
                requested_date = extract_date_range(query)
                
                # Info about fields
                if requested_fields:
                    st.info(f"**Requested Fields:** {', '.join(requested_fields)}")
                else:
                    st.info("**Mode:** Showing all available fields (no specific fields requested)")
                
                if requested_date:
                    if requested_date[0].date() == requested_date[1].date():
                        st.info(f"**Date Filter:** {requested_date[0].strftime('%B %d, %Y')}")
                    else:
                        st.info(
                            f"**Date Filter:** {requested_date[0].strftime('%B %d, %Y')} to {requested_date[1].strftime('%B %d, %Y')}"
                        )
                
                # Detect database using PART 1
                db_result = detect_database(query)
                db_name = (db_result.get('selected_dbs') or [db_result.get('selected_database')])[0]
                has_login_signal = any(token in query_lower for token in ['login', 'logged in', 'signin', 'sign in'])
                has_customer_wallet_activity_intent = (
                    ('customer' in query_lower)
                    and ('wallet' in query_lower)
                    and any(token in query_lower for token in ['activity', 'ledger', 'balance'])
                )
                has_payout_keyword = 'payout' in query_lower
                has_payout_vs_wallet_intent = has_payout_keyword and ('wallet balance' in query_lower)
                has_payout_report_intent = has_payout_keyword and any(
                    token in query_lower for token in ['merchant payout report', 'payout report', 'payout details', 'payout amount']
                )
                has_payout_intent = has_payout_report_intent or has_payout_vs_wallet_intent
                has_login_linked_transaction_query = has_login_signal and any(
                    token in query_lower for token in ['transaction', 'sales', 'order', 'purchase']
                )

                if has_customer_wallet_activity_intent:
                    db_name = 'numoni_customer'
                elif has_payout_intent:
                    db_name = 'numoni_merchant'
                elif has_login_linked_transaction_query:
                    db_name = 'numoni_merchant'
                
                # Detect collection using PART 2 - proper NLP matching
                from collection_router import detect_collection
                query_for_collection = query
                marker_positions = []
                query_lower_for_split = query.lower()
                for marker in [' i only want ', ' i want ', ' i need ', ' i need these ', ' i need these following fields', ' i need following fields', ' show me only ', ' display only ', ' just the ', ' include ', ' including ', ' fields:']:
                    pos = query_lower_for_split.find(marker)
                    if pos != -1:
                        marker_positions.append(pos)

                if marker_positions:
                    split_pos = min(marker_positions)
                    intent_part = query[:split_pos].strip(' .,-')
                    if intent_part:
                        query_for_collection = intent_part
                
                # SPECIAL PRIORITY RULE: If query has "transaction" + grouping keywords, force transaction_history
                query_lower = query.lower()
                grouping_keywords = ['group by', 'grouped by', 'group', 'grouped', 'categorize', 'category', 'breakdown']
                has_grouping = any(kw in query_lower for kw in grouping_keywords)
                has_transaction = 'transaction' in query_lower
                requested_fields_text = ' '.join((requested_fields or [])).lower()
                transaction_metric_tokens = [
                    'order amount', 'sales amount', 'total sales', 'transaction reference',
                    'reference id', 'transaction date', 'transaction count', 'transaction amount',
                    'customer name', 'customer id', 'merchant name', 'merchant id',
                    'pos id', 'pos name', 'total transaction count',
                    'customer details', 'merchant details', 'pos details',
                    'commission deducted', 'payout status'
                ]
                has_transaction_metric_field = any(token in requested_fields_text for token in transaction_metric_tokens)
                has_transactional_query = any(kw in query_lower for kw in ['transaction', 'sales', 'purchase', 'order'])
                has_report_intent = 'report' in query_lower
                has_sales_report_intent = (
                    ('sales report' in query_lower or 'order amount' in query_lower)
                    and any(k in query_lower for k in ['customer name', 'merchant name', 'merchant id', 'customer id'])
                )
                has_purchased_points_intent = any(k in query_lower for k in ['purchased points', 'purchase points', 'bought points'])
                collection_name = None
                
                if has_customer_wallet_activity_intent:
                    collection_name = 'customer_wallet_ledger'
                elif has_payout_intent:
                    collection_name = 'merchant_payout_initiatives'
                elif has_login_linked_transaction_query:
                    collection_name = 'transaction_history'
                elif has_sales_report_intent:
                    collection_name = 'transaction_history'
                elif has_purchased_points_intent:
                    collection_name = 'customer_wallet_ledger'
                elif has_transaction_metric_field and (has_transactional_query or has_report_intent):
                    collection_name = 'transaction_history'
                elif has_transaction and has_grouping:
                    # Priority boost: use transaction_history for transaction + grouping queries
                    collection_name = 'transaction_history'
                else:
                    collection_result = detect_collection(query_for_collection, db_name)
                    collection_name = collection_result.get('selected_collection')
                
                if not collection_name:
                    st.error(f"Could not determine collection from query")
                    st.stop()
                
                st.success(f"**Database:** {db_name.replace('numoni_', '').upper()} | **Collection:** {collection_name}")
                from advanced_filter_executor import (
                    build_merchant_payout_dataset,
                    apply_date_range_filter,
                    apply_login_link_filter,
                    apply_payout_wallet_comparison,
                    apply_top_bottom_n_filter,
                )
                
                # Load data
                data = load_collection_data(db_name, collection_name)
                if has_payout_intent:
                    data = build_merchant_payout_dataset(load_collection_data)
                    collection_name = 'merchant_payout* (merged)'

                if not data:
                    st.error(f"No data found in {collection_name}")
                    st.stop()
                
                st.success(f"✅ Loaded {len(data):,} records")

                def _parse_record_date(value):
                    try:
                        if value is None:
                            return None
                        if isinstance(value, (int, float)):
                            unit = 'ms' if value > 10**11 else 's'
                            dt = pd.to_datetime(value, unit=unit, errors='coerce')
                            return None if pd.isna(dt) else dt
                        if isinstance(value, str):
                            dt = pd.to_datetime(value, errors='coerce')
                            return None if pd.isna(dt) else dt
                        if isinstance(value, dict):
                            if '$date' in value:
                                date_val = value.get('$date')
                                if isinstance(date_val, dict):
                                    if '$numberLong' in date_val:
                                        epoch_ms = int(date_val.get('$numberLong'))
                                        dt = pd.to_datetime(epoch_ms, unit='ms', errors='coerce')
                                        return None if pd.isna(dt) else dt
                                elif isinstance(date_val, (int, float)):
                                    unit = 'ms' if date_val > 10**11 else 's'
                                    dt = pd.to_datetime(date_val, unit=unit, errors='coerce')
                                    return None if pd.isna(dt) else dt
                                else:
                                    dt = pd.to_datetime(date_val, errors='coerce')
                                    return None if pd.isna(dt) else dt
                            if '$numberLong' in value:
                                epoch_ms = int(value.get('$numberLong'))
                                dt = pd.to_datetime(epoch_ms, unit='ms', errors='coerce')
                                return None if pd.isna(dt) else dt
                    except Exception:
                        return None
                    return None
                
                # Apply date filter if specified
                if requested_date:
                    start_date, end_date = requested_date
                    data = apply_date_range_filter(data, start_date, end_date)
                    st.success(f"📅 After date filter: {len(data):,} records")

                if has_login_linked_transaction_query and data:
                    data = apply_login_link_filter(data, requested_date, load_collection_data)
                    st.success(f"🔐 After login-activity link filter: {len(data):,} records")

                if has_payout_vs_wallet_intent and data:
                    data = apply_payout_wallet_comparison(data, query, requested_date, load_collection_data)
                    st.success(f"💰 After payout vs wallet filter: {len(data):,} records")
                
                if not data:
                    st.warning("No records match the date filter")
                    st.stop()

                if requested_date and requested_fields and data:
                    date_columns = [
                        'transactionDate', 'createdDate', 'createdDt', 'updatedDt', 'updatedAt',
                        'activityTime', 'date', 'payoutDate', 'createdAt', 'entryDate'
                    ]
                    for col in date_columns:
                        if col in data[0] and col not in requested_fields:
                            requested_fields.append(col)
                
                # ===== CASE 1: Fields explicitly requested =====
                if requested_fields:
                    # Match requested field names to actual field names using smart mapping
                    field_mapping = {}
                    if data and isinstance(data[0], dict):
                        for req_field in requested_fields:
                            matched = normalize_field_name(req_field, data[0])
                            if matched:
                                field_mapping[req_field] = matched

                    # Enrich missing requested fields using same-DB collection linking
                    is_sales_report_fast_query = (
                        'sales report' in query_lower and
                        bool(requested_fields) and
                        'from' in query_lower and
                        'to' in query_lower
                    )
                    display_data, field_mapping, unresolved, join_notes = enrich_requested_fields_with_links(
                        database_name=db_name,
                        base_collection=collection_name,
                        base_data=data,
                        requested_fields=requested_fields,
                        direct_field_mapping=field_mapping,
                        query_text=query,
                        fast_mode=is_sales_report_fast_query,
                    )

                    if unresolved:
                        st.warning(f"⚠️ Could not find fields: {', '.join(unresolved)}")

                    if not field_mapping:
                        st.error("❌ None of the requested fields were found in the database or related collections.")
                        st.info(f"Available fields in {collection_name}: {', '.join(list(data[0].keys())[:15])}")
                        st.stop()

                    if join_notes:
                        st.info("🔗 Auto-linked fields:\n\n" + "\n".join(f"- {note}" for note in join_notes))

                    # Clean MongoDB/date formatting
                    for row in display_data:
                        for key, value in row.items():
                            if isinstance(value, dict) and '$date' in value:
                                row[key] = pd.to_datetime(value['$date']).strftime('%Y-%m-%d')
                            elif isinstance(value, str) and 'T' in value and ('Z' in value or '+' in value):
                                try:
                                    row[key] = pd.to_datetime(value).strftime('%Y-%m-%d')
                                except Exception:
                                    row[key] = value

                    # Value-level status filter (supports "failed transactions", "only pending", "status successful")
                    status_value = None
                    status_match = re.search(
                        r'\bonly\s+(pending|successful|success|failed|failure|completed|active|inactive)\b',
                        query_lower,
                    )
                    if not status_match:
                        status_match = re.search(
                            r'\bstatus\b[^,.;\n]*\b(pending|successful|success|failed|failure|completed|active|inactive)\b',
                            query_lower,
                        )
                    if not status_match:
                        status_match = re.search(
                            r'\b(pending|successful|success|failed|failure|completed|active|inactive)\s+(transaction|transactions|sales|orders|payout|payouts)\b',
                            query_lower,
                        )
                    if status_match:
                        raw_status = status_match.group(1).lower()
                        if raw_status in ['failed', 'failure']:
                            status_value = 'FAILED'
                        elif raw_status in ['successful', 'success']:
                            status_value = 'SUCCESSFUL'
                        else:
                            status_value = raw_status.upper()

                    if status_value and display_data:
                        status_cols = [col for col in display_data[0].keys() if 'status' in str(col).lower()]
                        status_equivalents = {
                            'FAILED': {'FAILED', 'FAILURE'},
                            'SUCCESSFUL': {'SUCCESSFUL', 'SUCCESS'},
                            'PENDING': {'PENDING'},
                            'COMPLETED': {'COMPLETED'},
                            'ACTIVE': {'ACTIVE'},
                            'INACTIVE': {'INACTIVE'},
                        }
                        accepted_values = status_equivalents.get(status_value, {status_value})
                        if status_cols:
                            display_data = [
                                row for row in display_data
                                if any(str(row.get(col, '')).strip().upper() in accepted_values for col in status_cols)
                            ]
                            st.info(f"**Status Filter:** {status_value}")

                    if display_data:
                        display_data, rank_info = apply_top_bottom_n_filter(
                            display_data,
                            query,
                            requested_fields=requested_fields,
                        )
                        if rank_info:
                            st.info(f"**Rank Filter:** {rank_info}")

                    is_city_sales_summary_query = (
                        (('city-wise' in query_lower) or ('city wise' in query_lower) or ('city' in query_lower))
                        and ('sales' in query_lower)
                        and ('merchant' in query_lower)
                        and ('total transaction count' in requested_fields_text or 'transaction count' in requested_fields_text)
                    )
                    if is_city_sales_summary_query and collection_name == 'transaction_history':
                        merchant_details = load_collection_data('numoni_merchant', 'merchantDetails') or []
                        merchant_locations = load_collection_data('numoni_merchant', 'merchantlocation') or []

                        merchant_id_to_user_id = {}
                        for row in merchant_details:
                            merchant_id = str(row.get('_id', '')).strip()
                            user_id = str(row.get('userId', '')).strip()
                            if merchant_id and user_id:
                                merchant_id_to_user_id[merchant_id] = user_id

                        user_id_to_city = {}
                        for row in merchant_locations:
                            user_id = str(row.get('userId', '')).strip()
                            city = str(row.get('city', '')).strip()
                            if user_id and city and user_id not in user_id_to_city:
                                user_id_to_city[user_id] = city

                        city_field = next((f for f in requested_fields if 'city' in f.lower()), 'city name')
                        merchant_field = next((f for f in requested_fields if 'merchant name' in f.lower()), 'merchant name')
                        sales_field = next((f for f in requested_fields if 'sales' in f.lower() or 'amount' in f.lower()), 'total sales amount')
                        count_field = next((f for f in requested_fields if 'count' in f.lower()), 'total transaction count')

                        aggregates = {}
                        for row in data:
                            merchant_id = str(row.get('merchantId', '')).strip()
                            merchant_name = row.get('merchantName') or 'Unknown Merchant'
                            user_id = merchant_id_to_user_id.get(merchant_id)
                            city_name = user_id_to_city.get(user_id) if user_id else None
                            city_name = city_name or 'Unknown'

                            amount_value = 0.0
                            for amount_key in ['amount', 'amountPaid', 'settledAmount', 'totalAmountPaid']:
                                raw = row.get(amount_key)
                                if raw is not None:
                                    try:
                                        amount_value = float(raw)
                                        break
                                    except Exception:
                                        pass

                            key = (city_name, str(merchant_name))
                            if key not in aggregates:
                                aggregates[key] = {'sales': 0.0, 'count': 0}
                            aggregates[key]['sales'] += amount_value
                            aggregates[key]['count'] += 1

                        display_data = []
                        for (city_name, merchant_name), metrics in aggregates.items():
                            out_row = {}
                            for field_name in requested_fields:
                                field_low = field_name.lower()
                                if field_name == city_field or 'city' in field_low:
                                    out_row[field_name] = city_name
                                elif field_name == merchant_field or 'merchant name' in field_low:
                                    out_row[field_name] = merchant_name
                                elif field_name == sales_field or 'sales' in field_low or ('amount' in field_low and 'transaction' not in field_low):
                                    out_row[field_name] = round(metrics['sales'], 2)
                                elif field_name == count_field or 'count' in field_low:
                                    out_row[field_name] = metrics['count']
                                else:
                                    out_row[field_name] = None
                            display_data.append(out_row)

                    # For "I only want ..." location-style queries, return unique rows only
                    requested_fields_lower = [field.lower().strip() for field in requested_fields]
                    requested_joined = ' '.join(requested_fields_lower)
                    requested_compact = requested_joined.replace(' ', '').replace('_', '')
                    unique_filter_tokens = load_unique_filter_tokens_from_databases()
                    is_location_unique_query = (
                        ('only want' in query_lower) and
                        any(
                            (token in requested_joined) or
                            (token.replace(' ', '').replace('_', '') in requested_compact)
                            for token in unique_filter_tokens
                        )
                    )
                    if is_location_unique_query and display_data:
                        seen = set()
                        unique_rows = []
                        for row in display_data:
                            key = tuple(str(row.get(field_name, '')) for field_name in requested_fields)
                            if key not in seen:
                                seen.add(key)
                                unique_rows.append(row)
                        display_data = unique_rows
                
                # ===== CASE 2: No specific fields requested - Show all fields =====
                else:
                    st.info(f"**All Fields:** Showing all available columns")
                    display_data = []
                    
                    # Limit to first 1000 records for performance
                    data_to_show = data[:1000] if len(data) > 1000 else data
                    
                    for record in data_to_show:
                        row = {}
                        for key, value in record.items():
                            # Skip MongoDB ObjectId
                            if key == '_id':
                                continue
                            # Clean MongoDB formatting
                            if isinstance(value, dict) and '$date' in value:
                                row[key] = pd.to_datetime(value['$date']).strftime('%Y-%m-%d')
                            elif isinstance(value, str) and 'T' in value and 'Z' in value:
                                row[key] = pd.to_datetime(value).strftime('%Y-%m-%d')
                            else:
                                row[key] = value
                        display_data.append(row)
                    
                    if len(data) > 1000:
                        st.warning(f"⚠️ Showing first 1,000 of {len(data):,} records for performance")
                
                # Display as table
                st.markdown("---")
                st.subheader(f"📊 Results ({len(display_data)} records)")
                df = pd.DataFrame(display_data)
                if df.empty:
                    st.info("No matching records found (0 records).")
                else:
                    st.dataframe(df, use_container_width=True, hide_index=True)
                
                # CSV Download
                csv_buffer = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv_buffer,
                    file_name=f"{collection_name}_filtered_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
                # Summary stats (only if fields were explicitly specified)
                if requested_fields and not df.empty:
                    st.markdown("---")
                    st.subheader("📈 Summary")
                    cols = st.columns(len(field_mapping)) if field_mapping and len(field_mapping) > 0 else []
                    for idx, (display_name, _) in enumerate(field_mapping.items() if field_mapping else []):
                        if idx < len(cols):
                            with cols[idx]:
                                if display_name not in df.columns:
                                    st.metric(label=display_name, value="0")
                                    continue
                                col_data = df[display_name]
                                if col_data.dtype in ['int64', 'float64']:
                                    st.metric(label=display_name, value=f"{col_data.sum():,.0f}")
                                else:
                                    safe_col_data = col_data.map(
                                        lambda value: str(value) if isinstance(value, (dict, list, set)) else value
                                    )
                                    st.metric(label=display_name, value=f"{safe_col_data.nunique()} unique")
            
            st.stop()
        
        # CHECK 0: Is this a RANKING query?
        is_ranking = any(word in query_lower for word in ['rank', 'ranking', 'ranked by', 'top by', 'list by', 'sort by'])
        
        # CHECK 1: Is this a COMPARISON query?
        is_comparison = any(word in query_lower for word in ['compare', 'comparison', 'versus', 'vs', 'difference between'])
        
        if is_ranking:
            st.markdown('<div class="part-header">🏆 Ranking Query Mode</div>', unsafe_allow_html=True)
            
            with st.spinner("Processing ranking query..."):
                # Parse ranking query with NLP
                ranking_parsed = parse_ranking_query(query)
                
                if not ranking_parsed:
                    st.error("Could not parse ranking query. Try: 'rank [entity] by [field]' or 'rank [entity] based on [field]'")
                    st.stop()
                
                rank_entity = ranking_parsed['rank_entity']
                rank_by = ranking_parsed['rank_by']
                
                st.write(f"**Parsed Query:** Rank '{rank_entity}' by '{rank_by}'")
                
                # PART 1: Database Detection (Intelligent for Ranking Queries)
                st.markdown("---")
                st.subheader("📊 PART 1: Database Detection")
                
                # Smart database detection for ranking queries
                rank_entity_lower = rank_entity.lower()
                rank_by_lower = rank_by.lower()
                query_context = f"{rank_entity_lower} {rank_by_lower} {query_lower}".lower()
                
                # Priority: explicit entity mention in rank_entity
                # Check for CUSTOMER-specific entities
                customer_entities = ['customer', 'client', 'user', 'wallet', 'otp', 'topup', 
                                   'favorite deal', 'favourite deal', 'customer session', 'customer error',
                                   'customer transaction']
                
                # Check for MERCHANT-specific entities
                merchant_entities = ['merchant', 'vendor', 'shop', 'store', 'business', 'pos', 'terminal',
                                   'bank', 'deal', 'payout', 'settlement', 'review', 'rating',
                                   'region', 'regions', 'geography', 'state']  # FIXED: geography is merchant DB
                
                db_name = None
                db_reason = None
                
                # 1. Check if rank_entity explicitly mentions customer (non-geography)
                if any(entity in rank_entity_lower for entity in ['customer', 'client', 'user']):
                    # But exclude if it's about regions/geography
                    if not any(geo in rank_entity_lower for geo in ['region', 'region', 'geography', 'state']):
                        db_name = 'numoni_customer'
                        db_reason = f"Explicitly about '{rank_entity}' (customer-related entity)"
                
                # 2. Check if rank_entity contains favorite deals (customer db)
                if not db_name and any(fav in rank_entity_lower for fav in ['favorite', 'favourite']):
                    db_name = 'numoni_customer'
                    db_reason = f"'{rank_entity}' found in customer database"
                
                # 3. Check if rank_by mentions customer context
                if not db_name and 'customer' in rank_by_lower:
                    db_name = 'numoni_customer'
                    db_reason = f"Ranking by customer-related field: '{rank_by}'"
                
                # 4. Check for geography/region/state entities (MERCHANT db, not customer)
                if not db_name and any(geo in rank_entity_lower for geo in ['region', 'regions', 'geography', 'state']):
                    db_name = 'numoni_merchant'
                    db_reason = f"'{rank_entity}' is a geography entity (merchant database has geography table)"
                
                # 5. Check if this is a merchant-specific entity
                if not db_name and any(entity in rank_entity_lower for entity in merchant_entities):
                    db_name = 'numoni_merchant'
                    db_reason = f"'{rank_entity}' is a merchant-specific entity"
                
                # 6. Fallback: use original detect_database function
                if not db_name:
                    db_result = detect_database(query)
                    db_name = (db_result.get('selected_dbs') or [db_result.get('selected_database')])[0]
                    db_reason = db_result.get('reason', 'Standard keyword matching')
                
                st.success(f"**Selected Database:** {db_name.replace('numoni_', '').upper()}")
                with st.expander("Details"):
                    st.info(f"Reason: {db_reason}")
                
                # Show available tables and keywords for this database
                db_short = 'CUSTOMER_DB' if 'customer' in db_name else 'MERCHANT_DB'
                available_tables = list(KEYWORDS_DB[db_short].keys())
                with st.expander("📊 Available Tables & Fields"):
                    cols = st.columns(2)
                    with cols[0]:
                        st.caption(f"**Total Tables:** {len(available_tables)}")
                        for i, table in enumerate(sorted(available_tables)[:12]):
                            st.text(f"• {table}")
                    with cols[1]:
                        if i + 1 < len(available_tables):
                            for table in sorted(available_tables)[12:]:
                                st.text(f"• {table}")
                
                # PART 2: Collection Detection
                st.markdown("---")
                st.subheader("📋 PART 2: Collection Detection")
                
                # Special handling for ranking queries - smart collection preferences
                metadata = load_metadata(db_name)
                
                # Use intelligent entity-to-collection matching from ranking_handler
                # This properly matches "customer error" → "customerError", etc.
                collection_name, match_confidence = match_entity_to_collection(rank_entity, metadata)
                
                # If no good match, try fuzzy matching as fallback
                if not collection_name or match_confidence < 0.5:
                    best_coll = None
                    best_score = 0
                    
                    for coll in metadata.keys():
                        # Give higher weight to entity match than field match
                        entity_score = similarity(rank_entity, coll)
                        field_score = similarity(rank_by, coll)
                        
                        # Boost for word presence
                        if rank_entity.lower() in coll.lower():
                            entity_score = min(1.0, entity_score + 0.3)
                        if rank_by.lower() in coll.lower():
                            field_score = min(1.0, field_score + 0.3)
                        
                        # Combined score: 70% entity, 30% field
                        combined_score = (entity_score * 0.7) + (field_score * 0.3)
                        
                        if combined_score > best_score:
                            best_score = combined_score
                            best_coll = coll
                    
                    collection_name = best_coll or list(metadata.keys())[0]
                    match_confidence = best_score
                
                st.success(f"**Selected Collection:** {collection_name}")
                with st.expander("Details"):
                    st.info(f"Reason: Matched '{rank_entity}' to collection with {match_confidence:.0%} confidence")
                
                # Show available fields for this collection
                db_short = 'CUSTOMER_DB' if 'customer' in db_name else 'MERCHANT_DB'
                available_fields = get_table_fields(collection_name, db_short)
                if available_fields:
                    with st.expander("🎯 Available Fields"):
                        st.caption(f"**Total Fields:** {len(available_fields)}")
                        field_str = ", ".join(sorted(available_fields)[:15])
                        st.text(field_str)
                        if len(available_fields) > 15:
                            st.caption(f"... and {len(available_fields) - 15} more fields")
                
                # Load data
                st.markdown("---")
                st.subheader("📥 Loading Data")
                data = load_collection_data(db_name, collection_name)
                st.success(f"✅ Loaded {len(data):,} records")
                
                if not data:
                    st.error(f"No data found in {collection_name}")
                    st.stop()
                
                # PART 3: Field Detection
                st.markdown("---")
                st.subheader("🔍 PART 3: Field Analysis")
                
                all_fields = set()
                for record in data:
                    all_fields.update(record.keys())
                
                st.write(f"**Available fields:** {', '.join(sorted(all_fields)[:10])}...")
                
                # Execute ranking
                ranking_result = execute_ranking(data, rank_entity, rank_by, 'count', collection_name)
                
                grouping_field = ranking_result.get('grouping_field')
                ranking_field = ranking_result.get('ranking_field')
                
                if 'error' in ranking_result:
                    st.error(f"❌ {ranking_result['error']}")
                    st.stop()
                
                st.success(f"**Grouping by:** {grouping_field}")
                st.success(f"**Ranking metric:** Count (by {ranking_field or 'frequency'})")
                
                # PART 4: Ranking Results
                st.markdown("---")
                st.subheader("🏆 PART 4: Ranking Results")
                
                formatted = format_ranking_results(ranking_result, collection_name, limit=20)
                
                if isinstance(formatted, dict) and 'error' in formatted:
                    st.error(f"❌ {formatted['error']}")
                else:
                    df = pd.DataFrame(formatted)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    
                    total_groups = ranking_result.get('total_groups', 0)
                    st.caption(f"Showing top 20 of {total_groups} groups")
                    
                    # Visualization
                    if len(formatted) > 0:
                        st.bar_chart(pd.DataFrame(formatted).head(10).set_index('Name')['Count'])
            
            st.stop()
        
        # CHECK 1: Is this a COMPARISON query?
        
        if is_comparison:
            st.markdown('<div class="part-header">📊 Comparison Analysis Mode</div>', unsafe_allow_html=True)

            # Keyword-driven comparison path using part2 collection_keywords.json
            compare_keyword_result = handle_compare_keyword_query(query, load_collection_data)
            if compare_keyword_result.get("handled"):
                collections_used = compare_keyword_result.get("collections", [])
                source_dbs = compare_keyword_result.get("source_dbs", {})
                rows = compare_keyword_result.get("rows", [])
                metric_labels = compare_keyword_result.get("metric_labels", [])
                scope = compare_keyword_result.get("scope", "entity")

                st.success(f"**Selected Collections:** {', '.join(collections_used)}")
                if source_dbs:
                    st.caption(
                        "Source DBs: " + ", ".join(
                            f"{collection} ({db_name})" for collection, db_name in source_dbs.items()
                        )
                    )

                if rows:
                    display_df = pd.DataFrame(rows)
                    top_n_match = re.search(r"\btop\s+(\d+)\b", query_lower)
                    if top_n_match:
                        try:
                            limit = max(1, int(top_n_match.group(1)))
                            display_df = display_df.head(limit)
                        except Exception:
                            pass

                    x_label = f"{str(scope).title()} Name"
                    if x_label not in display_df.columns:
                        fallback_x = [col for col in [f"{str(scope).title()} ID", "Entity Name", "Entity ID"] if col in display_df.columns]
                        x_label = fallback_x[0] if fallback_x else display_df.columns[0]

                    chart_metrics = [label for label in metric_labels if label in display_df.columns]
                    if chart_metrics:
                        st.subheader("📊 Comparison Chart")
                        chart_df = display_df[[x_label] + chart_metrics].copy()
                        chart_df[x_label] = chart_df[x_label].astype(str)
                        st.bar_chart(chart_df.set_index(x_label)[chart_metrics])

                    st.subheader("📈 Comparison Table")
                    st.dataframe(display_df, use_container_width=True, hide_index=True)

                    with st.expander("Keyword Matching Details"):
                        st.json(
                            {
                                "conjunction_counts": compare_keyword_result.get("conjunction_counts", {}),
                                "compare_phrases": compare_keyword_result.get("compare_phrases", []),
                                "compare_phrase_matches": compare_keyword_result.get("compare_phrase_matches", []),
                                "ranked_collections": compare_keyword_result.get("ranked_collections", []),
                            },
                            expanded=False,
                        )
                else:
                    st.info("No comparison rows found for the selected collections.")

                st.stop()
            
            with st.spinner("Analyzing comparison query..."):
                # Detect time periods (last year vs this year, last month vs this month, etc.)
                import re
                from datetime import datetime, timedelta
                from advanced_filter_executor import apply_advanced_filters
                
                current_date = datetime(2026, 2, 16)
                
                # Parse comparison time periods
                has_last_year = 'last year' in query_lower
                has_this_year = 'this year' in query_lower
                has_last_month = 'last month' in query_lower
                has_this_month = 'this month' in query_lower
                
                # Detect entity type (merchant, customer, transaction)
                entity_type = None
                collection_name = None
                if 'merchant' in query_lower:
                    entity_type = 'merchant'
                    if 'transaction' in query_lower:
                        collection_name = 'transaction_history'
                        db_name = 'numoni_merchant'
                    else:
                        collection_name = 'merchantDetails'
                        db_name = 'numoni_merchant'
                elif 'customer' in query_lower:
                    entity_type = 'customer'
                    if 'transaction' in query_lower:
                        collection_name = 'transaction_history'
                        db_name = 'numoni_customer'
                    else:
                        collection_name = 'customerDetails'
                        db_name = 'numoni_customer'
                elif 'transaction' in query_lower:
                    collection_name = 'transaction_history'
                    db_name = 'numoni_merchant'
                
                if not collection_name:
                    st.error("Could not determine what to compare. Please specify merchant, customer, or transaction.")
                    st.stop()
                
                st.info(f"**Comparing:** {entity_type or 'transaction'} data in {collection_name}")
                
                # Load data
                data = load_collection_data(db_name, collection_name)
                
                if not data:
                    st.error(f"No data found in {collection_name}")
                    st.stop()
                
                st.success(f"Loaded {len(data)} records")
                
                # Filter by time periods
                from advanced_filter_detector import detect_advanced_filters
                
                results = {}
                
                if has_last_year and has_this_year:
                    # Last year
                    filters_ly = detect_advanced_filters(f"data from last year", datetime(2026, 2, 16))
                    filtered_ly, _ = apply_advanced_filters(data, filters_ly, collection_name)
                    results['Last Year (2025)'] = len(filtered_ly)
                    
                    # This year
                    filters_ty = detect_advanced_filters(f"data from this year", datetime(2026, 2, 16))
                    filtered_ty, _ = apply_advanced_filters(data, filters_ty, collection_name)
                    results['This Year (2026)'] = len(filtered_ty)
                
                elif has_last_month and has_this_month:
                    # Last month
                    filters_lm = detect_advanced_filters(f"data from last month", datetime(2026, 2, 16))
                    filtered_lm, _ = apply_advanced_filters(data, filters_lm, collection_name)
                    results['Last Month (Jan 2026)'] = len(filtered_lm)
                    
                    # This month
                    filters_tm = detect_advanced_filters(f"data from this month", datetime(2026, 2, 16))
                    filtered_tm, _ = apply_advanced_filters(data, filters_tm, collection_name)
                    results['This Month (Feb 2026)'] = len(filtered_tm)
                
                # Display results
                st.subheader("📈 Comparison Results")
                
                col1, col2, col3 = st.columns(3)
                periods = list(results.keys())
                values = list(results.values())
                
                if len(periods) >= 2:
                    with col1:
                        st.metric(periods[0], f"{values[0]:,}")
                    with col2:
                        st.metric(periods[1], f"{values[1]:,}")
                    with col3:
                        diff = values[1] - values[0]
                        pct = (diff / values[0] * 100) if values[0] > 0 else 0
                        st.metric("Change", f"{diff:+,}", f"{pct:+.1f}%")
                
                # Visualization
                df_results = pd.DataFrame({'Period': periods, 'Count': values})
                st.bar_chart(df_results.set_index('Period'))
                
                # AI Analysis with Groq
                st.markdown("---")
                st.subheader("🤖 AI Analysis")
                
                try:
                    groq_key = os.getenv('GROQ_API_KEY', '')
                    client = Groq(api_key=groq_key)
                    
                    analysis_prompt = f"""Based on this comparison data:
{periods[0]}: {values[0]:,} records
{periods[1]}: {values[1]:,} records
Change: {diff:+,} ({pct:+.1f}%)

Entity: {entity_type or 'transactions'}
Collection: {collection_name}

Provide a brief 2-3 sentence analysis explaining:
1. What this trend indicates
2. What business insights can be drawn
3. Any recommendations

Keep it concise and actionable."""
                    
                    with st.spinner("Generating AI analysis..."):
                        response = client.chat.completions.create(
                            model="llama-3.1-70b-versatile",
                            messages=[{"role": "user", "content": analysis_prompt}],
                            temperature=0.7,
                            max_tokens=200
                        )
                    
                    st.success("**AI Insights:**")
                    st.write(response.choices[0].message.content)
                
                except Exception as e:
                    st.warning(f"AI analysis unavailable: {str(e)}")
            
            st.stop()  # Exit after comparison
        
        # CHECK 2: Is this a generic "show information/details" query OR just search terms?
        is_info_query = any(phrase in query_lower for phrase in [
            'show me information', 'information of', 'info about', 'details about', 
            'tell me about', 'show information', 'find information', 'show me details',
            'details of', 'show details'
        ])
        
        # Check if query is just asking about a bank name (simple search)
        import re
        bank_keywords = ['bank', 'zenith', 'sterling', 'access', 'gtbank', 'uba', 'firstbank', 'moniepoint', 'globus']
        is_simple_search = any(keyword in query_lower for keyword in bank_keywords) and len(query.split()) <= 5
        
        if is_info_query or is_simple_search:
            # Extract search terms
            if is_info_query:
                terms_patterns = [
                    r'(?:information|info|details)(?:\s+of)?\s+(?:about\s+)?(.+)',
                    r'(?:tell me about|show)\s+(.+)',
                    r'find\s+(?:information|details)\s+(?:of|about)\s+(.+)'
                ]
                search_terms = []
                for pattern in terms_patterns:
                    match = re.search(pattern, query_lower)
                    if match:
                        term_text = match.group(1).strip()
                        search_terms = [t.strip() for t in re.split(r'\s+or\s+', term_text)]
                        break
            else:
                # Simple search - use the whole query
                search_terms = [query_lower.strip()]
            
            if search_terms:
                st.markdown('<div class="part-header">🔍 Smart Content Search</div>', unsafe_allow_html=True)
                st.info(f"Searching for: **{', '.join(search_terms)}**")
                
                with st.spinner("Searching all collections..."):
                    # Field importance map per collection type
                    important_fields_map = {
                        'bankInformation': ['bankname', 'bankcode', 'accountNo', 'accountHolderName', 'bankTransferCode', 'primary', 'active', 'minimumSpentAmount'],
                        'transaction': ['transactionName', 'amount', 'status', 'senderName', 'receiverName', 'merchantName', 'customerName', 'senderBankName'],
                        'customer': ['name', 'email', 'phoneNumber', 'status', 'balance', 'accountNumber'],
                        'merchant': ['businessName', 'name', 'email', 'phoneNumber', 'status', 'balance', 'category'],
                        'wallet': ['amount', 'balance', 'status', 'senderName', 'senderBankName', 'beneficiaryAccountName', 'bankName'],
                        'default': ['name', 'businessName', 'bankname', 'accountNo', 'accountHolderName', 'amount', 'status', 'email', 'phoneNumber']
                    }
                    
                    def get_important_fields_for_collection(coll_name, record):
                        # Determine field set based on collection name
                        if 'bank' in coll_name.lower():
                            field_list = important_fields_map['bankInformation']
                        elif 'transaction' in coll_name.lower():
                            field_list = important_fields_map['transaction']
                        elif 'customer' in coll_name.lower() and 'merchant' not in coll_name.lower():
                            field_list = important_fields_map['customer']
                        elif 'merchant' in coll_name.lower():
                            field_list = important_fields_map['merchant']
                        elif 'wallet' in coll_name.lower() or 'ledger' in coll_name.lower():
                            field_list = important_fields_map['wallet']
                        else:
                            field_list = important_fields_map['default']
                        
                        result = {k: v for k, v in record.items() if k in field_list and v is not None}
                        return result if result else {k: v for k, v in list(record.items())[:6]}  # Fallback: first 6 fields
                    
                    # Search all collections
                    all_results = []
                    for db in ["numoni_customer", "numoni_merchant"]:
                        metadata = load_metadata(db)
                        for coll in metadata.keys():
                            try:
                                data = load_collection_data(db, coll)
                                if not data:
                                    continue
                                
                                matches = []
                                for record in data:
                                    for term in search_terms:
                                        # Fuzzy match: check if term is substring of any field value
                                        if any(term in str(v).lower() for v in record.values() if v):
                                            matches.append(record)
                                            break
                                
                                if matches:
                                    all_results.append({
                                        'database': db,
                                        'collection': coll,
                                        'count': len(matches),
                                        'matches': matches[:20]  # Top 20 per collection
                                    })
                            except:
                                continue
                    
                    # Show results
                    if all_results:
                        st.success(f"✅ Found data in {len(all_results)} collections")
                        
                        # Show collection summary
                        st.subheader("📊 Results by Collection")
                        summary_data = []
                        for r in all_results:
                            summary_data.append({
                                'Database': r['database'].replace('numoni_', '').upper(),
                                'Collection': r['collection'],
                                'Matches': r['count']
                            })
                        st.dataframe(pd.DataFrame(summary_data), hide_index=True, use_container_width=True)
                        
                        # Show data from collections
                        st.markdown("---")
                        st.subheader("🔎 Detailed Results")
                        
                        for r in all_results[:8]:  # Show top 8 collections
                            with st.expander(f"📁 {r['collection']} ({r['count']} matches)", expanded=True if r['collection'] == 'bankInformation' else False):
                                # Extract important fields based on collection type
                                display_data = []
                                for match in r['matches'][:10]:  # Show 10 records per collection
                                    important = get_important_fields_for_collection(r['collection'], match)
                                    if important:
                                        display_data.append(important)
                                
                                if display_data:
                                    df = pd.DataFrame(display_data)
                                    st.dataframe(df, use_container_width=True, hide_index=True)
                                    
                                    if r['count'] > 10:
                                        st.caption(f"Showing 10 of {r['count']} total matches")
                                else:
                                    st.write("No displayable fields")
                    else:
                        st.warning("❌ No matching data found")
                
                st.stop()  # Exit after content search
        
        # Clean query for detection - remove field specifications to avoid confusion
        # e.g., "show me merchant wallet. I want invoice" -> "show me merchant wallet"
        import re
        query_for_detection = query
        field_spec_patterns = [
            r'[,\.]\s*i\s+(?:only\s+)?want\s+.+$',
            r'[,\.]\s*i\s+need\s+.+$', 
            r'[,\.]\s*looking\s+for\s+.+$',
            r'[,\.]\s*with\s+[a-z\s,]+$'
        ]
        for pattern in field_spec_patterns:
            query_for_detection = re.sub(pattern, '', query_for_detection, flags=re.IGNORECASE)
        query_for_detection = query_for_detection.strip()
        
        # PART 1: Database Detection
        st.markdown('<div class="part-header">📊 PART 1: Database Detection</div>', unsafe_allow_html=True)
        with st.spinner("Detecting database..."):
            db_result = detect_database(query_for_detection)
            db_list = db_result.get('selected_dbs') or [db_result.get('selected_database')]
            database_name = db_list[0] if db_list else None
        
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            db_display = database_name.replace('numoni_', '').upper() if database_name else 'N/A'
            st.success(f"**Selected Database:** {db_display}")
        with col2:
            st.metric("Confidence", f"{db_result.get('confidence', 0.5):.0%}")
        with col3:
            st.metric("Score", f"{db_result.get('score', 0):.1f}")
        
        with st.expander("🔍 Database Detection Details"):
            st.info(f"**Reason:** {db_result.get('reason', 'N/A')}")
        
        if not database_name:
            st.error("No database detected")
            st.stop()
        
        # PART 2: Collection Detection
        st.markdown('<div class="part-header">�️ PART 2: Collection Detection</div>', unsafe_allow_html=True)
        
        # Clean query for collection detection - use the same cleaned query from database detection
        # This avoids field specifications confusing collection detection
        # e.g., "show me payout. I want merchant fee" -> "show me payout"
        
        with st.spinner("Detecting collection..."):
            # SPECIAL PRIORITY RULES: keep transaction-intent queries on transaction_history
            query_lower_check = query.lower()
            grouping_keywords = ['group by', 'grouped by', 'group', 'grouped', 'categorize', 'category', 'breakdown']
            has_grouping = any(kw in query_lower_check for kw in grouping_keywords)
            transaction_keywords = [
                'transaction', 'transactions', 'transcation', 'transcations',
                'transaction history', 'last transaction',
                'debit', 'credit', 'payment', 'payments'
            ]
            transaction_intent_phrases = [
                'balance before', 'balance after',
                'failed transaction', 'failed transactions',
                'successful transaction', 'successful transactions'
            ]
            has_transaction = any(kw in query_lower_check for kw in transaction_keywords)
            has_transaction_intent = has_transaction or any(p in query_lower_check for p in transaction_intent_phrases)
            
            if has_transaction and has_grouping:
                # Priority boost: use transaction_history for transaction + grouping queries
                collection_name = 'transaction_history'
                coll_result = {
                    'selected_collection': 'transaction_history',
                    'confidence': 1.0,
                    'score': 999,
                    'reason': 'Priority rule: Transaction + Grouping query → transaction_history collection'
                }
            elif has_transaction_intent and database_name in ['numoni_customer', 'numoni_merchant']:
                # Priority boost: use transaction_history for transaction-intent queries
                collection_name = 'transaction_history'
                coll_result = {
                    'selected_collection': 'transaction_history',
                    'confidence': 0.98,
                    'score': 995,
                    'reason': 'Priority rule: Transaction-intent query → transaction_history collection'
                }
            else:
                coll_result = detect_collection(query_for_detection, database_name)
                collection_name = coll_result.get('selected_collection')
        
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.success(f"**Selected Collection:** {collection_name}")
        with col2:
            st.metric("Confidence", f"{coll_result.get('confidence', 0):.0%}")
        with col3:
            st.metric("Score", f"{coll_result.get('score', 0):.1f}")
        
        with st.expander("🔍 Collection Detection Details"):
            st.info(f"**Reason:** {coll_result.get('reason', 'N/A')}")
            
            if coll_result.get('matched_fields'):
                matched_fields_str = ', '.join(coll_result['matched_fields'][:8])
                st.markdown(f"**Matched Fields:** {matched_fields_str}")
            
            if coll_result.get('matched_values'):
                matched_values_str = ', '.join(str(v) for v in coll_result['matched_values'][:5])
                st.markdown(f"**Matched Values:** {matched_values_str}")
            
            alternatives = coll_result.get('alternatives', [])
            if alternatives:
                st.markdown("**Alternatives:**")
                for i, alt in enumerate(alternatives[:5], 1):
                    st.text(f"  {i}. {alt['collection']}: {alt['score']:.1f} pts (confidence: {alt['confidence']:.0%})")
        
        if not collection_name:
            st.error("No collection detected")
            st.stop()
        
        # PART 3: Action Detection
        st.markdown('<div class="part-header">⚡ PART 3: Action Detection & Execution</div>', unsafe_allow_html=True)
        action_result = detect_action(query)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info(f"**Action:** {action_result.get('primary_action', 'N/A').upper()}")
        with col2:
            if action_result.get('aggregation'):
                st.info(f"**Aggregation:** {action_result['aggregation']}")
            elif action_result.get('limit'):
                st.info(f"**Limit:** {action_result['limit']} records")
            else:
                st.info("**Type:** Query")
        with col3:
            filters_count = len(action_result.get('filters', {}))
            st.info(f"**Filters:** {filters_count}")
        
        with st.expander("🔍 Action Metadata"):
            st.json(action_result, expanded=False)
        
        # PART 4: Advanced Filters
        st.markdown('<div class="part-header">🔍 PART 4: Advanced Filters</div>', unsafe_allow_html=True)
        advanced_filters = detect_advanced_filters(query)
        
        if advanced_filters['has_advanced_filters']:
            cols = st.columns(3)
            
            # Date filters
            if advanced_filters.get('date_filters'):
                with cols[0]:
                    df = advanced_filters['date_filters']
                    filter_type = df.get('type', 'N/A')
                    details = [f"Type: {filter_type}"]
                    if 'year' in df:
                        details.append(f"Year: {df['year']}")
                    if 'month' in df:
                        details.append(f"Month: {df['month']}")
                    if 'days_ago' in df:
                        details.append(f"Days ago: {df['days_ago']}")
                    
                    st.info("**📅 Date Filter**\n\n" + "\n\n".join(details))
                    
                    # Show suggestion if collection mismatch
                    if advanced_filters.get('suggested_collection'):
                        if advanced_filters['suggested_collection'] != collection_name:
                            st.warning(f"💡 **Suggestion:** {advanced_filters['collection_reason']}")
            
            # Location filters
            if advanced_filters.get('location_filters'):
                with cols[1]:
                    lf = advanced_filters['location_filters']
                    st.info(f"**🌍 Location Filter**\n\nLocation: {lf.get('location', 'N/A')}\n\nType: {lf.get('type', 'any')}")
            
            # Numeric filters
            if advanced_filters.get('numeric_filters'):
                with cols[2]:
                    filter_descs = [nf['description'] for nf in advanced_filters['numeric_filters']]
                    st.info(f"**🔢 Numeric Filter**\n\n{chr(10).join(filter_descs)}")
        else:
            st.info("No advanced filters detected")
        
        # EXECUTION
        st.markdown("---")
        st.subheader("📊 Query Results")
        with st.spinner("Executing query on database..."):
            exec_result = execute_action(
                action_result,
                database_name,
                collection_name,
                advanced_filters=advanced_filters
            )
        
        # Show filter validation messages
        if exec_result.get('filter_messages'):
            for msg in exec_result['filter_messages']:
                if '⚠️' in msg:
                    st.warning(msg)
                else:
                    st.success(msg)
        
        if 'error' in exec_result:
            st.error(f"❌ Error: {exec_result['error']}")
        elif 'result' in exec_result:
            result = exec_result['result']
            
            if exec_result.get('summary'):
                st.success(f"✅ {exec_result['summary']}")
            
            # SPECIAL HANDLING FOR GROUP_BY: Show all records + grouped visualization
            primary_action = action_result.get('primary_action', '').lower()
            if primary_action == 'group_by' and isinstance(result, dict) and 'results' in result:
                # For GROUP_BY queries, load the full data to show all records
                st.markdown("**📋 All Transaction Records**")
                full_data = exec_result.get('filtered_data') or load_collection_data(database_name, collection_name)
                
                if full_data and len(full_data) > 0:
                    df = pd.DataFrame(full_data)
                    
                    # Show all records
                    display_limit = min(100, len(full_data))
                    st.dataframe(df.head(display_limit), use_container_width=True, hide_index=True)
                    
                    if len(full_data) > display_limit:
                        st.info(f"Showing {display_limit} of {len(full_data)} records")
                    
                    # Download option
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Full Results (CSV)",
                        data=csv,
                        file_name=f"{collection_name}_results.csv",
                        mime="text/csv"
                    )
                
                # Now show the grouped visualization
                st.markdown("---")
                st.markdown(f"**📊 Grouped by {result.get('grouped_by', 'field')}**")
                
                # Build DataFrame from grouped results
                group_field = result.get('grouped_by', 'field')
                agg_type = result.get('aggregation', 'count')
                grouping_data = result.get('results', {})
                
                if grouping_data and isinstance(grouping_data, dict):
                    metric_label = 'Value'
                    if agg_type == 'count':
                        metric_label = 'Count'
                    elif agg_type == 'sum':
                        metric_label = 'Total'
                    elif agg_type == 'avg':
                        metric_label = 'Average'

                    results_df = pd.DataFrame([
                        {"Group": str(k), metric_label: v}
                        for k, v in sorted(grouping_data.items(), key=lambda x: x[1], reverse=True)
                    ])
                    
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.dataframe(results_df, hide_index=True, use_container_width=True)
                    with col2:
                        if len(results_df) > 0:
                            st.bar_chart(results_df.set_index("Group"))
            
            elif isinstance(result, list):
                st.markdown(f"**Found {len(result)} records**")
                
                if len(result) > 0:
                    # NOTE: result already has column filtering applied via filter_important_columns()
                    # Convert to DataFrame
                    df = pd.DataFrame(result)
                    
                    # TRY TO EXTRACT REQUESTED FIELDS from query (e.g., "show me details, I want name, phone")
                    from field_extractor import extract_requested_fields, map_fields_to_columns
                    requested_fields = extract_requested_fields(query)
                    
                    if requested_fields:
                        # User specified which fields they want - filter DataFrame to show only those
                        available_cols = list(df.columns)
                        
                        # Map user field names to exact DataFrame column names
                        field_mapping = map_fields_to_columns(requested_fields, available_cols)
                        
                        if field_mapping:
                            # Get the actual column names from the mapping
                            cols_to_show = [field_mapping[user_field] for user_field in requested_fields if user_field in field_mapping]
                            cols_to_show = [col for col in cols_to_show if col in available_cols]  # Verify columns exist
                            
                            if cols_to_show:
                                # Show only the requested columns with their actual names
                                df = df[cols_to_show]
                                
                                # Rename columns to have capitalized first letter for display
                                rename_map = {col: col[0].upper() + col[1:] if len(col) > 0 else col for col in cols_to_show}
                                df = df.rename(columns=rename_map)
                                
                                # Show info message with capitalized names
                                display_names = list(rename_map.values())
                                st.info(f"**Showing requested fields:** {', '.join(display_names)}")
                            # If no columns matched, still show the table with all available columns
                        # If mapping failed, still show the table with all available columns
                    
                    # Handle MongoDB date format for display
                    for col in df.columns:
                        if col in ['createdDt', 'createdDate', 'updatedDt', 'transactionDate', 'date']:
                            # MongoDB format: {'$date': 'ISO-string'} or plain ISO string
                            def extract_date(val):
                                if isinstance(val, dict) and '$date' in val:
                                    return val['$date']
                                return val
                            
                            df[col] = df[col].apply(extract_date)
                            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    
                    # Show sample records
                    display_limit = min(50, len(result))
                    st.dataframe(df.head(display_limit), use_container_width=True)
                    
                    if len(result) > display_limit:
                        st.info(f"Showing {display_limit} of {len(result)} records")
                    
                    # Download option
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Full Results (CSV)",
                        data=csv,
                        file_name=f"{collection_name}_results.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("No matching records found")
            
            elif isinstance(result, dict):
                # Handle count/aggregation results
                if 'count' in result:
                    st.metric("Total Count", f"{result['count']:,}")
                else:
                    # Generic dict result (skip 'results' key since that's handled in GROUP_BY special case)
                    cols = st.columns(min(len(result), 4))
                    for idx, (key, value) in enumerate(result.items()):
                        with cols[idx % 4]:
                            if isinstance(value, (int, float)):
                                st.metric(key, f"{value:,.2f}" if isinstance(value, float) else f"{value:,}")
                            else:
                                st.json({key: value})

            render_optional_json_linker_section(
                query=query,
                database_name=database_name,
                collection_name=collection_name,
                load_collection_data=load_collection_data,
                linker_path=JSON_LINKER_PATH,
            )
        else:
            st.info("No results returned")

st.markdown("---")
st.caption("Powered by 4-Part Intelligent Pipeline | Token-optimized (<2000 tokens per component)")

