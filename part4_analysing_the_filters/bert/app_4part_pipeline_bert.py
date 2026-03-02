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
from sentence_transformers import SentenceTransformer, util

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db" / "bert"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection" / "bert"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action" / "bert"))
sys.path.insert(0, str(BASE_DIR / "part4_analysing_the_filters" / "bert"))
sys.path.insert(0, str(BASE_DIR / "part4_analysing_the_filters"))

from db_keyword_router_bert import detect_database
from collection_router_bert import detect_collection, load_metadata
from action_detector_bert import detect_action
from action_executor_bert import execute_action, load_collection_data
from advanced_filter_detector_bert import detect_advanced_filters
from ranking_handler_bert import parse_ranking_query, execute_ranking, format_ranking_results
from keywords_lookup import get_matching_tables, get_table_fields, KEYWORDS_DB
from table_linker_bert import enrich_requested_fields_with_links
import os
from datetime import datetime
from groq import Groq


print("🤖 Loading BERT model for Part 4 pipeline...")
try:
    BERT_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    print("✅ BERT model loaded successfully!")
except Exception as e:
    print(f"⚠️ Warning: Could not load BERT model: {e}")
    BERT_MODEL = None

EMBEDDING_CACHE = {}


def get_embedding(text: str):
    if text in EMBEDDING_CACHE:
        return EMBEDDING_CACHE[text]

    embedding = BERT_MODEL.encode(text, convert_to_tensor=True)
    EMBEDDING_CACHE[text] = embedding
    return embedding


def semantic_similarity(text1: str, text2: str) -> float:
    if not text1 or not text2 or BERT_MODEL is None:
        return 0.0

    try:
        emb1 = get_embedding(text1.lower().strip())
        emb2 = get_embedding(text2.lower().strip())
        return float(util.cos_sim(emb1, emb2).item())
    except Exception:
        return 0.0


def match_entity_to_collection_bert(entity: str, rank_by: str, metadata: dict):
    if not metadata:
        return None, 0.0

    best_coll = None
    best_score = 0.0

    for coll in metadata.keys():
        entity_score = semantic_similarity(entity, coll)
        field_score = semantic_similarity(rank_by, coll) if rank_by else 0.0

        if entity.lower() in coll.lower():
            entity_score = min(1.0, entity_score + 0.30)
        if rank_by and rank_by.lower() in coll.lower():
            field_score = min(1.0, field_score + 0.30)

        combined_score = (entity_score * 0.7) + (field_score * 0.3)

        if combined_score > best_score:
            best_score = combined_score
            best_coll = coll

    return best_coll, best_score


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
        has_field_intent = any(phrase in query_lower for phrase in [
            'i want', 'only want', 'show me only', 'display only', 'just the',
            'these fields', 'these columns', 'following fields', 'following columns',
            'field', 'column'
        ])
        
        # CHECK 0a: Is this a DATA RETRIEVAL query with specific fields requested?
        # Only trigger for queries that explicitly ask for field selection, NOT generic "show me"
        is_data_query = (bool(requested_fields_hint) and has_field_intent) or (
            any(phrase in query_lower for phrase in [
            'get me all the data', 'only want', 'i want these', 'show me only', 'display only',
            'just the', 'these fields', 'these columns', 'following fields', 'following columns'
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
                    st.info(f"**Date Filter:** {requested_date[0].strftime('%B %d, %Y')}")
                
                # Detect database using PART 1
                db_result = detect_database(query)
                db_name = (db_result.get('selected_dbs') or [db_result.get('selected_database')])[0]
                
                # Detect collection using PART 2 - proper NLP matching
                from collection_router_bert import detect_collection
                query_for_collection = query
                marker_positions = []
                query_lower_for_split = query.lower()
                for marker in [' i only want ', ' i want ', ' show me only ', ' display only ', ' just the ']:
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
                collection_name = None
                
                if has_transaction and has_grouping:
                    # Priority boost: use transaction_history for transaction + grouping queries
                    collection_name = 'transaction_history'
                else:
                    collection_result = detect_collection(query_for_collection, db_name)
                    collection_name = collection_result.get('selected_collection')
                
                if not collection_name:
                    st.error(f"Could not determine collection from query")
                    st.stop()
                
                st.success(f"**Database:** {db_name.replace('numoni_', '').upper()} | **Collection:** {collection_name}")
                
                # Load data
                data = load_collection_data(db_name, collection_name)
                if not data:
                    st.error(f"No data found in {collection_name}")
                    st.stop()
                
                st.success(f"✅ Loaded {len(data):,} records")
                
                # Apply date filter if specified
                if requested_date:
                    start_date, end_date = requested_date
                    filtered_data = []
                    for record in data:
                        # Check common date fields
                        record_date = None
                        for date_field in ['transactionDate', 'createdDate', 'createdDt', 'updatedDt', 'updatedAt', 'activityTime', 'date']:
                            if date_field in record:
                                try:
                                    date_str = record[date_field]
                                    if isinstance(date_str, str):
                                        record_date = pd.to_datetime(date_str)
                                    elif isinstance(date_str, dict) and '$date' in date_str:
                                        record_date = pd.to_datetime(date_str['$date'])
                                    
                                    if record_date and start_date.date() <= record_date.date() <= end_date.date():
                                        filtered_data.append(record)
                                    break
                                except:
                                    pass
                    data = filtered_data
                    st.success(f"📅 After date filter: {len(data):,} records")
                
                if not data:
                    st.warning("No records match the date filter")
                    st.stop()
                
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
                if requested_fields:
                    st.markdown("---")
                    st.subheader("📈 Summary")
                    cols = st.columns(len(field_mapping)) if field_mapping and len(field_mapping) > 0 else []
                    for idx, (display_name, _) in enumerate(field_mapping.items() if field_mapping else []):
                        if idx < len(cols):
                            with cols[idx]:
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
                
                # Use BERT semantic entity-to-collection matching
                collection_name, match_confidence = match_entity_to_collection_bert(rank_entity, rank_by, metadata)

                if not collection_name:
                    collection_name = list(metadata.keys())[0]
                    match_confidence = 0.0
                
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
                from advanced_filter_detector_bert import detect_advanced_filters
                
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
                                        # Match: check if term is substring of any field value
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
            # SPECIAL PRIORITY RULE: If query has "transaction" + grouping keywords, force transaction_history
            query_lower_check = query.lower()
            grouping_keywords = ['group by', 'grouped by', 'group', 'grouped', 'categorize', 'category', 'breakdown']
            has_grouping = any(kw in query_lower_check for kw in grouping_keywords)
            has_transaction = 'transaction' in query_lower_check
            
            if has_transaction and has_grouping:
                # Priority boost: use transaction_history for transaction + grouping queries
                collection_name = 'transaction_history'
                coll_result = {
                    'selected_collection': 'transaction_history',
                    'confidence': 1.0,
                    'score': 999,
                    'reason': 'Priority rule: Transaction + Grouping query → transaction_history collection'
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
                full_data = load_collection_data(database_name, collection_name)
                
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
                grouping_data = result.get('results', {})
                
                if grouping_data and isinstance(grouping_data, dict):
                    results_df = pd.DataFrame([
                        {"Group": str(k), "Count": v}
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
                    from field_extractor_bert import extract_requested_fields, map_fields_to_columns
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
        else:
            st.info("No results returned")

st.markdown("---")
st.caption("Powered by 4-Part Intelligent Pipeline | Token-optimized (<2000 tokens per component)")

