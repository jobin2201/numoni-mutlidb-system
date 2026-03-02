#!/usr/bin/env python
"""
Part 2: Collection Router - Streamlit App
Shows which collection/table to use in the selected database
"""
import streamlit as st
import sys
import os
from pathlib import Path

# Add part1 to path - use absolute path
part1_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'part1_analysing_the_db'))
if part1_path not in sys.path:
    sys.path.insert(0, part1_path)

try:
    from db_keyword_router_fuzzy import detect_database
except ImportError as e:
    st.error(f"Import error: {e}. Path: {part1_path}")
    st.stop()

from collection_router import detect_collection

st.set_page_config(
    page_title="Part 2: Collection Router",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Part 2: Collection/Table Router")
st.markdown("**Intelligently detects which collection/table to query based on your question**")

# Input
user_query = st.text_input(
    "💬 Ask your question:",
    placeholder="e.g., Show customer wallet balance, merchant bank accounts, transaction history"
)

if user_query:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Step 1️⃣: Database Detection")
        
        # Database detection
        db_result = detect_database(user_query)
        selected_dbs = db_result.get("selected_dbs", [])
        
        if selected_dbs == ["unknown"]:
            st.error("❌ Could not determine database")
            st.caption(db_result.get("reason"))
        else:
            st.success(f"✅ Selected Database: **{', '.join(selected_dbs)}**")
            st.info(f"📌 Reason: {db_result.get('reason')}")
    
    with col2:
        st.subheader("Step 2️⃣: Collection Detection")
        
        if selected_dbs and selected_dbs != ["unknown"]:
            # For single database
            if len(selected_dbs) == 1:
                db_name = selected_dbs[0]
                
                if db_name in ["numoni_customer", "numoni_merchant", "authentication"]:
                    collection_result = detect_collection(user_query, db_name)
                    
                    if collection_result['selected_collection']:
                        st.success(f"📊 Collection: **{collection_result['selected_collection']}**")
                        
                        # Show details
                        confidence = collection_result['confidence']
                        if confidence >= 0.7:
                            confidence_color = "🟢"
                        elif confidence >= 0.4:
                            confidence_color = "🟡"
                        else:
                            confidence_color = "🔴"
                        
                        st.metric(
                            f"{confidence_color} Confidence",
                            f"{confidence}",
                            f"{collection_result.get('score', 0)} points"
                        )
                        
                        st.info(f"💡 {collection_result['reason']}")
                        
                        # Show matched fields
                        if collection_result.get('matched_fields'):
                            st.caption(f"🔑 **Key Fields**: {', '.join(collection_result['matched_fields'][:5])}")
                        
                        st.caption(f"📈 **Total Records**: {collection_result.get('total_records', 0):,}")
                    else:
                        st.warning("⚠️ No matching collection found")
            
            # For multiple databases
            else:
                for db_name in selected_dbs:
                    if db_name in ["numoni_customer", "numoni_merchant", "authentication"]:
                        st.markdown(f"### {db_name}")
                        
                        collection_result = detect_collection(user_query, db_name)
                        
                        if collection_result['selected_collection']:
                            st.success(f"📊 {collection_result['selected_collection']}")
                            st.caption(f"Confidence: {collection_result['confidence']}")
                            st.caption(f"Records: {collection_result.get('total_records', 0):,}")
        else:
            st.warning("⚠️ No database selected")
    
    # Show alternatives
    if selected_dbs and selected_dbs != ["unknown"] and len(selected_dbs) == 1:
        db_name = selected_dbs[0]
        
        if db_name in ["numoni_customer", "numoni_merchant", "authentication"]:
            collection_result = detect_collection(user_query, db_name)
            
            if collection_result.get('alternatives'):
                st.markdown("---")
                st.subheader("🔄 Alternative Collections")
                
                alt_cols = st.columns(3)
                for idx, alt in enumerate(collection_result['alternatives'][:3]):
                    with alt_cols[idx]:
                        st.metric(
                            alt['collection'],
                            f"{alt['confidence']:.2f}",
                            delta=f"{alt['score']:.0f} pts",
                            delta_color="off"
                        )

# Sidebar with examples
st.sidebar.markdown("## 📝 Example Queries")

st.sidebar.markdown("### Customer Queries")
examples_customer = [
    "Show all customer details",
    "Customer wallet balance",
    "Customer transaction history",
    "Customer locations",
    "Customer errors",
    "Load money records",
    "OTP verification"
]

for example in examples_customer:
    if st.sidebar.button(example, key=f"ex_cust_{example}"):
        st.query_params.update({"q": example})
        st.rerun()

st.sidebar.markdown("### Merchant Queries")
examples_merchant = [
    "Merchant business details",
    "Bank account information",
    "Merchant locations",
    "Deals and offers",
    "Merchant reviews",
    "POS terminals",
    "Payout records",
    "Merchant wallet"
]

for example in examples_merchant:
    if st.sidebar.button(example, key=f"ex_merch_{example}"):
        st.query_params.update({"q": example})
        st.rerun()

st.sidebar.markdown("### Authentication Queries")
examples_auth = [
    "Authentication login activities",
    "Signin records",
    "User sessions",
    "OTP verification",
    "Refresh token records",
    "Audit trail"
]

for example in examples_auth:
    if st.sidebar.button(example, key=f"ex_auth_{example}"):
        st.query_params.update({"q": example})
        st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### Stats")
st.sidebar.info("""
**Customer Collections**: 24  
**Merchant Collections**: 28  
**Authentication Collections**: 12  
**Total Collections**: 64  
**Metadata Tokens**: <2000  
**Empty Collections**: 6  
""")
