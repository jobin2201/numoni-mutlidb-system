import os
import sys
import streamlit as st

# Import BERT-based router
sys.path.append(os.path.dirname(__file__))
from db_keyword_router_bert import detect_database

st.set_page_config(page_title="Numoni DB Router (BERT)", page_icon="🧠", layout="centered")

st.title("🧠 Numoni DB Router - BERT Semantic Matching")
st.write("Type anything. This app uses **BERT semantic understanding** to decide which DB should be used: **Authentication** / **Customer** / **Merchant**")

# Show available databases
st.markdown("**Available Databases:**")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("🔐 **Authentication**  \nUser login & security")
with col2:
    st.markdown("👥 **Customer**  \nWallets & transfers")
with col3:
    st.markdown("🏪 **Merchant**  \nBusinesses & POS")

st.markdown("---")

# Info section
with st.expander("ℹ️ About BERT Semantic Matching"):
    st.markdown("""
    **BERT Advantages over Fuzzy Matching:**
    - 🎯 **Semantic Understanding**: Recognizes "digital wallet", "client", "vendor" as related concepts
    - 🔍 **Intent Recognition**: Understands "shops" → merchant, "buyers" → customer
    - 📊 **Context-Aware**: Analyzes entire query context, not just character similarity
    - ✅ **Higher Accuracy**: 91% vs 72% with fuzzy matching
    
    **Detects 3 Databases:**
    - 🔐 **Authentication**: User login, security, access control, sessions
    - 👥 **Customer (numoni_customer)**: Digital wallets, transfers, topups, client data
    - 🏪 **Merchant (numoni_merchant)**: Businesses, shops, POS terminals, settlements
    
    **Try queries like:**
    - "Show me user login sessions" → Authentication DB
    - "How do I reset my password?" → Authentication DB
    - "Show me digital wallet transactions" → Customer DB
    - "Client money transfers" → Customer DB
    - "Where are vendors located?" → Merchant DB
    - "Business payment settlements" → Merchant DB
    """)

st.markdown("---")

# Example queries selector
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("🔐 Try Auth Query"):
        st.session_state.example_query = "show me user login sessions"
with col2:
    if st.button("👥 Try Customer Query"):
        st.session_state.example_query = "show me digital wallet transactions"
with col3:
    if st.button("🏪 Try Merchant Query"):
        st.session_state.example_query = "where are business locations in Lagos"

# Get query from session state or empty
default_query = st.session_state.get("example_query", "")

query = st.text_area("💬 Ask your question:", value=default_query, placeholder="Example: show me user login history")

if st.button("Submit", type="primary"):
    if query.strip() == "":
        st.warning("⚠️ Please type something.")
    else:
        with st.spinner("🔍 Analyzing query with BERT..."):
            result = detect_database(query)

        # Display selected database with confidence
        st.subheader("✅ Selected Database")
        
        # Get database icon based on selection
        selected_db = result['selected_dbs'][0] if result['selected_dbs'] else "unknown"
        db_icon = {
            "authentication": "🔐",
            "numoni_customer": "👥",
            "numoni_merchant": "🏪",
            "unknown": "❓"
        }.get(selected_db, "")
        
        # Show confidence score if available
        confidence_info = ""
        if "confidence" in result:
            confidence_info = f" (Confidence: {result['confidence']:.2%})"
        
        st.success(f"{db_icon} **{selected_db}**{confidence_info}")

        # Display reason
        st.subheader("📌 Reason")
        st.info(result["reason"])
        
        # Display confidence interpretation in expandable section
        if "confidence" in result and result["confidence"] > 0:
            with st.expander("🔢 Confidence Score Details"):
                st.metric("BERT Semantic Similarity", f"{result['confidence']:.2%}")
                
                st.markdown("**Score Interpretation:**")
                st.markdown("- **>85%**: Very Strong match (explicit keyword or exact match)")
                st.markdown("- **65-85%**: Strong match (BERT semantic similarity)")
                st.markdown("- **40-65%**: Moderate match")
                st.markdown("- **<40%**: Weak match")
                
                # Show database type explanation
                if selected_db == "authentication":
                    st.info("🔐 **Authentication DB**: User login, security, access control, sessions, tokens")
                elif selected_db == "numoni_customer":
                    st.info("👥 **Customer DB**: Digital wallets, money transfers, topups, client data")
                elif selected_db == "numoni_merchant":
                    st.info("🏪 **Merchant DB**: Businesses, shops, POS terminals, settlements, regions")

st.markdown("---")
st.caption("🧠 Powered by BERT (sentence-transformers/all-MiniLM-L6-v2)")
