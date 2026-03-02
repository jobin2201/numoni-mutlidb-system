import os
import sys
import streamlit as st

# Import router
sys.path.append(os.path.dirname(__file__))
from db_keyword_router_fuzzy import detect_database

st.set_page_config(page_title="Numoni DB Router Chatbot", page_icon="🤖", layout="centered")

st.title("🤖 Numoni DB Router Chatbot")
st.write("Type anything. This app will decide which DB should be used (Customer / Merchant / Both).")

st.markdown("---")

query = st.text_area("💬 Ask your question:", placeholder="Example: total transaction of Chicken Republic")

if st.button("Submit"):
    if query.strip() == "":
        st.warning("Please type something.")
    else:
        result = detect_database(query)

        st.subheader("✅ Selected Database")
        st.success(result["selected_dbs"])

        st.subheader("📌 Reason")
        st.info(result["reason"])
