#!/usr/bin/env python
"""
Streamlit Application - Complete 3-Part Pipeline
Shows: Database Detection → Collection Detection → Action Execution with Results
"""
import streamlit as st
import sys
import os
from pathlib import Path
import json

# Add paths
BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR / "part1_analysing_the_db" / "bert"))
sys.path.insert(0, str(BASE_DIR / "part2_analysing_the_collection" / "bert"))
sys.path.insert(0, str(BASE_DIR / "part3_analysing_the_action" / "bert"))

from db_keyword_router_bert import detect_database
from collection_router_bert import detect_collection
from action_detector_bert import detect_action
from action_executor_bert import execute_action

# Page config
st.set_page_config(
    page_title="Numoni Query System - 3-Part Pipeline",
    page_icon="🎯",
    layout="wide"
)

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
    .result-box {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 5px solid #667eea;
        margin: 1rem 0;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }
    .alternative-box {
        background-color: #fff9e6;
        padding: 1rem;
        border-radius: 8px;
        border-left: 3px solid #ffc107;
        margin-top: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<div class="main-header">🎯 Numoni Query System</div>', unsafe_allow_html=True)
st.markdown("**3-Part Intelligent Pipeline:** Database → Collection → Action")

# Example queries
with st.expander("💡 Example Queries"):
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("""
        **Counting:**
        - How many customers have errors?
        - Count transactions by status
        - How many merchants in Lagos?
        
        **Listing:**
        - Show all customer details
        - List top 10 transactions
        - Display pending payments
        """)
    with col2:
        st.markdown("""
        **Aggregations:**
        - Total transaction amount
        - Average wallet balance
        - Highest load money amount
        
        **Filtering:**
        - Customers with share money transactions
        - Successful payments last month
        - Merchants with reviews
        """)
    with col3:
        st.markdown("""
        **Authentication:**
        - Show user authentication details
        - List login activities
        - Show user sessions
        - List OTP records
        - Show audit trail
        - Show account deletion requests
        """)

# Query input
st.markdown("---")
query = st.text_input(
    "🔍 Enter your query:",
    placeholder="e.g., How many customers have share money type transactions?",
    help="Ask any question about customers, merchants or authentication"
)

if st.button("🚀 Execute Query", type="primary") or query:
    
    if not query:
        st.warning("Please enter a query")
    else:
        
        # Initialize results container
        results_container = st.container()
        
        with results_container:
            # PART 1: Database Detection
            st.markdown('<div class="part-header">📊 PART 1: Database Detection</div>', unsafe_allow_html=True)
            
            with st.spinner("Detecting database..."):
                db_result = detect_database(query)
            
            # Handle both old and new formats
            selected_db = db_result.get('selected_database') or db_result.get('selected_dbs', ['unknown'])[0]
            confidence = db_result.get('confidence', 0.5)
            score = db_result.get('score', 0)
            reason = db_result.get('reason', 'Keyword matching')
            
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                db_name = selected_db.replace('numoni_', '').upper()
                st.success(f"**Selected Database:** {db_name}")
            
            with col2:
                st.metric("Confidence", f"{confidence:.0%}")
            
            with col3:
                st.metric("Score", f"{score:.1f}")
            
            with st.expander("🔍 Database Detection Details"):
                st.info(f"**Reason:** {reason}")
                if db_result.get('alternatives'):
                    st.markdown("**Alternatives:**")
                    for alt in db_result['alternatives']:
                        st.text(f"  • {alt['database']}: {alt['confidence']:.1%}")
            
            # PART 2: Collection Detection
            st.markdown('<div class="part-header">📋 PART 2: Collection Detection</div>', unsafe_allow_html=True)
            
            with st.spinner("Detecting collection..."):
                collection_result = detect_collection(query, selected_db)
            
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                st.success(f"**Selected Collection:** {collection_result['selected_collection']}")
            
            with col2:
                st.metric("Confidence", f"{collection_result['confidence']:.0%}")
            
            with col3:
                st.metric("Score", f"{collection_result.get('score', 0):.1f}")
            
            with st.expander("🔍 Collection Detection Details"):
                st.info(f"**Reason:** {collection_result['reason']}")
                
                if collection_result.get('matched_fields'):
                    st.markdown(f"**Matched Fields:** {', '.join(collection_result['matched_fields'][:5])}")
                
                if collection_result.get('matched_values'):
                    st.markdown(f"**Matched Values:** {', '.join(collection_result['matched_values'][:3])}")
                
                alternatives = collection_result.get('alternatives', [])
                if alternatives:
                    st.markdown("**Alternatives:**")
                    for i, alt in enumerate(alternatives[:3], 1):
                        st.text(f"  {i}. {alt['collection']}: {alt['score']:.1f} pts (confidence: {alt['confidence']:.0%})")
            
            # PART 3: Action Detection & Execution
            st.markdown('<div class="part-header">⚡ PART 3: Action Detection & Execution</div>', unsafe_allow_html=True)
            
            with st.spinner("Detecting action..."):
                action_metadata = detect_action(query)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.info(f"**Action:** {action_metadata['primary_action'].upper()}")
            
            with col2:
                if action_metadata.get('aggregation'):
                    st.info(f"**Aggregation:** {action_metadata['aggregation']}")
                elif action_metadata.get('limit'):
                    st.info(f"**Limit:** {action_metadata['limit']} records")
                else:
                    st.info("**Type:** Query")
            
            with col3:
                filters_count = len(action_metadata.get('filters', {}))
                st.info(f"**Filters:** {filters_count}")
            
            with st.expander("🔍 Action Metadata"):
                st.json(action_metadata, expanded=False)
            
            # Execute Action
            st.markdown("---")
            with st.spinner("Executing action on database..."):
                # Check if alternatives have similar scores
                primary_score = collection_result.get('score', 0)
                close_alternatives = []
                
                for alt in alternatives[:2]:
                    if alt['score'] >= primary_score * 0.9:  # Within 90%
                        close_alternatives.append(alt)
                
                execution_result = execute_action(
                    action_metadata,
                    selected_db,
                    collection_result['selected_collection'],
                    close_alternatives if close_alternatives else None
                )
            
            # Display Results
            st.markdown("### 📊 RESULTS")
            
            if execution_result.get('error'):
                st.error(execution_result['error'])
            else:
                st.success(execution_result.get('summary', 'Query executed successfully'))
                
                result = execution_result.get('result')
                action = action_metadata['primary_action']
                
                # Display based on action type
                if action == 'count':
                    if isinstance(result, dict) and 'count' in result:
                        st.markdown(f"### 🔢 Total Count: **{result['count']:,}**")
                    
                    elif isinstance(result, dict) and 'results' in result:
                        st.markdown("### 📊 Grouped Results")
                        
                        # Create bar chart data
                        import pandas as pd
                        df = pd.DataFrame([
                            {"Group": k, "Count": v} 
                            for k, v in sorted(result['results'].items(), key=lambda x: x[1], reverse=True)
                        ])
                        
                        col1, col2 = st.columns([1, 2])
                        with col1:
                            st.dataframe(df, hide_index=True, use_container_width=True)
                        with col2:
                            st.bar_chart(df.set_index("Group"))
                
                elif action == 'list':
                    if isinstance(result, list):
                        st.markdown(f"### 📋 Records ({len(result)} found)")
                        
                        import pandas as pd
                        df = pd.DataFrame(result)
                        
                        # Limit columns for display
                        if len(df.columns) > 10:
                            important_cols = ['customerId', 'merchantId', 'name', 'amount', 'status', 'type', 'date']
                            display_cols = [c for c in important_cols if c in df.columns]
                            if not display_cols:
                                display_cols = list(df.columns)[:10]
                            df = df[display_cols]
                        
                        st.dataframe(df, use_container_width=True, height=400)
                        
                        # Download button
                        csv = df.to_csv(index=False)
                        st.download_button(
                            "📥 Download CSV",
                            csv,
                            "results.csv",
                            "text/csv"
                        )
                
                elif action in ['sum', 'average']:
                    results_key = 'sums' if action == 'sum' else 'averages'
                    if isinstance(result, dict) and results_key in result:
                        st.markdown(f"### 📈 {action.upper()} Results")
                        
                        cols = st.columns(min(len(result[results_key]), 4))
                        for i, (field, value) in enumerate(result[results_key].items()):
                            with cols[i % 4]:
                                st.metric(field, f"{value:,.2f}")
                
                elif action in ['max', 'min']:
                    st.markdown(f"### 🎯 {action.upper()} Results")
                    
                    for field, data in result.items():
                        if isinstance(data, dict):
                            value_key = 'max' if action == 'max' else 'min'
                            if value_key in data:
                                st.markdown(f"**{field}:** {data[value_key]:,.2f}")
                                
                                with st.expander(f"View {field} record"):
                                    st.json(data.get('record', {}))
                
                elif action in ['top_n', 'bottom_n']:
                    if isinstance(result, list):
                        st.markdown(f"### 🏆 {action.upper().replace('_', ' ').title()} Results")
                        
                        import pandas as pd
                        df = pd.DataFrame(result)
                        st.dataframe(df, use_container_width=True)
                
                # Display alternative results
                if execution_result.get('alternative_results'):
                    st.markdown("---")
                    st.markdown("### 📊 Alternative Results (Similar Score Collections)")
                    
                    for alt in execution_result['alternative_results']:
                        with st.expander(f"📁 {alt['collection']}"):
                            alt_result = alt['result']
                            
                            if isinstance(alt_result, dict) and 'count' in alt_result:
                                st.metric("Count", f"{alt_result['count']:,}")
                            elif isinstance(alt_result, list):
                                st.write(f"**Records:** {len(alt_result)}")
                                import pandas as pd
                                st.dataframe(pd.DataFrame(alt_result).head(10), use_container_width=True)
                            else:
                                st.json(alt_result)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 2rem;'>
    <b>Numoni Query System</b> | 3-Part Intelligent Pipeline<br>
    Database Detection → Collection Detection → Action Execution
</div>
""", unsafe_allow_html=True)
