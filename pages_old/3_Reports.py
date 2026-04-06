import streamlit as st
import pandas as pd
import io

import db

st.set_page_config(page_title="Analytics & Export", layout="wide")

st.title("📊 Financial & Operational Reports")
st.markdown("Filter, generate, and download analytical data. Specifically designed for MCF & Courier analysis.")

# Filters
col1, col2, col3 = st.columns(3)

status_filter = col1.selectbox("By Order Status", ["Any", "NEW", "PROCESSING", "SHIPPED", "DELIVERED", "FAILED"])
channel_filter = col2.selectbox("By Origin Channel", ["Any", "SHOPIFY", "AMAZON"])
fulfillment_filter = col3.selectbox("By Fulfillment Mode", ["Any", "MCF", "DELHIVERY"])

# Fetch
raw_orders = db.get_orders()
df = pd.DataFrame(raw_orders)

if not df.empty:
    # Apply Filters
    if status_filter != "Any":
        df = df[df['status'] == status_filter]
    if channel_filter != "Any":
        df = df[df['source_channel'] == channel_filter]
    if fulfillment_filter != "Any":
        df = df[df['fulfillment_channel'] == fulfillment_filter]

    st.markdown(f"**Found {len(df)} matching records.**")

    # Master View
    st.dataframe(df, use_container_width=True)

    # Exports
    st.subheader("📥 Export Downloads")
    
    # 1. Non-Amazon Export Request (from Master Prompt special requirement)
    non_amazon_df = df[df['source_channel'] != 'AMAZON']
    mcf_df = df[df['fulfillment_channel'] == 'MCF']
    
    csv_buffer_all = df.to_csv(index=False).encode('utf-8')
    csv_buffer_non_amazon = non_amazon_df.to_csv(index=False).encode('utf-8')
    csv_buffer_mcf = mcf_df.to_csv(index=False).encode('utf-8')
    
    ex_c1, ex_c2, ex_c3 = st.columns(3)
    
    ex_c1.download_button(
        label="Download Filtered Report (CSV)",
        data=csv_buffer_all,
        file_name='filtered_orders_report.csv',
        mime='text/csv'
    )
    
    ex_c2.download_button(
        label="Download Non-Amazon Sales",
        data=csv_buffer_non_amazon,
        file_name='non_amazon_report.csv',
        mime='text/csv'
    )
    
    ex_c3.download_button(
        label="Download MCF Tracking Audit",
        data=csv_buffer_mcf,
        file_name='mcf_fulfillment_audit.csv',
        mime='text/csv'
    )
    
    # Stats Summary
    st.markdown("---")
    st.subheader("📈 Quick Statistics (Based on Filter)")
    st.write(f"- Total Operational Value: ₹{df['total_amount'].sum() if 'total_amount' in df.columns else 0:,.2f}")
    
    if "is_cod" in df.columns:
        cod_count = len(df[df['is_cod'] == 1])
        st.write(f"- COD Percentage: { (cod_count/len(df))*100 if len(df) > 0 else 0:.1f}%")

else:
    st.info("No data in Database to run reports on.")
