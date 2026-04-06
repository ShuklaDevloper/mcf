import streamlit as st
import pandas as pd
import db

st.set_page_config(page_title="Order History & Tracking", layout="wide")

st.title("🗂️ Global Order History & Automation Status")
st.markdown("Delhivery-One Style Master View of all processed, synced, and failed orders across Amazon MCF and Delhivery.")

# Fetch DB
orders = db.get_orders()

if not orders:
    st.info("No orders found in local database. Fetch orders in the 'Order Processing' tab first.")
else:
    # Build Display Data
    df = pd.DataFrame(orders)
    
    # Filter by Status
    status_filter = st.selectbox("Filter Status", ["ALL", "NEW", "PROCESSING", "SHIPPED", "DELIVERED", "FAILED"])
    
    if status_filter != "ALL":
        df = df[df["status"] == status_filter]

    if len(df) == 0:
        st.warning("No orders matching this status.")
    else:
        # Display Columns
        display_df = df[[
            "order_id", "status", "fulfillment_channel", 
            "tracking_number", "tracking_company", "updated_at", "total_amount"
        ]].copy()
        
        display_df.rename(columns={
            "order_id": "Order Number",
            "status": "Current Status",
            "fulfillment_channel": "Route",
            "tracking_number": "AWB / Tracking ID",
            "tracking_company": "Carrier",
            "updated_at": "Last Updated",
            "total_amount": "Order Value (₹)"
        }, inplace=True)
        
        # Color coding map isn't natively supported in base df display so we just show regular dataframe
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Drawer / Detailed Timeline Simulator
    st.markdown("---")
    st.subheader("🔍 Order Audit Trail (Timeline)")
    
    select_order = st.selectbox("Select an Order to View History Timeline", df['order_id'].tolist() if not df.empty else [])
    
    if select_order:
        history = db.get_order_history(select_order)
        if not history:
            st.info("No audit logs found for this order.")
        else:
            for event in history:
                icon = "🟢" if "SHIPPED" in str(event['new_status']) else ("🟡" if "PROCESSING" in str(event['new_status']) else "📝")
                msg = f"**{event['timestamp']}**: Changed from `{event['old_status']}` to `{event['new_status']}`"
                reason = f" > *By {event['actor']}: {event['reason']}*"
                st.markdown(f"{icon} {msg} \n{reason}")
