import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta

from utils import (
    read_secret, get_access_token, clean_phone_number, validate_address,
    validate_pincode, init_sheets_service, update_sheet_status, update_sheet_tracking,
    get_shopify_config, get_shopify_order, fulfill_order,
    create_mcf_order, create_delhivery_order, APPS_SCRIPT_URL, SHEET_ID
)
import db
import os

st.set_page_config(page_title="Order Processing", layout="wide")

secrets = db.read_secret() if hasattr(db, "read_secret") else read_secret()

st.title("Order Processing Workflow")
st.markdown("Pull new orders, validate details, and dispatch to Amazon MCF or Delhivery.")

# =========================
# Sidebar
# =========================
st.sidebar.title("Settings")
pickup_location = st.sidebar.text_input("Delhivery Pickup Location", "emaar")
sheet_url = st.sidebar.text_input("Raw Order Source", value=APPS_SCRIPT_URL)

has_amazon = bool(secrets.get("IN_LWA_REFRESH_TOKEN"))
has_delhivery = bool(secrets.get("DELHIVERY_API_KEY"))

# =========================
# Session State
# =========================
if "orders_df" not in st.session_state:
    st.session_state.orders_df = None
if "processing_log" not in st.session_state:
    st.session_state.processing_log = []
if "token" not in st.session_state:
    st.session_state.token = None
    st.session_state.token_time = None

def get_fresh_token():
    now = datetime.now()
    if (st.session_state.token and st.session_state.token_time
            and now - st.session_state.token_time < timedelta(minutes=50)):
        return st.session_state.token, None
    token, err = get_access_token(secrets)
    if token:
        st.session_state.token = token
        st.session_state.token_time = now
    return token, err

def fetch_orders(url):
    resp = requests.get(url)
    data = resp.json()
    if not data.get("success") or not data.get("orders"):
        return None, "No orders found or endpoint error"
    
    orders = data["orders"]
    rows = []
    
    # Pre-save to DB to ensure audit trail exists & capture NEW orders systematically
    for o in orders:
        status_raw = str(o.get("status", "")).lower()
        fulfilled = str(o.get("fulfilled", "")).strip()
        
        phone = clean_phone_number(o.get("phone", ""))
        full_address = f"{o.get('address1', '')} {o.get('address2', '')}".strip()
        addr1, addr2, addr3, addr_valid = validate_address(full_address)
        pin_valid = validate_pincode(o.get("pincode", ""))
        is_valid = addr_valid and pin_valid and len(phone) == 10
        
        order_id = str(o.get("ord_serial", "")).replace("#", "").strip()
        
        # Build normalized order
        order_data = {
            "order_id": order_id,
            "date": o.get("date", ""),
            "customer": o.get("customer", ""),
            "phone": phone,
            "amount": o.get("amount", 0),
            "address1_raw": o.get("address1", ""),
            "address2_raw": o.get("address2", ""),
            "addr_line1": addr1,
            "addr_line2": addr2,
            "addr_line3": addr3,
            "pincode": str(o.get("pincode", "")),
            "state_code": o.get("state_code", ""),
            "city": o.get("city", ""),
            "is_cod": str(o.get("is_cod", "")),
            "seller_sku": o.get("seller_sku", ""),
            "title": o.get("title", ""),
            "qty": int(o.get("qty", 1) or 1),
            "items": [{
                "seller_sku": o.get("seller_sku", ""),
                "title": o.get("title", ""),
                "quantity": int(o.get("qty", 1) or 1),
                "price": o.get("amount", 0)
            }]
        }
        
        if fulfilled or status_raw not in ["pending", ""]:
            # Even if skipped, ensure we have an initial record in DB if not there
            db.save_order(order_data)
            continue
            
        # Write to DB as NEW
        order_data["row_number"] = o.get("row_number", 0)  # We added row_number
        db.save_order(order_data)

    # Return fetched count internally
    return True, None

def load_pending_orders_to_df():
    """Loads all NEW orders directly from local DB."""
    pending = db.get_orders({"status": "NEW"})
    if not pending:
        return None
        
    rows = []
    for p in pending:
        phone = str(p['customer_phone'])
        valid_phone = len(phone) == 10
        valid_addr = len(str(p['address_line1']).strip()) > 0
        
        # Load item details (simplifying to just grab main sku/title if exists)
        conn = db.get_connection()
        c = conn.cursor()
        c.execute("SELECT seller_sku, title, quantity FROM order_items WHERE order_id=?", (p['order_id'],))
        item = c.fetchone()
        conn.close()
        
        sku = item['seller_sku'] if item else ""
        title = item['title'] if item else "Unknown"
        qty = item['quantity'] if item else 1
        
        # Get row number if available from DB
        # To avoid schema changes breaking, default to 0
        row_num = p.get('row_number', 0)
        
        rows.append({
            "select": True,
            "row_number": row_num,
            "order_id": p['order_id'],
            "customer": p['customer_name'],
            "phone": phone,
            "amount": p['total_amount'],
            "addr_line1": p['address_line1'],
            "addr_line2": p['address_line2'],
            "addr_line3": "",
            "pincode": p['pincode'],
            "state_code": p['state_code'],
            "city": p['city'],
            "is_cod": "COD" if p['is_cod'] else "Prepaid",
            "seller_sku": sku,
            "title": title,
            "qty": qty,
            "address_valid": valid_phone and valid_addr,
            "validation_issue": ("Failed Auth" if not valid_phone else ""),
            "path": "MCF",
            "date": p['created_at']
        })
        
    return pd.DataFrame(rows)

# Auto-Load from DB on script rerun
st.session_state.orders_df = load_pending_orders_to_df()

colA, colB = st.columns([1, 4])
if colA.button("Sync Now (Pull From Source)", key="refresh_orders_btn"):
    with st.spinner("Fetching orders from Source & syncing to Internal DB..."):
        success, err = fetch_orders(sheet_url)
        if err:
            st.error(err)
        else:
            st.session_state.orders_df = load_pending_orders_to_df()
            st.session_state.processing_log = []
            st.success(f"Successfully Synced. Loaded {len(st.session_state.orders_df) if st.session_state.orders_df is not None else 0} pending orders.")

if st.session_state.orders_df is not None:
    df = st.session_state.orders_df
    st.markdown("---")
    
    display_cols = [
        "select", "order_id", "customer", "phone", "address_valid",
        "validation_issue", "addr_line1", "addr_line2", "addr_line3",
        "city", "state_code", "pincode", "amount", "is_cod", "path"
    ]
    
    edited_df = st.data_editor(
        df[display_cols],
        column_config={
            "select": st.column_config.CheckboxColumn("Select", default=True),
            "order_id": st.column_config.TextColumn("Order ID", disabled=True),
            "customer": st.column_config.TextColumn("Customer"),
            "address_valid": st.column_config.CheckboxColumn("Valid", disabled=True),
            "path": st.column_config.SelectboxColumn("Dispatch To", options=["MCF", "Delhivery"]),
        },
        hide_index=True,
        use_container_width=True,
        num_rows="fixed"
    )
    
    # Sync edits back
    df["select"] = edited_df["select"]
    df["path"] = edited_df["path"]
    df["addr_line1"] = edited_df["addr_line1"]
    df["addr_line2"] = edited_df["addr_line2"]
    df["addr_line3"] = edited_df["addr_line3"]
    df["phone"] = edited_df["phone"]
    df["pincode"] = edited_df["pincode"]
    
    st.session_state.orders_df = df
    
    st.markdown("---")
    selected = df[df["select"] == True]
    
    if st.button("🚀 Process Selected Orders", type="primary"):
        if len(selected) == 0:
            st.warning("No orders selected for processing.")
        else:
            log = []
            sheet_updates = []
            tracking_updates = []
            shopify_cfg = get_shopify_config(secrets)
            sheets_service = init_sheets_service()

            progress = st.progress(0)
            status_text = st.empty()
            total = len(selected)
            
            mcf_orders = selected[selected["path"] == "MCF"]
            del_orders = selected[selected["path"] == "Delhivery"]

            # ========================
            # PROCESS MCF
            # ========================
            if len(mcf_orders) > 0:
                token, token_err = get_fresh_token()
                if token_err:
                    st.error(f"Amazon Auth Failed: {token_err}")
                else:
                    for i, (idx, row) in enumerate(mcf_orders.iterrows()):
                        status_text.text(f"MCF: Submitting Order {row['order_id']}...")
                        items = [{
                            "sellerSku": row["seller_sku"],
                            "sellerFulfillmentOrderItemId": f"{row['seller_sku']}-{row['order_id']}-{row['qty']}",
                            "quantity": row["qty"],
                            "perUnitDeclaredValue": {"currencyCode": "INR", "value": str(row["amount"])}
                        }]
                        order_data = row.to_dict()
                        order_data['items'] = items

                        success, err = create_mcf_order(token, order_data)

                        if success:
                            # 1) Log in UI
                            log.append({"order_id": row["order_id"], "path": "MCF", "status": "success", "msg": err or "Planning"})
                            # 2) Sync to Database (Updates status to PROCESSING, logs timeline)
                            db.update_order_status(row["order_id"], "PROCESSING", "USER", "Submitted to Amazon SP-API", "MCF")
                            # 3) Prep Sheet status update
                            sheet_updates.append({"row": row["row_number"], "source": "MCF", "status": "Fulfilled"})
                        else:
                            log.append({"order_id": row["order_id"], "path": "MCF", "status": "error", "msg": err})
                            db.update_order_status(row["order_id"], "FAILED", "SYSTEM", f"Amazon API Error: {err[:50]}")
                            sheet_updates.append({"row": row["row_number"], "source": "MCF", "status": f"Error: {err[:50]}"})
                        
                        progress.progress((i + 1) / total)
                        
            # ========================
            # PROCESS DELHIVERY
            # ========================
            if len(del_orders) > 0:
                del_key = secrets.get("DELHIVERY_API_KEY", "")
                if not del_key:
                    st.error("DELHIVERY_API_KEY missing in secret.txt")
                else:
                    mcf_count = len(mcf_orders)
                    for i, (idx, row) in enumerate(del_orders.iterrows()):
                        status_text.text(f"Delhivery: Creating order {row['order_id']}...")
                        order_data = row.to_dict()
                        order_data["total_qty"] = row["qty"]

                        success, resp_data, err = create_delhivery_order(del_key, order_data, pickup_location)

                        if success:
                            log.append({"order_id": row["order_id"], "path": "Delhivery", "status": "success", "msg": "Waybill Generated"})
                            sheet_updates.append({"row": row["row_number"], "source": "Delhivery", "status": "Fulfilled"})
                            
                            # Get tracking immediately
                            packages = resp_data.get("packages", [])
                            t_info = None
                            if packages and packages[0].get("waybill"):
                                waybill = packages[0]["waybill"]
                                t_info = {"company": "Delhivery", "number": str(waybill), "url": f"https://www.delhivery.com/tracking?id={waybill}"}
                                
                                # Google Sheet Tracker Update
                                tracking_updates.append({
                                    "row": row["row_number"],
                                    "tracking_company": t_info["company"],
                                    "tracking_id": t_info["number"],
                                    "tracking_url": t_info["url"]
                                })
                                
                            # Update DB directly to SHIPPED
                            db.update_order_status(row["order_id"], "PROCESSING", "USER", "Pushed to Delhivery", "DELHIVERY")
                            if t_info:
                                db.update_order_tracking(row["order_id"], t_info["company"], t_info["number"], t_info["url"])

                            # Auto Fulfill Shopify for Delhivery
                            if shopify_cfg["shop_url"]:
                                try:
                                    s_order = get_shopify_order(row["order_id"], shopify_cfg["headers"], shopify_cfg["shop_url"])
                                    if s_order:
                                        fulfill_order(s_order, shopify_cfg["headers"], shopify_cfg["shop_url"], tracking_info=t_info)
                                except Exception as e:
                                    db.log_audit(row["order_id"], "SHIPPED", "SHIPPED", "SYSTEM", f"Shopify API Error: {str(e)}")
                        else:
                            log.append({"order_id": row["order_id"], "path": "Delhivery", "status": "error", "msg": err})
                            db.update_order_status(row["order_id"], "FAILED", "SYSTEM", f"Delhivery API Error: {str(err)[:50]}")
                            sheet_updates.append({"row": row["row_number"], "source": "Delhivery", "status": f"Error: {str(err)[:50]}"})
                            
                        progress.progress((mcf_count + i + 1) / total)

            # --- Update Google Sheet (Legacy Backup) ---
            if sheet_updates:
                status_text.text("Updating internal Google Sheet logs...")
                try: 
                    update_sheet_status(sheets_service, SHEET_ID, sheet_updates) 
                except: pass
                try: 
                    if tracking_updates: update_sheet_tracking(sheets_service, SHEET_ID, tracking_updates) 
                except: pass

            status_text.text("Processing Batch Complete!")
            progress.progress(1.0)
            
            # --- Render Log ---
            st.session_state.processing_log = log
            st.markdown("### Action Logs")
            for entry in log:
                if entry["status"] == "success":
                    st.success(f"{entry['order_id']} dispatched via {entry['path']} | {entry['msg']}")
                else:
                    st.error(f"{entry['order_id']} failed on {entry['path']} | {entry['msg']}")
