"""
app.py — Order Fulfillment Dashboard (Delhivery One Style)
Multi-channel OMS: Amazon MCF + Delhivery + Shopify + Google Sheet sync

Run: streamlit run app.py
"""
import io
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
import streamlit as st

import db
from utils import (
    APPS_SCRIPT_URL,
    SHEET_ID,
    clean_phone_number,
    create_delhivery_order,
    create_mcf_order,
    fulfill_order,
    get_access_token,
    get_delhivery_tracking,
    get_shopify_config,
    get_shopify_order,
    init_sheets_service,
    parse_date,
    read_secret,
    update_sheet_remarks,
    update_sheet_tracking,
    validate_address,
    validate_pincode,
)
from w import fetch_mcf_data
from live_tracker import run_live_tracking_update

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Order Fulfillment Dashboard",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# SESSION STATE INIT
# ─────────────────────────────────────────────
def ss(key, default):
    if key not in st.session_state:
        st.session_state[key] = default

ss("secrets", read_secret())
ss("token", None)
ss("token_time", None)
ss("pending_df", None)
ss("processed_df", None)
ss("processing_log", [])
ss("page", "Dashboard")

secrets = st.session_state.secrets


def get_fresh_token():
    now = datetime.now()
    if (
        st.session_state.token
        and st.session_state.token_time
        and now - st.session_state.token_time < timedelta(minutes=50)
    ):
        return st.session_state.token, None
    token, err = get_access_token(secrets)
    if token:
        st.session_state.token = token
        st.session_state.token_time = now
    return token, err


# ─────────────────────────────────────────────
# SIDEBAR NAVIGATION
# ─────────────────────────────────────────────
PAGES = {
    "📊 Dashboard": "Dashboard",
    "📋 Orders": "Orders",
    "🚚 Tracking": "Tracking",
    "📈 Reports": "Reports",
    "🔄 Sync": "Sync",
}

with st.sidebar:
    st.markdown("## 📦 Fulfillment OMS")
    st.markdown("---")
    for label, page_id in PAGES.items():
        if st.button(label, use_container_width=True, key=f"nav_{page_id}"):
            st.session_state.page = page_id

    st.markdown("---")
    st.subheader("⚙️ Settings")
    pickup_loc = st.text_input("Delhivery Pickup", value="emaar", key="pickup_loc")

    st.markdown("---")
    st.subheader("🔌 Status")
    st.markdown(f"Amazon: {'🟢' if secrets.get('IN_LWA_REFRESH_TOKEN') else '🔴'}")
    st.markdown(f"Delhivery: {'🟢' if secrets.get('DELHIVERY_API_KEY') else '🔴'}")
    st.markdown(f"Shopify: {'🟢' if secrets.get('shop_url') else '🔴'}")


# ─────────────────────────────────────────────
# HELPER: Fetch from Apps Script Endpoint
# ─────────────────────────────────────────────
def fetch_endpoint_orders():
    """Fetch all orders from Apps Script. Returns (pending_list, processed_list, error)."""
    try:
        resp = requests.get(APPS_SCRIPT_URL, timeout=30)
        data = resp.json()
    except Exception as e:
        return [], [], f"Endpoint error: {e}"

    if not data.get("success"):
        return [], [], "Endpoint returned success=false"

    pending, processed = [], []
    try:
        repeat_phones = db.get_all_phones()
    except Exception:
        repeat_phones = set()

    for o in data.get("orders", []):
        source = str(o.get("source", "")).strip()
        fulfilled = str(o.get("fulfilled", "")).strip()
        order_id = str(o.get("ord_serial", "")).replace("#", "").strip()
        if not order_id:
            continue

        phone = clean_phone_number(o.get("phone", ""))
        full_addr = f"{o.get('address1', '')} {o.get('address2', '')}".strip()
        addr1, addr2, addr3, addr_valid = validate_address(full_addr)
        pin_valid = validate_pincode(o.get("pincode", ""))
        phone_valid = len(phone) == 10

        is_valid = addr_valid and pin_valid and phone_valid
        issue = (
            "Address overflow" if not addr_valid
            else "Invalid pincode" if not pin_valid
            else "Invalid phone" if not phone_valid
            else ""
        )

        raw_qty = str(o.get("qty", "1")).strip()
        seller_sku = str(o.get("seller_sku", "")).strip()
        
        is_multi = False
        if "," in raw_qty or "," in seller_sku:
            is_multi = True
        elif raw_qty.isdigit() and int(raw_qty) > 1:
            is_multi = True
            
        is_repeat = phone in repeat_phones

        row = {
            "row_number": int(o.get("row_number", 0) or 0),
            "order_id": order_id,
            "date": o.get("date", ""),
            "customer": o.get("customer", ""),
            "phone": phone,
            "amount": float(o.get("amount", 0) or 0),
            "addr_line1": addr1,
            "addr_line2": addr2,
            "addr_line3": addr3,
            "pincode": str(o.get("pincode", "")),
            "state_code": o.get("state_code", ""),
            "city": o.get("city", ""),
            "is_cod": o.get("is_cod", ""),
            "seller_sku": seller_sku,
            "title": o.get("title", ""),
            "qty": sum([int(q.strip()) for q in raw_qty.split(",") if q.strip().isdigit()]) if "," in raw_qty else (int(raw_qty) if raw_qty.isdigit() else 1),
            "raw_qty": raw_qty,
            "is_multi": is_multi,
            "is_repeat": is_repeat,
            "address_valid": is_valid,
            "issue": issue,
            "source": source,
            "fulfilled": fulfilled,
            "tracking_no": o.get("tracking_no", ""),
            "carrier": o.get("carrier", ""),
            "status": o.get("status", ""),
        }

        is_err = "error" in str(fulfilled).lower() or "fail" in str(fulfilled).lower() or "error" in str(o.get("status", "")).lower()
        if (not source and not fulfilled) or is_err:
            row["select"] = False if is_err else True
            row["is_error"] = is_err
            row["path"] = "MCF"
            pending.append(row)
        elif source:
            processed.append(row)

    return pending, processed, None


# ─────────────────────────────────────────────
# STAT CARD HELPER
# ─────────────────────────────────────────────
def stat_card(col, label, value, color="#1f77b4"):
    col.markdown(
        f"""
        <div style="background:{color};padding:14px 10px;border-radius:10px;text-align:center;color:white">
          <div style="font-size:1.8rem;font-weight:700">{value}</div>
          <div style="font-size:0.8rem;margin-top:4px">{label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────
# PAGE 1: DASHBOARD
# ─────────────────────────────────────────────
def page_dashboard():
    st.title("📊 Dashboard")
    stats = db.get_stats()

    # Row 1
    cols = st.columns(4)
    stat_card(cols[0], "Total Orders", stats["total"], "#2c3e50")
    stat_card(cols[1], "Pending", stats["pending"], "#e67e22")
    stat_card(cols[2], "Processing", stats["processing"], "#3498db")
    stat_card(cols[3], "Shipped", stats["shipped"], "#27ae60")

    st.markdown("<br>", unsafe_allow_html=True)

    # Row 2
    cols2 = st.columns(4)
    stat_card(cols2[0], "MCF Orders", stats["mcf_count"], "#8e44ad")
    stat_card(cols2[1], "Delhivery", stats["delhivery_count"], "#e74c3c")
    stat_card(cols2[2], "Shopify Fulfilled", stats["shopify_fulfilled"], "#16a085")
    stat_card(cols2[3], "Failed", stats["failed"], "#c0392b")

    st.markdown("---")

    # Row 3 — secondary metrics
    cols3 = st.columns(5)
    cols3[0].metric("Today's Orders", stats["today_orders"])
    cols3[1].metric("This Week", stats["week_orders"])
    cols3[2].metric("This Month", stats["month_orders"])
    cols3[3].metric("Avg Processing (hrs)", stats["avg_processing_hours"])
    cols3[4].metric("COD / Prepaid", f"{stats['cod_count']} / {stats['prepaid_count']}")

    st.markdown("---")

    # Row 4 — tracking
    cols4 = st.columns(2)
    cols4[0].metric("With Tracking ID", stats["with_tracking"])
    cols4[1].metric("MCF Awaiting Tracking", stats["without_tracking"])

    # Recent orders
    st.subheader("Recent Orders")
    recent = db.get_orders_filtered(limit=20)
    if recent:
        df = pd.DataFrame(recent)
        show_cols = ["order_id", "customer_name", "status", "fulfillment_channel", "tracking_number", "created_at"]
        show_cols = [c for c in show_cols if c in df.columns]
        st.dataframe(df[show_cols], use_container_width=True, hide_index=True)
    else:
        st.info("No orders in local DB yet. Go to Sync to fetch orders.")


# ─────────────────────────────────────────────
# PAGE 2: ORDERS
# ─────────────────────────────────────────────
def page_orders():
    st.title("📋 Orders")

    if st.button("🔄 Refresh from Endpoint", type="primary"):
        with st.spinner("Fetching from Google Sheet endpoint..."):
            pending, processed, err = fetch_endpoint_orders()
            if err:
                st.error(err)
            else:
                st.session_state.pending_df = pd.DataFrame(pending) if pending else pd.DataFrame()
                st.session_state.processed_df = pd.DataFrame(processed) if processed else pd.DataFrame()
                st.session_state.processing_log = []
                st.success(f"✅ Pending: {len(pending)} | Already Processed: {len(processed)}")

    tab1, tab2 = st.tabs(["⏳ Pending Orders", "✅ Already Processed"])

    # ── TAB 1: PENDING ──────────────────────────────────────────────────
    with tab1:
        df = st.session_state.pending_df
        if df is None:
            st.info("Click 'Refresh from Endpoint' to load orders.")
        elif df.empty:
            st.success("🎉 No pending orders!")
        else:
            # Summary metrics
            total = len(df)
            invalid = int((~df["address_valid"]).sum())
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Pending", total)
            c2.metric("Valid", total - invalid)
            c3.metric("Address Issues", invalid)

            st.markdown("---")
            b1, b2 = st.columns([1, 4])
            if b1.button("☑️ Select All Pending"):
                st.session_state.pending_df["select"] = True
                st.rerun()
            if b2.button("🔲 Unselect All Pending"):
                st.session_state.pending_df["select"] = False
                st.rerun()

            # Editable table
            display_cols = [
                "select", "order_id", "customer", "phone",
                "address_valid", "issue", "raw_qty", "seller_sku",
                "addr_line1", "addr_line2", "addr_line3",
                "city", "state_code", "pincode",
                "amount", "is_cod", "path"
            ]
            
            def render_grid(sub_df, key):
                if sub_df.empty:
                    st.info(f"No orders in {key}.")
                    return sub_df.copy()
                return st.data_editor(
                    sub_df[display_cols].copy(),
                    column_config={
                        "select":         st.column_config.CheckboxColumn("☑", default=True),
                        "order_id":       st.column_config.TextColumn("Order ID", disabled=True),
                        "customer":       st.column_config.TextColumn("Customer", disabled=True),
                        "phone":          st.column_config.TextColumn("Phone"),
                        "address_valid":  st.column_config.CheckboxColumn("✓ Addr", disabled=True),
                        "issue":          st.column_config.TextColumn("Issue", disabled=True),
                        "raw_qty":        st.column_config.TextColumn("Qty", disabled=True),
                        "seller_sku":     st.column_config.TextColumn("SKU", disabled=True),
                        "addr_line1":     st.column_config.TextColumn("Addr L1 (60)"),
                        "addr_line2":     st.column_config.TextColumn("Addr L2 (60)"),
                        "addr_line3":     st.column_config.TextColumn("Addr L3 (60)"),
                        "city":           st.column_config.TextColumn("City"),
                        "state_code":     st.column_config.TextColumn("State"),
                        "pincode":        st.column_config.TextColumn("Pin"),
                        "amount":         st.column_config.NumberColumn("Amount ₹"),
                        "is_cod":         st.column_config.TextColumn("Payment", disabled=True),
                        "path":           st.column_config.SelectboxColumn("Path", options=["MCF", "Delhivery"]),
                    },
                    hide_index=True,
                    use_container_width=True,
                    num_rows="fixed",
                    key=f"pending_editor_{key}",
                )

            t_single, t_multi, t_repeat, t_error = st.tabs(["⏳ Single Orders", "📦 Multi SKU/Unit Orders", "⚠️ Repeated Customers", "❌ Error / Retry Orders"])
            
            with t_single:
                mask1 = (~df["is_multi"]) & (~df["is_repeat"]) & (~df.get("is_error", False))
                edit1 = render_grid(df[mask1], "single")
            with t_multi:
                mask2 = (df["is_multi"]) & (~df["is_repeat"]) & (~df.get("is_error", False))
                edit2 = render_grid(df[mask2], "multi")
            with t_repeat:
                mask3 = (df["is_repeat"]) & (~df.get("is_error", False))
                edit3 = render_grid(df[mask3], "repeat")
            with t_error:
                mask4 = df.get("is_error", False)
                edit4 = render_grid(df[mask4], "error")

            edit_pieces = []
            for e in [edit1, edit2, edit3, edit4]:
                if not e.empty: edit_pieces.append(e)
            
            if edit_pieces:
                edit_df = pd.concat(edit_pieces)
            else:
                edit_df = df[display_cols].copy()

            # Build selected rows from edit_df (do NOT write back to session_state
            # on every render — that causes the double-click checkbox bug)
            selected_ids = edit_df.loc[edit_df["select"] == True, "order_id"].tolist()
            # Merge path + address edits from edit_df into full df
            edit_map = edit_df.set_index("order_id")[["path", "addr_line1", "addr_line2", "addr_line3", "phone", "pincode"]].to_dict("index")
            full_rows = df.copy()
            for oid, vals in edit_map.items():
                mask = full_rows["order_id"] == oid
                for col, val in vals.items():
                    full_rows.loc[mask, col] = val

            selected  = full_rows[full_rows["order_id"].isin(selected_ids)]
            mcf_sel   = selected[selected["path"] == "MCF"]
            del_sel   = selected[selected["path"] == "Delhivery"]

            sc1, sc2, sc3 = st.columns(3)
            sc1.metric("Selected", len(selected))
            sc2.metric("MCF", len(mcf_sel))
            sc3.metric("Delhivery", len(del_sel))

            st.markdown("---")
            if st.button("▶ Process Selected Orders", type="primary", disabled=len(selected) == 0):
                _process_orders(df, selected, mcf_sel, del_sel)

    # ── TAB 2: ALREADY PROCESSED ────────────────────────────────────────
    with tab2:
        df2 = st.session_state.processed_df
        if df2 is None:
            st.info("Click 'Refresh from Endpoint' to load orders.")
        elif df2.empty:
            st.info("No processed orders found.")
        else:
            show = ["order_id", "customer", "source", "fulfilled", "tracking_no", "carrier", "status"]
            show = [c for c in show if c in df2.columns]
            st.metric("Total Processed", len(df2))
            st.dataframe(df2[show], use_container_width=True, hide_index=True)

    # ── PROCESSING LOG ───────────────────────────────────────────────────
    if st.session_state.processing_log:
        st.markdown("---")
        st.subheader("📋 Processing Results")
        ok_count = sum(1 for e in st.session_state.processing_log if e["ok"])
        fail_count = len(st.session_state.processing_log) - ok_count
        rc1, rc2, rc3 = st.columns(3)
        rc1.metric("Total Processed", len(st.session_state.processing_log))
        rc2.metric("✅ Success", ok_count)
        rc3.metric("❌ Failed", fail_count)

        # Table view with all details
        log_rows = []
        for entry in st.session_state.processing_log:
            log_rows.append({
                "Order ID":    entry["order_id"],
                "Path":        entry["path"],
                "MCF/Del Status": "✅ " + entry["msg"] if entry["ok"] else "❌ " + entry["msg"],
                "Shopify":     entry.get("shopify", "—"),
                "Tracking ID": entry.get("tracking", "—"),
            })
        st.dataframe(pd.DataFrame(log_rows), use_container_width=True, hide_index=True)

        # Detail per order
        with st.expander("View order-by-order details"):
            for entry in st.session_state.processing_log:
                cols = st.columns([2, 2, 3, 3])
                cols[0].markdown(f"**{entry['order_id']}**")
                cols[1].markdown(entry["path"])
                cols[2].markdown(entry.get("shopify", "—"))
                tracking = entry.get("tracking", "—")
                if tracking and tracking not in ["—", "Pending (background worker will fetch)", "Not assigned yet"]:
                    cols[3].markdown(f"`{tracking}`")
                else:
                    cols[3].markdown(tracking)


def _process_orders(full_df, selected, mcf_sel, del_sel):
    """Core processing logic for selected orders."""
    log = []
    sheet_updates = []
    sheet_tracking_updates = []
    shopify_cfg = get_shopify_config(secrets)
    total = len(selected)

    try:
        sheets_service = init_sheets_service()
    except Exception as e:
        st.error(f"Google Sheets init failed: {e}")
        return

    progress = st.progress(0)
    status_text = st.empty()
    done = 0

    # ── MCF ──────────────────────────────────────────────────────────────
    if len(mcf_sel) > 0:
        token, token_err = get_fresh_token()
        if token_err:
            st.error(f"Amazon auth failed: {token_err}")
        else:
            for _, row in mcf_sel.iterrows():
                order_id = row["order_id"]
                status_text.text(f"MCF: Processing {order_id}...")

                skus = [s.strip() for s in str(row.get("seller_sku", "")).split(",") if s.strip()]
                raw_qty = str(row.get("raw_qty", row.get("qty", "1")))
                qtys_str = [q.strip() for q in raw_qty.split(",") if q.strip()]

                if len(qtys_str) == 1 and len(skus) > 1:
                    qtys = [int(qtys_str[0])]*len(skus)
                else:
                    qtys = [int(q) if q.isdigit() else 1 for q in qtys_str]

                while len(qtys) < len(skus):
                    qtys.append(1)

                items = []
                # Distribute amount across items roughly
                amount_per_item = float(row["amount"]) / max(1, len(skus))
                
                for idx, sku in enumerate(skus):
                    if not sku: continue
                    items.append({
                        "sellerSku": sku,
                        "sellerFulfillmentOrderItemId": f"{sku}-{order_id}-{idx}",
                        "quantity": qtys[idx],
                        "perUnitDeclaredValue": {"currencyCode": "INR", "value": str(round(amount_per_item, 2))},
                    })
                
                if not items:
                    # Fallback single item
                    items = [{
                        "sellerSku": "Unknown",
                        "sellerFulfillmentOrderItemId": f"Unknown-{order_id}-0",
                        "quantity": 1,
                        "perUnitDeclaredValue": {"currencyCode": "INR", "value": str(row["amount"])},
                    }]

                order_data = dict(row) | {"items": items}
                success, msg = create_mcf_order(token, order_data)

                if success:
                    db.save_order(order_data)
                    db.update_order_status(order_id, "PROCESSING", fulfillment_channel="MCF")
                    sheet_updates.append({"row": row["row_number"], "source": "MCF", "status": "Fulfilled"})

                    # Shopify fulfill (MCF tracking comes async — no tracking yet at this stage)
                    shopify_ok, shopify_msg = _shopify_fulfill(order_id, shopify_cfg, tracking_info=None)

                    log.append({
                        "order_id": order_id, "path": "MCF", "ok": True,
                        "msg": msg,
                        "shopify": "✅ Fulfilled" if shopify_ok else f"⚠️ {shopify_msg}",
                        "tracking": "Pending (background worker will fetch)",
                    })
                else:
                    db.save_order(order_data)
                    db.update_order_status(order_id, "FAILED", reason=msg)
                    sheet_updates.append({"row": row["row_number"], "source": "MCF", "status": f"Error: {msg[:40]}"})
                    log.append({
                        "order_id": order_id, "path": "MCF", "ok": False,
                        "msg": msg, "shopify": "—", "tracking": "—",
                    })

                done += 1
                progress.progress(done / total)

    # ── DELHIVERY ────────────────────────────────────────────────────────
    if len(del_sel) > 0:
        del_key = secrets.get("DELHIVERY_API_KEY", "")
        if not del_key:
            st.error("DELHIVERY_API_KEY missing in secret.txt")
        else:
            for _, row in del_sel.iterrows():
                order_id = row["order_id"]
                status_text.text(f"Delhivery: Processing {order_id}...")

                order_data = dict(row) | {"total_qty": int(row["qty"])}
                success, resp, err = create_delhivery_order(
                    del_key, order_data,
                    pickup_location=st.session_state.get("pickup_loc", "emaar")
                )

                if success:
                    # Extract waybill (AWB) immediately from Delhivery response
                    pkgs = resp.get("packages", [])
                    waybill = pkgs[0].get("waybill", "") if pkgs else ""

                    db.save_order(order_data)
                    db.update_order_status(order_id, "PROCESSING", fulfillment_channel="DELHIVERY")

                    # If waybill received, update tracking in DB immediately
                    if waybill:
                        db.update_order_tracking(order_id, "Delhivery", waybill, "")

                    sheet_updates.append({"row": row["row_number"], "source": "Delhivery", "status": "Fulfilled"})

                    # Delhivery waybill → Queue sheet tracking update (S/T/U)
                    if waybill and row.get("row_number"):
                        sheet_tracking_updates.append({
                            "row": row["row_number"],
                            "carrier": "Delhivery",
                            "tracking_no": waybill,
                            "url": f"https://www.delhivery.com/track/package/{waybill}",
                        })

                    # Shopify fulfill WITH tracking info if waybill available
                    tracking_info = None
                    if waybill:
                        tracking_info = {
                            "number": waybill,
                            "company": "Delhivery",
                            "url": f"https://www.delhivery.com/track/package/{waybill}",
                        }
                    shopify_ok, shopify_msg = _shopify_fulfill(order_id, shopify_cfg, tracking_info=tracking_info)

                    log.append({
                        "order_id": order_id, "path": "Delhivery", "ok": True,
                        "msg": "Submitted",
                        "shopify": "✅ Fulfilled" if shopify_ok else f"⚠️ {shopify_msg}",
                        "tracking": waybill if waybill else "Not assigned yet",
                    })
                else:
                    db.save_order(order_data)
                    db.update_order_status(order_id, "FAILED", reason=str(err))
                    sheet_updates.append({"row": row["row_number"], "source": "Delhivery", "status": f"Error: {str(err)[:40]}"})
                    log.append({
                        "order_id": order_id, "path": "Delhivery", "ok": False,
                        "msg": str(err), "shopify": "—", "tracking": "—",
                    })

                done += 1
                progress.progress(done / total)

    # ── SHEET UPDATES ────────────────────────────────────────────────────
    if sheet_updates:
        status_text.text("Updating Google Sheet (Q/R columns)...")
        try:
            update_sheet_remarks(sheets_service, SHEET_ID, sheet_updates)
            db.log_sync("SHEET_REMARKS", "SUCCESS", f"{len(sheet_updates)} rows updated")
        except Exception as e:
            st.error(f"Sheet Q/R update failed: {e}")
            db.log_sync("SHEET_REMARKS", "ERROR", str(e)[:300])

    if sheet_tracking_updates:
        status_text.text("Updating Google Sheet (S/T/U tracking columns)...")
        try:
            update_sheet_tracking(sheets_service, SHEET_ID, sheet_tracking_updates)
            db.log_sync("SHEET_TRACKING", "SUCCESS", f"{len(sheet_tracking_updates)} rows updated")
        except Exception as e:
            st.error(f"Sheet S/T/U update failed: {e}")

    status_text.text("✅ Done!")
    progress.progress(1.0)
    st.session_state.processing_log = log


def _shopify_fulfill(order_id, shopify_cfg, tracking_info=None):
    """Fulfill order on Shopify with optional tracking info.
    - New order: creates fulfillment with tracking.
    - Already fulfilled: updates tracking on existing fulfillment.
    Returns (success: bool, message: str)
    """
    if not shopify_cfg.get("shop_url"):
        return False, "Shopify not configured"
    try:
        s_order = get_shopify_order(order_id, shopify_cfg["headers"], shopify_cfg["shop_url"])
        if not s_order:
            return False, "Order not found on Shopify"
        ok = fulfill_order(s_order, shopify_cfg["headers"], shopify_cfg["shop_url"], tracking_info=tracking_info)
        if ok:
            db.mark_shopify_fulfilled(order_id)
            if tracking_info and tracking_info.get("number"):
                return True, f"Tracking: {tracking_info['number']}"
            return True, "Fulfilled"
        # fulfill_order returns False only when truly nothing to do
        return True, "Already fulfilled (no change)"
    except Exception as e:
        return False, str(e)[:80]


# ─────────────────────────────────────────────
# PAGE 3: TRACKING
# ─────────────────────────────────────────────
def page_tracking():
    st.title("🚚 Tracking Hub")
    tab_awb, tab_live = st.tabs(["📦 AWB Fetch (MCF)", "🟢 Live Transit Updates"])
    
    with tab_live:
        _render_live_updates()
        
    with tab_awb:
        _render_awb_fetch()

def _render_live_updates():
    st.info(
        "ℹ️ **Live Tracker:** यह टूल Swiship (Amazon) और Delhivery की API का उपयोग करके शीट में मौजूद ट्रैकिंग नम्बरों को ट्रैक करता है "
        "और शीट के अंत में 'Status', 'ETA', 'Pickup/Delivery Date' और 'RTO' वाले कॉलम भर देता है।"
    )
    if st.button("▶ Run Full Live Tracking Update", type="primary"):
        prog = st.progress(0)
        status_txt = st.empty()
        
        def cb(idx, total, no):
            prog.progress((idx + 1) / total)
            status_txt.text(f"Tracking: {no} ({idx + 1}/{total})")
            
        with st.spinner("Fetching data and querying APIs..."):
            res = run_live_tracking_update(progress_callback=cb)
            status_txt.text("✅ Sheet Updated with Live Status")
            if res:
                df = pd.DataFrame(res)
                # Ensure the 'Status' column exists before trying to display it specifically
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.warning("No tracking numbers found to update.")

def _render_awb_fetch():
    st.info(
        "ℹ️ **MCF tracking Amazon ke ship karne ke 1-2 din baad milti hai.** "
        "Yahan Sheet ke saare MCF orders dikhte hain — MCF status (Planning/Shipped) ke saath. "
        "'Fetch All' click karo → jo ship ho chuke hain unka AWB milega + Shopify + Sheet update ho jayega."
    )

    # ── Load MCF orders from Sheet endpoint (source of truth) ────────────
    ss("tracking_sheet_orders", None)

    if st.button("🔄 Load MCF Orders from Sheet", key="load_mcf_btn"):
        with st.spinner("Sheet se MCF orders fetch kar raha hoon..."):
            try:
                resp = requests.get(APPS_SCRIPT_URL, timeout=30)
                data = resp.json()
                mcf_orders = []
                for o in data.get("orders", []):
                    source = str(o.get("source", "")).strip().upper()
                    if "MCF" in source or "DELHI" in source:
                        mcf_orders.append({
                            "row_number":   int(o.get("row_number", 0) or 0),
                            "order_id":     str(o.get("ord_serial", "")).replace("#", "").strip(),
                            "customer":     o.get("customer", ""),
                            "amount":       o.get("amount", 0),
                            "tracking_no":  str(o.get("tracking_no", "")).strip(),
                            "carrier":      str(o.get("carrier", "")).strip(),
                            "source":       source,
                            "fulfilled":    str(o.get("fulfilled", "")).strip(),
                        })
                st.session_state.tracking_sheet_orders = mcf_orders
                has_trk = sum(1 for o in mcf_orders if o["tracking_no"])
                st.success(f"✅ {len(mcf_orders)} MCF orders found | With tracking: {has_trk} | Without: {len(mcf_orders)-has_trk}")
            except Exception as e:
                st.error(f"Endpoint error: {e}")

    mcf_orders = st.session_state.tracking_sheet_orders
    if mcf_orders is None:
        st.warning("Upar 'Load MCF Orders from Sheet' click karo.")
        return

    # ── Summary metrics ───────────────────────────────────────────────────
    has_trk  = [o for o in mcf_orders if o["tracking_no"]]
    need_trk = [o for o in mcf_orders if not o["tracking_no"]]
    m1, m2, m3 = st.columns(3)
    m1.metric("Total MCF Orders", len(mcf_orders))
    m2.metric("✅ Tracking Added", len(has_trk))
    m3.metric("⏳ Tracking Pending", len(need_trk))

    # ── Tabs: Pending | Already have tracking ────────────────────────────
    t1, t2 = st.tabs([f"⏳ Pending ({len(need_trk)})", f"✅ Tracking Added ({len(has_trk)})"])

    with t1:
        if not need_trk:
            st.success("Sab orders ka tracking add ho chuka hai!")
        else:
            df_need = pd.DataFrame(need_trk)[["order_id", "customer", "amount", "row_number"]]
            st.dataframe(df_need, use_container_width=True, hide_index=True)

            st.markdown("---")
            btn_col1, btn_col2 = st.columns([2, 3])

            # ── FETCH ALL button ──────────────────────────────────────────
            if btn_col1.button("🔍 Fetch Tracking for All", type="primary", key="fetch_all_btn"):
                token, err = get_fresh_token()
                if err:
                    st.error(f"Auth failed: {err}")
                else:
                    shopify_cfg = get_shopify_config(secrets)
                    try:
                        sheets_svc = init_sheets_service()
                    except Exception as e:
                        st.error(f"Sheets init failed: {e}")
                        return

                    sheet_updates        = []   # S/T/U/V — tracking + status remark
                    no_trk_remark_updates = []  # Q/R — MCF status for orders without tracking
                    result_rows          = []
                    prog = st.progress(0)
                    status_txt = st.empty()
                    total = len(need_trk)

                    for i, order in enumerate(need_trk):
                        order_id = order["order_id"]
                        orig_source = str(order.get("source", "")).upper()
                        status_txt.text(f"Checking {order_id} ({i+1}/{total})...")

                        tn, cc, mcf_status = "", "", ""
                        is_delhivery_first = "DELHI" in orig_source
                        del_api_key = secrets.get("DELHIVERY_API_KEY", "")

                        if not is_delhivery_first:
                            tn, cc, mcf_status, _ = fetch_mcf_data(order_id, token)

                        # Status label for sheet remark
                        if tn:
                            from datetime import datetime as _dt
                            remark = f"Tracking Added {_dt.now().strftime('%d/%m %H:%M')}"
                            db.update_order_tracking(order_id, cc or "", tn, "")
                            t_info = {"number": tn, "company": cc or "Amazon", "url": ""}
                            s_ok, s_msg = _shopify_fulfill(order_id, shopify_cfg, tracking_info=t_info)

                            sheet_updates.append({"row": order["row_number"], "carrier": cc or "Amazon", "tracking_no": tn, "url": "", "remark": remark})
                            result_rows.append({"Order ID": order_id, "Customer": order["customer"], "Status": mcf_status, "Tracking ID": tn, "Carrier": cc or "", "Shopify": "✅ Fulfilled" if s_ok else f"⚠️ {s_msg}", "Sheet": "✅ Updated"})
                        else:
                            # ── Delhivery Check (Either Fallback or Primary) ──
                            if is_delhivery_first or mcf_status == "NotFound":
                                del_found, del_awb, del_status, _ = get_delhivery_tracking(del_api_key, order_id)
                                if del_found and del_awb:
                                    from datetime import datetime as _dt
                                    remark = f"Delhivery AWB {_dt.now().strftime('%d/%m %H:%M')}"
                                    db.update_order_tracking(order_id, "Delhivery", del_awb, "")
                                    t_info = {"number": del_awb, "company": "Delhivery", "url": ""}
                                    s_ok, s_msg = _shopify_fulfill(order_id, shopify_cfg, tracking_info=t_info)

                                    sheet_updates.append({"row": order["row_number"], "carrier": "Delhivery", "tracking_no": del_awb, "url": "", "remark": remark})
                                    no_trk_remark_updates.append({"row": order["row_number"], "source": "Delhivery", "status": f"Fulfilled | {del_status}" if del_status else "Fulfilled"})
                                    result_rows.append({"Order ID": order_id, "Customer": order["customer"], "Status": "Found on Delhivery", "Tracking ID": del_awb, "Carrier": "Delhivery", "Shopify": "✅ Fulfilled" if s_ok else f"⚠️ {s_msg}", "Sheet": "✅ Delhivery AWB"})
                                    prog.progress((i + 1) / total)
                                    time.sleep(0.4)
                                    continue  # Move to next order

                            # Not found anywhere. Create proper status label based on source
                            if is_delhivery_first:
                                status_label = "Delhivery: Not Found"
                                mcf_status = "NotFound"
                            else:
                                status_label = {
                                    "Planning": "MCF: Planning", "Received": "MCF: Received", "Processing": "MCF: Processing",
                                    "Complete": "MCF: Complete", "Cancelled": "MCF: Cancelled", "NotFound": "MCF: Not Found"
                                }.get(mcf_status, f"MCF: {mcf_status}")

                            sheet_updates.append({"row": order["row_number"], "carrier": "", "tracking_no": "", "url": "", "remark": status_label})
                            no_trk_remark_updates.append({"row": order["row_number"], "source": "Delhivery" if is_delhivery_first else "MCF", "status": status_label})
                            result_rows.append({"Order ID": order_id, "Customer": order["customer"], "Status": mcf_status if not is_delhivery_first else "Delhivery Not Found", "Tracking ID": "—", "Carrier": "—", "Shopify": "—", "Sheet": f"✅ R col: {status_label}"})

                        prog.progress((i + 1) / total)
                        time.sleep(0.4)

                    status_txt.text("Sheet update ho raha hai...")

                    # Batch update S/T/U/V for orders WITH tracking
                    if sheet_updates:
                        try:
                            update_sheet_tracking(sheets_svc, SHEET_ID, sheet_updates)
                            db.log_sync("SHEET_TRACKING", "SUCCESS", f"{len(sheet_updates)} rows updated")
                        except Exception as e:
                            st.error(f"Sheet S/T/U/V update failed: {e}")

                    # Batch update Q/R for orders WITHOUT tracking (so user sees MCF status in visible cols)
                    if no_trk_remark_updates:
                        try:
                            update_sheet_remarks(sheets_svc, SHEET_ID, no_trk_remark_updates)
                            db.log_sync("SHEET_REMARKS", "SUCCESS", f"{len(no_trk_remark_updates)} no-tracking rows → Q/R updated")
                        except Exception as e:
                            st.error(f"Sheet Q/R update failed: {e}")

                    status_txt.text("✅ Done!")
                    prog.progress(1.0)

                    # Results summary
                    found_count = sum(1 for r in result_rows if r["Tracking ID"] != "—")
                    st.subheader("Tracking Fetch Results")
                    rc1, rc2 = st.columns(2)
                    rc1.metric("Tracking Found", found_count)
                    rc2.metric("Still Pending", total - found_count)
                    st.dataframe(pd.DataFrame(result_rows), use_container_width=True, hide_index=True)

                    # Reload orders
                    st.session_state.tracking_sheet_orders = None
                    st.info("Reload ke liye upar 'Load MCF Orders from Sheet' dobara click karo.")

            # ── SINGLE ORDER manual check ─────────────────────────────────
            with btn_col2:
                with st.expander("🔎 Single Order Check"):
                    manual_id = st.text_input("Order ID (e.g. 2860)", key="manual_track_id")
                    if st.button("Check", key="manual_check_btn") and manual_id:
                        token2, err2 = get_fresh_token()
                        if token2:
                            tn, cc, mcf_status, raw = fetch_mcf_data(manual_id.strip(), token2)
                            st.write(f"**MCF Status:** `{mcf_status}`")
                            if tn:
                                st.success(f"✅ Tracking: **{tn}** | Carrier: {cc}")
                                db.update_order_tracking(manual_id.strip(), cc or "", tn, "")
                                s_ok, s_msg = _shopify_fulfill(
                                    manual_id.strip(), get_shopify_config(secrets),
                                    tracking_info={"number": tn, "company": cc or "Amazon", "url": ""}
                                )
                                st.info(f"Shopify: {'✅ Fulfilled' if s_ok else '⚠️ ' + s_msg}")
                                # Sheet update for this single order
                                o_meta = next((o for o in need_trk if o["order_id"] == manual_id.strip()), None)
                                if o_meta and o_meta.get("row_number"):
                                    try:
                                        from datetime import datetime as _dt
                                        svc = init_sheets_service()
                                        update_sheet_tracking(svc, SHEET_ID, [{
                                            "row": o_meta["row_number"],
                                            "carrier": cc or "Amazon",
                                            "tracking_no": tn,
                                            "url": "",
                                            "remark": f"Tracking Added {_dt.now().strftime('%d/%m %H:%M')}",
                                        }])
                                        st.success("✅ Sheet updated (S/T/U/V)")
                                    except Exception as e:
                                        st.warning(f"Sheet failed: {e}")
                            else:
                                st.warning(f"⏳ No tracking yet. MCF Status: `{mcf_status}`")
                        else:
                            st.error(f"Auth: {err2}")

    with t2:
        if not has_trk:
            st.info("Abhi kisi order ka tracking nahi aaya.")
        else:
            df_has = pd.DataFrame(has_trk)[["order_id", "customer", "tracking_no", "carrier", "fulfilled"]]
            st.dataframe(df_has, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────
# PAGE 4: REPORTS
# ─────────────────────────────────────────────
def page_reports():
    st.title("📈 Reports & Export")

    with st.expander("🔍 Filters", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            status_opts = ["All", "NEW", "PROCESSING", "SHIPPED", "DELIVERED", "FAILED"]
            sel_status = st.multiselect("Status", status_opts, default=["All"])
        with fc2:
            ch_opts = ["All", "MCF", "DELHIVERY"]
            sel_channel = st.multiselect("Channel", ch_opts, default=["All"])
        with fc3:
            search = st.text_input("Search (Order ID / Customer / Tracking)")

        dc1, dc2 = st.columns(2)
        date_from = dc1.date_input("From Date", value=None)
        date_to = dc2.date_input("To Date", value=None)

    # Build filter args
    status_filter = None if "All" in sel_status or not sel_status else sel_status
    channel_filter = None if "All" in sel_channel or not sel_channel else sel_channel

    orders = db.get_orders_filtered(
        status=status_filter,
        channel=channel_filter,
        date_from=date_from,
        date_to=date_to,
        search=search or None,
        limit=1000,
    )

    st.metric("Results", len(orders))

    if orders:
        df = pd.DataFrame(orders)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # CSV Export
        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        st.download_button(
            "📥 Download CSV",
            data=csv_buf.getvalue(),
            file_name=f"orders_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )

        # Excel Export
        try:
            excel_buf = io.BytesIO()
            with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Orders")
            st.download_button(
                "📥 Download Excel",
                data=excel_buf.getvalue(),
                file_name=f"orders_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as e:
            st.warning(f"Excel export failed: {e}")
    else:
        st.info("No orders match the selected filters.")


# ─────────────────────────────────────────────
# PAGE 5: SYNC
# ─────────────────────────────────────────────
def page_sync():
    st.title("🔄 Sync & Logs")

    st.info("💡 For automatic background sync, run separately:\n```\npython background_worker.py\n```")

    if st.button("▶ Manual Sync Now", type="primary"):
        with st.spinner("Syncing from Apps Script endpoint..."):
            try:
                resp = requests.get(APPS_SCRIPT_URL, timeout=30)
                data = resp.json()

                if not data.get("success") or not data.get("orders"):
                    st.warning("No orders from endpoint")
                else:
                    added = 0
                    for o in data["orders"]:
                        status_raw = str(o.get("status", "")).lower()
                        fulfilled = str(o.get("fulfilled", "")).strip()
                        if fulfilled or status_raw not in ["pending", ""]:
                            continue

                        order_id = str(o.get("ord_serial", "")).replace("#", "").strip()
                        if not order_id:
                            continue

                        phone = clean_phone_number(o.get("phone", ""))
                        addr1, addr2, addr3, _ = validate_address(
                            f"{o.get('address1', '')} {o.get('address2', '')}".strip()
                        )

                        order_data = {
                            "order_id": order_id,
                            "date": o.get("date", ""),
                            "customer": o.get("customer", ""),
                            "phone": phone,
                            "amount": float(o.get("amount", 0) or 0),
                            "addr_line1": addr1, "addr_line2": addr2, "addr_line3": addr3,
                            "pincode": str(o.get("pincode", "")),
                            "state_code": o.get("state_code", ""),
                            "city": o.get("city", ""),
                            "is_cod": str(o.get("is_cod", "")),
                            "seller_sku": o.get("seller_sku", ""),
                            "title": o.get("title", "")[:200],
                            "qty": (int(str(o.get("qty", 1)).strip()) if str(o.get("qty", "1")).strip().isdigit() else 1),
                            "row_number": int(o.get("row_number", 0) or 0),
                            "source_channel": "SHOPIFY",
                            "items": [{
                                "seller_sku": o.get("seller_sku", ""),
                                "title": o.get("title", ""),
                                "quantity": int(o.get("qty", 1) or 1),
                                "price": o.get("amount", 0),
                            }],
                        }
                        if db.save_order(order_data):
                            added += 1

                    st.success(f"✅ Synced {added} new orders to local DB")
                    db.log_sync("MANUAL_SYNC", "SUCCESS", f"Added {added} orders")

            except Exception as e:
                st.error(f"Sync failed: {e}")
                db.log_sync("MANUAL_SYNC", "ERROR", str(e)[:300])

    st.markdown("---")

    # Sync Logs
    st.subheader("Sync Logs (Recent 50)")
    logs = db.get_sync_logs(50)
    if logs:
        df_logs = pd.DataFrame(logs)
        show_log = ["event_type", "status", "details", "timestamp"]
        show_log = [c for c in show_log if c in df_logs.columns]
        st.dataframe(df_logs[show_log], use_container_width=True, hide_index=True)
    else:
        st.info("No sync logs yet.")


# ─────────────────────────────────────────────
# ROUTER
# ─────────────────────────────────────────────
page = st.session_state.page
if page == "Dashboard":
    page_dashboard()
elif page == "Orders":
    page_orders()
elif page == "Tracking":
    page_tracking()
elif page == "Reports":
    page_reports()
elif page == "Sync":
    page_sync()
else:
    page_dashboard()
