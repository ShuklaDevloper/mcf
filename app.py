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
    build_tracking_url,
    clean_phone_number,
    create_delhivery_order,
    create_mcf_order,
    fulfill_order,
    get_access_token,
    get_delhivery_tracking,
    get_shopify_config,
    get_shopify_order,
    infer_sheet_source_q,
    init_sheets_service,
    parse_date,
    read_secret,
    update_sheet_remarks,
    update_sheet_tracking,
    validate_address,
    validate_pincode,
)
from live_tracker import lookup_awb_by_order_id, run_live_tracking_update

# ─────────────────────────────────────────────
# MCF BLOCKED SKUs — these will NOT be sent to Amazon MCF
# ─────────────────────────────────────────────
MCF_BLOCKED_SKUS = {
    "WC_Cervical_Pillow_White",
    "WC_Back_Rest_Black",
    "WC_Wedge_Pillow_Black",
}

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
    "🛠️ Shopify Tools": "Shopify Tools",
    "🏷️ Generate Labels": "Labels",
}

with st.sidebar:
    st.markdown("## 📦 Fulfillment OMS")
    st.markdown("---")
    for label, page_id in PAGES.items():
        if st.button(label, width='stretch', key=f"nav_{page_id}"):
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
        repeat_phones = set(db.get_all_phones())
    except Exception:
        repeat_phones = set()

    current_batch_phones = {}
    for o in data.get("orders", []):
        p = clean_phone_number(o.get("phone", ""))
        if p:
            current_batch_phones[p] = current_batch_phones.get(p, 0) + 1

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
        state_code = str(o.get("state_code", "")).strip().upper()
        
        issue = (
            "Address overflow" if not addr_valid
            else "Invalid pincode" if not pin_valid
            else "Invalid phone" if not phone_valid
            else ""
        )

        if state_code in ["UP", "UTTAR PRADESH", "DELHI", "NEW DELHI", "DL", "HARYANA", "HR"]:
            issue = f"Blocked MCF State ({state_code})" if not issue else issue

        is_cod_flag = str(o.get("is_cod", "")).lower() in ["true", "yes", "1", "cod"]

        raw_qty = str(o.get("qty", "1")).strip()
        seller_sku = str(o.get("seller_sku", "")).strip()
        
        is_multi = False
        if "," in raw_qty or "," in seller_sku:
            is_multi = True
        elif raw_qty.isdigit() and int(raw_qty) > 1:
            is_multi = True
            
        is_repeat = (phone in repeat_phones) or (current_batch_phones.get(phone, 0) > 1)

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
            "paymentInformationList": [] if is_cod_flag else [{
                "paymentMethod": "Prepaid",
                "paymentAmount": {
                    "currencyCode": "INR",
                    "value": str(round(float(o.get("amount", 0) or 0), 2))
                }
            }]
        }

        is_err = "error" in str(fulfilled).lower() or "fail" in str(fulfilled).lower() or "error" in str(o.get("status", "")).lower()
        if state_code in ["UP", "UTTAR PRADESH", "DELHI", "NEW DELHI", "DL", "HARYANA", "HR"]:
            is_err = True

        # Check if any SKU in this order is blocked from MCF
        order_skus = [s.strip() for s in seller_sku.split(",") if s.strip()]
        is_mcf_blocked = any(sku in MCF_BLOCKED_SKUS for sku in order_skus)
        if is_mcf_blocked:
            if not issue:
                issue = f"MCF Blocked SKU"
            row["issue"] = issue
            is_err = True  # Treat as error so it won't be auto-selected

        if (not source and not fulfilled) or is_err:
            row["select"] = False if is_err else True
            row["is_error"] = is_err
            row["path"] = "MCF"
            if not is_mcf_blocked:  # Don't add blocked SKU orders to MCF pending queue
                pending.append(row)
        elif source:
            processed.append(row)

    return pending, processed, None


@st.cache_data(ttl=120)
def _cached_endpoint_snapshot():
    """Cached Apps Script fetch — safe for Streamlit Cloud (SQLite often empty)."""
    return fetch_endpoint_orders()


def ensure_sheet_orders_loaded():
    """Load pending/processed from sheet once per session (first page visit)."""
    if st.session_state.pending_df is not None:
        return
    pending, processed, err = _cached_endpoint_snapshot()
    st.session_state.pending_df = pd.DataFrame(pending) if pending else pd.DataFrame()
    st.session_state.processed_df = pd.DataFrame(processed) if processed else pd.DataFrame()
    st.session_state._endpoint_snapshot_error = err


def _parse_row_date_for_filter(val):
    if not val:
        return None
    s = str(val).strip()
    try:
        if "T" not in s and len(s) >= 10:
            s = s.replace(" ", "T", 1)
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def _report_safe_int_qty(val, default=1):
    """Same rules as db.safe_int_qty; local copy so Reports works if Cloud has older db.py."""
    if val is None or val == "":
        return default
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return int(val)
    s = str(val).strip()
    if not s:
        return default
    if s.isdigit():
        return int(s)
    parts = [p.strip() for p in s.split(",") if p.strip().isdigit()]
    if parts:
        return sum(int(p) for p in parts)
    try:
        return int(float(s))
    except ValueError:
        return default


def _report_safe_row_number(val, default=0):
    if val is None or val == "":
        return default
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return default


def sheet_row_to_report_dict(row) -> dict:
    """Map fetch_endpoint_orders row → same shape as DB for Reports."""
    fulfilled = str(row.get("fulfilled", "")).strip()
    source = str(row.get("source", "")).strip()
    tracking = str(row.get("tracking_no", "")).strip()
    sheet_st = str(row.get("status", "")).strip()
    # Sheet Q/R/V can all say Cancelled — must not treat as PROCESSING just because `source` is non-empty
    blob = f"{source.lower()} {fulfilled.lower()} {sheet_st.lower()}"
    cancelled = "cancel" in blob

    ch = ""
    su = source.upper()
    if "DELHI" in su:
        ch = "DELHIVERY"
    elif "MCF" in su:
        ch = "MCF"

    status = "NEW"
    fl = fulfilled.lower()
    st_l = sheet_st.lower()
    if cancelled:
        status = "CANCELLED"
    elif row.get("is_error") or "error" in fl or "fail" in fl or "error" in st_l or "fail" in st_l:
        status = "FAILED"
    elif "deliver" in fl or "deliver" in st_l:
        status = "DELIVERED"
    elif tracking:
        status = "SHIPPED"
    elif source and not cancelled:
        status = "PROCESSING"
    created = str(row.get("date", ""))
    return {
        "order_id": row.get("order_id", ""),
        "customer_name": row.get("customer", "") or "",
        "status": status,
        "sheet_status": sheet_st,
        "fulfillment_channel": ch,
        "tracking_number": tracking,
        "tracking_company": str(row.get("carrier", "")),
        "tracking_url": "",
        "total_amount": float(row.get("amount", 0) or 0),
        "created_at": created,
        "updated_at": created,
        "row_number": _report_safe_row_number(row.get("row_number", 0)),
        "seller_sku": str(row.get("seller_sku", "")),
        "title": str(row.get("title", ""))[:200],
        "qty": _report_safe_int_qty(row.get("qty", 1), 1),
        "is_cod": 1 if str(row.get("is_cod", "")).lower() in ("true", "yes", "1", "cod") else 0,
        "pincode": str(row.get("pincode", "")),
        "city": str(row.get("city", "")),
        "source_sheet_q": source,
        "sheet_fulfilled": fulfilled,
        "column_r": fulfilled,
        "source_channel": "SHOPIFY",
        "_data_source": "sheet",
    }


def merge_db_and_sheet_reports(db_orders: list, sheet_report_rows: list) -> list:
    """DB rows override sheet for same order_id; keep sheet column R for reporting."""
    by_oid = {}
    for r in sheet_report_rows:
        oid = str(r.get("order_id", "")).strip()
        if oid:
            row = dict(r)
            row["column_r"] = str(row.get("sheet_fulfilled", row.get("column_r", "")) or "")
            row["sheet_status"] = str(row.get("sheet_status", row.get("status", "")) or "")
            by_oid[oid] = row
    for r in db_orders:
        oid = str(r.get("order_id", "")).strip()
        if not oid:
            continue
        prev = by_oid.get(oid)
        d = dict(r)
        d["_data_source"] = "db"
        if prev:
            d["column_r"] = str(prev.get("column_r", prev.get("sheet_fulfilled", "")) or "")
            d["sheet_status"] = str(prev.get("sheet_status", "") or "")
            d["source_sheet_q"] = str(prev.get("source_sheet_q", "") or "")
        else:
            d["column_r"] = str(d.get("sheet_fulfilled", "") or "")
            d["sheet_status"] = str(d.get("sheet_status", "") or "")
        by_oid[oid] = d
    return list(by_oid.values())


def normalize_report_multiselect(selected: list, all_token: str = "All"):
    """If user picks All + PROCESSING, ignore All and apply PROCESSING only."""
    if not selected:
        return None
    specific = [x for x in selected if x != all_token]
    if not specific:
        return None
    return specific


def row_matches_column_r_filter(column_r_value, categories: list) -> bool:
    """OR semantics: row passes if it matches any selected R category."""
    if not categories:
        return True
    rv = str(column_r_value or "").strip()
    rv_l = rv.lower()
    for tag in categories:
        if tag == "Blank R":
            if not rv:
                return True
        elif tag == "Non-blank R":
            if rv:
                return True
        elif tag == "FULFILLED (exact)":
            if rv.upper() == "FULFILLED":
                return True
        elif tag == "Includes 'Planning'":
            if "planning" in rv_l:
                return True
        elif tag == "Includes 'ful'":
            if "ful" in rv_l:
                return True
        elif tag == "Includes 'MCF:'":
            if "mcf:" in rv_l:
                return True
        elif tag == "Includes 'cancel'":
            if "cancel" in rv_l:
                return True
        elif tag == "Includes 'error' or 'fail'":
            if "error" in rv_l or "fail" in rv_l:
                return True
    return False


def apply_report_filters_python(
    rows,
    status_filter,
    channel_filter,
    date_from,
    date_to,
    search,
    r_column_filter=None,
    result_limit: int = 5000,
) -> list:
    """Same rules as db.get_orders_filtered, for merged in-memory rows."""
    out = []
    for r in rows:
        if status_filter:
            stv = str(r.get("status", "") or "")
            if isinstance(status_filter, list):
                if stv not in status_filter:
                    continue
            elif stv != status_filter:
                continue
        if channel_filter:
            ch = str(r.get("fulfillment_channel", "") or "").upper()
            allowed = [cf.upper() for cf in channel_filter]
            match = ("DELHIVERY" in allowed and ch == "DELHIVERY") or ("MCF" in allowed and ch == "MCF")
            if not match:
                continue
        if r_column_filter and not row_matches_column_r_filter(r.get("column_r", ""), r_column_filter):
            continue
        if date_from or date_to:
            rd = _parse_row_date_for_filter(r.get("created_at") or r.get("updated_at"))
            if rd is None:
                if date_from:
                    continue
            else:
                if date_from and rd < date_from:
                    continue
                if date_to and rd > date_to:
                    continue
        if search:
            s = search.lower()
            oid = str(r.get("order_id", "")).lower()
            cname = str(r.get("customer_name", "")).lower()
            trk = str(r.get("tracking_number", "")).lower()
            rcol = str(r.get("column_r", "")).lower()
            sst = str(r.get("sheet_status", "")).lower()
            if s not in oid and s not in cname and s not in trk and s not in rcol and s not in sst:
                continue
        out.append(r)
    out.sort(key=lambda x: str(x.get("created_at") or x.get("updated_at") or ""), reverse=True)
    return out[: int(result_limit)]


def compute_sheet_dashboard_stats(pending: list, processed: list) -> dict:
    """Approximate db.get_stats() from live sheet rows (for Streamlit Cloud)."""
    all_r = list(pending) + list(processed)
    if not all_r:
        return db.get_stats()
    with_trk = sum(1 for r in all_r if str(r.get("tracking_no", "")).strip())
    mcf = sum(1 for r in all_r if "MCF" in str(r.get("source", "")).upper())
    delh = sum(1 for r in all_r if "DELHI" in str(r.get("source", "")).upper())
    failed = sum(1 for r in all_r if r.get("is_error") or "error" in str(r.get("fulfilled", "")).lower() or "fail" in str(r.get("fulfilled", "")).lower())
    cod = sum(1 for r in all_r if str(r.get("is_cod", "")).lower() in ("true", "yes", "1", "cod"))
    shopify_fu = sum(1 for r in all_r if "ful" in str(r.get("fulfilled", "")).lower())
    today = datetime.now().strftime("%Y-%m-%d")
    week_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    month_start = datetime.now().strftime("%Y-%m-01")
    week_cut = datetime.strptime(week_start, "%Y-%m-%d").date()
    month_cut = datetime.strptime(month_start, "%Y-%m-%d").date()
    today_d = datetime.now().date()

    today_orders = 0
    week_orders = 0
    month_orders = 0
    for r in all_r:
        d = _parse_row_date_for_filter(r.get("date"))
        if d is None:
            continue
        if d == today_d:
            today_orders += 1
        if d >= week_cut:
            week_orders += 1
        if d >= month_cut:
            month_orders += 1

    proc_no_trk = [r for r in processed if not str(r.get("tracking_no", "")).strip()]
    return {
        "total": len(all_r),
        "pending": len(pending),
        "processing": len(proc_no_trk),
        "shipped": with_trk,
        "fulfilled": shopify_fu,
        "failed": failed,
        "mcf_count": mcf,
        "delhivery_count": delh,
        "cod_count": cod,
        "prepaid_count": max(0, len(all_r) - cod),
        "shopify_fulfilled": shopify_fu,
        "with_tracking": with_trk,
        "without_tracking": sum(1 for r in all_r if "MCF" in str(r.get("source", "")).upper() and not str(r.get("tracking_no", "")).strip()),
        "today_orders": today_orders,
        "week_orders": week_orders,
        "month_orders": month_orders,
        "avg_processing_hours": 0.0,
    }


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
    ensure_sheet_orders_loaded()
    err = st.session_state.get("_endpoint_snapshot_error")
    if err:
        st.error(f"Sheet endpoint: {err}")

    p_df = st.session_state.pending_df
    r_df = st.session_state.processed_df
    p_list = p_df.to_dict("records") if p_df is not None and not p_df.empty else []
    r_list = r_df.to_dict("records") if r_df is not None and not r_df.empty else []

    db_stats = db.get_stats()
    sheet_stats = compute_sheet_dashboard_stats(p_list, r_list)
    # Live sheet is source of truth for counts on Streamlit Cloud; DB enriches fulfilled orders.
    stats = sheet_stats if (p_list or r_list) else db_stats
    if (p_list or r_list) and db_stats.get("total", 0) > 0:
        st.caption(
            f"Top metrics from **Google Sheet** (cached ~2 min). "
            f"App SQLite: **{db_stats['total']}** orders with fulfillment detail on this server."
        )

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
    aph = stats["avg_processing_hours"]
    if not (p_list or r_list):
        cols3[3].metric("Avg Processing (hrs)", aph)
    else:
        cols3[3].metric("Avg Processing (hrs)", aph, help="From SQLite only; sheet view has no dwell time.")
    cols3[4].metric("COD / Prepaid", f"{stats['cod_count']} / {stats['prepaid_count']}")

    st.markdown("---")

    # Row 4 — tracking
    cols4 = st.columns(2)
    cols4[0].metric("With Tracking ID", stats["with_tracking"])
    cols4[1].metric("MCF Awaiting Tracking", stats["without_tracking"])

    # Recent orders (sheet-backed when available)
    st.subheader("Recent Orders")
    all_live = p_list + r_list
    if all_live:
        all_live = sorted(all_live, key=lambda x: str(x.get("date", "")), reverse=True)[:20]
        rec_df = pd.DataFrame(all_live)
        show_cols = ["order_id", "customer", "source", "tracking_no", "fulfilled", "date"]
        show_cols = [c for c in show_cols if c in rec_df.columns]
        st.dataframe(rec_df[show_cols], width="stretch", hide_index=True)
    else:
        recent = db.get_orders_filtered(limit=20)
        if recent:
            df = pd.DataFrame(recent)
            show_cols = ["order_id", "customer_name", "status", "fulfillment_channel", "tracking_number", "created_at"]
            show_cols = [c for c in show_cols if c in df.columns]
            st.dataframe(df[show_cols], width='stretch', hide_index=True)
        else:
            st.info("No orders from sheet yet. Check Secrets / Apps Script URL or open **Orders**.")


# ─────────────────────────────────────────────
# PAGE 2: ORDERS
# ─────────────────────────────────────────────
def page_orders():
    st.title("📋 Orders")
    ensure_sheet_orders_loaded()
    if st.session_state.get("_endpoint_snapshot_error"):
        st.warning(f"Sheet endpoint: {st.session_state._endpoint_snapshot_error}")

    if st.button("🔄 Refresh from Endpoint", type="primary"):
        _cached_endpoint_snapshot.clear()
        with st.spinner("Fetching from Google Sheet endpoint..."):
            pending, processed, err = fetch_endpoint_orders()
            if err:
                st.error(err)
            else:
                st.session_state.pending_df = pd.DataFrame(pending) if pending else pd.DataFrame()
                st.session_state.processed_df = pd.DataFrame(processed) if processed else pd.DataFrame()
                st.session_state.processing_log = []
                st.session_state._endpoint_snapshot_error = None
                st.success(f"✅ Pending: {len(pending)} | Already Processed: {len(processed)}")

    tab1, tab2 = st.tabs(["⏳ Pending Orders", "✅ Already Processed"])

    # ── TAB 1: PENDING ──────────────────────────────────────────────────
    with tab1:
        df = st.session_state.pending_df
        if df is None or df.empty:
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
                    width='stretch',
                    num_rows="fixed",
                    key=f"pending_editor_{key}",
                )

            def render_tab_buttons(mask_series, key_prefix):
                b1, b2, _ = st.columns([1.5, 1.5, 4])
                if b1.button("☑️ Select All", key=f"sel_{key_prefix}"):
                    st.session_state.pending_df.loc[mask_series, "select"] = True
                    st.rerun()
                if b2.button("🔲 Unselect All", key=f"unsel_{key_prefix}"):
                    st.session_state.pending_df.loc[mask_series, "select"] = False
                    st.rerun()

            t_single, t_multi, t_repeat, t_error = st.tabs(["⏳ Single Orders", "📦 Multi SKU/Unit Orders", "⚠️ Repeated Customers", "❌ Error / Retry Orders"])
            
            with t_single:
                mask1 = (~df["is_multi"]) & (~df["is_repeat"]) & (~df.get("is_error", False))
                render_tab_buttons(mask1, "single")
                edit1 = render_grid(df[mask1], "single")
            with t_multi:
                mask2 = (df["is_multi"]) & (~df["is_repeat"]) & (~df.get("is_error", False))
                render_tab_buttons(mask2, "multi")
                edit2 = render_grid(df[mask2], "multi")
            with t_repeat:
                mask3 = (df["is_repeat"]) & (~df.get("is_error", False))
                render_tab_buttons(mask3, "rep")
                edit3 = render_grid(df[mask3], "repeat")
            with t_error:
                mask4 = df.get("is_error", False)
                if isinstance(mask4, bool): mask4 = pd.Series(mask4, index=df.index)
                render_tab_buttons(mask4, "err")
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
            edit_map = edit_df.drop_duplicates(subset=["order_id"]).set_index("order_id")[["path", "addr_line1", "addr_line2", "addr_line3", "phone", "pincode"]].to_dict("index")
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
        if df2 is None or df2.empty:
            st.info("No processed orders found.")
        else:
            show = ["order_id", "customer", "source", "fulfilled", "tracking_no", "carrier", "status"]
            show = [c for c in show if c in df2.columns]
            st.metric("Total Processed", len(df2))
            st.dataframe(df2[show], width='stretch', hide_index=True)

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
        st.dataframe(pd.DataFrame(log_rows), width='stretch', hide_index=True)

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

                # ── Check Shopify Fulfillment Status Before Processing ──
                s_order = get_shopify_order(order_id, shopify_cfg["headers"], shopify_cfg["shop_url"])
                if s_order and str(s_order.get("fulfillment_status", "")).lower() == "fulfilled":
                    st.toast(f"{order_id} already fulfilled in Shopify — syncing sheet…", icon="ℹ️")
                    ok_sync, msg_sync, trk = _sync_shopify_fulfilled_row_to_sheet(
                        order_id,
                        int(row["row_number"]),
                        str(row.get("source", "")),
                        shopify_cfg,
                        sheets_service,
                    )
                    if not ok_sync:
                        db.update_order_status(order_id, "ALREADY_FULFILLED", reason=msg_sync)
                        log.append({
                            "order_id": order_id, "path": "MCF", "ok": False,
                            "msg": msg_sync, "shopify": "Fulfilled", "tracking": "—",
                        })
                    else:
                        if not trk:
                            db.update_order_status(order_id, "ALREADY_FULFILLED", reason="Shopify sync (no tracking)")
                        log.append({
                            "order_id": order_id, "path": "MCF", "ok": True,
                            "msg": msg_sync, "shopify": "Fulfilled", "tracking": trk or "—",
                        })
                    done += 1
                    progress.progress(done / total)
                    continue

                skus = [s.strip() for s in str(row.get("seller_sku", "")).split(",") if s.strip()]
                raw_qty = str(row.get("raw_qty", row.get("qty", "1")))
                qtys_str = [q.strip() for q in raw_qty.split(",") if q.strip()]

                def parse_qty(q_str):
                    try:
                        return int(float(q_str))
                    except ValueError:
                        return 1

                if len(qtys_str) == 1 and len(skus) > 1:
                    qtys = [parse_qty(qtys_str[0])] * len(skus)
                else:
                    qtys = [parse_qty(q) for q in qtys_str]

                while len(qtys) < len(skus):
                    qtys.append(1)

                items = []
                # Distribute amount across total units correctly so perUnitDeclaredValue is accurate
                total_units = sum(qtys)
                amount_per_item = float(row["amount"]) / max(1, total_units)
                
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
                    sheet_updates.append({"row": row["row_number"], "source": "MCF", "status": "FULFILLED"})

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
        del_keys = [
            secrets.get("DELHIVERY_API_KEY", ""),
            secrets.get("DELHIVERY_API_KEY2", "")
        ]
        del_keys = [k for k in del_keys if k]
        
        if not del_keys:
            st.error("DELHIVERY_API_KEY missing in secret.txt or secrets")
        else:
            for _, row in del_sel.iterrows():
                order_id = row["order_id"]
                status_text.text(f"Delhivery: Processing {order_id}...")

                # ── Check Shopify Fulfillment Status Before Processing ──
                s_order = get_shopify_order(order_id, shopify_cfg["headers"], shopify_cfg["shop_url"])
                if s_order and str(s_order.get("fulfillment_status", "")).lower() == "fulfilled":
                    st.toast(f"{order_id} already fulfilled in Shopify — syncing sheet…", icon="ℹ️")
                    ok_sync, msg_sync, trk = _sync_shopify_fulfilled_row_to_sheet(
                        order_id,
                        int(row["row_number"]),
                        str(row.get("source", "")),
                        shopify_cfg,
                        sheets_service,
                    )
                    if not ok_sync:
                        db.update_order_status(order_id, "ALREADY_FULFILLED", reason=msg_sync)
                        log.append({
                            "order_id": order_id, "path": "Delhivery", "ok": False,
                            "msg": msg_sync, "shopify": "Fulfilled", "tracking": "—",
                        })
                    else:
                        if not trk:
                            db.update_order_status(order_id, "ALREADY_FULFILLED", reason="Shopify sync (no tracking)")
                        log.append({
                            "order_id": order_id, "path": "Delhivery", "ok": True,
                            "msg": msg_sync, "shopify": "Fulfilled", "tracking": trk or "—",
                        })
                    done += 1
                    progress.progress(done / total)
                    continue

                order_data = dict(row) | {"total_qty": int(row["qty"])}
                success, resp, err = create_delhivery_order(
                    del_keys, order_data,
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

                    sheet_updates.append({"row": row["row_number"], "source": "Delhivery", "status": "FULFILLED"})

                    # Delhivery waybill → Queue sheet tracking update (S/T/U)
                    if waybill and row.get("row_number"):
                        sheet_tracking_updates.append({
                            "row": row["row_number"],
                            "carrier": "Delhivery",
                            "tracking_no": waybill,
                            "url": f"https://www.delhivery.com/track/package/{waybill}",
                            "fill_source_q": False,
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
        status_text.text("Updating Google Sheet (R/S columns)...")
        try:
            update_sheet_remarks(sheets_service, SHEET_ID, sheet_updates)
            db.log_sync("SHEET_REMARKS", "SUCCESS", f"{len(sheet_updates)} rows updated")
        except Exception as e:
            st.error(f"Sheet R/S update failed: {e}")
            db.log_sync("SHEET_REMARKS", "ERROR", str(e)[:300])

    if sheet_tracking_updates:
        status_text.text("Updating Google Sheet (T/U/V/W tracking columns)...")
        try:
            update_sheet_tracking(sheets_service, SHEET_ID, sheet_tracking_updates)
            db.log_sync("SHEET_TRACKING", "SUCCESS", f"{len(sheet_tracking_updates)} rows updated")
        except Exception as e:
            st.error(f"Sheet T/U/V/W update failed: {e}")

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


def row_indicates_fulfilled_for_mcf_lookup(fulfilled_str: str) -> bool:
    """Column S (fulfilled) must suggest fulfillment before we call MCF / Delhivery AWB APIs."""
    s = (fulfilled_str or "").lower()
    return "ful" in s or "plan" in s or "process" in s or "mcf" in s


def row_indicates_fulfilled_for_mcf_lookup(fulfilled_str: str) -> bool:
    """Column S (fulfilled) must suggest fulfillment before we call MCF / Delhivery AWB APIs."""
    s = (fulfilled_str or "").lower()
    return "ful" in s or "plan" in s or "process" in s or "mcf" in s


AWB_FETCH_BATCH_SIZE = 10
AWB_LOOKUP_TIMEOUT_SEC = 22


def _lookup_awb_with_timeout(order_id, secrets, mcf_token, timeout_sec=AWB_LOOKUP_TIMEOUT_SEC):
    """Cap per-order lookup so Streamlit never hangs 90s+ on slow iThink scan."""
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(
            lookup_awb_by_order_id,
            order_id,
            secrets=secrets,
            mcf_token=mcf_token,
        )
        try:
            return fut.result(timeout=timeout_sec)
        except FutTimeout:
            return "", "", "", f"Timeout ({timeout_sec}s) — deploy latest live_tracker.py"


def _awb_fetch_flush_buffers(sheets_svc, track_buf, qr_ok_buf, qr_fail_buf):
    if track_buf:
        for su in track_buf:
            su["fill_source_q"] = False
        update_sheet_tracking(sheets_svc, SHEET_ID, track_buf)
    all_qr = qr_ok_buf + qr_fail_buf
    if all_qr:
        update_sheet_remarks(sheets_svc, SHEET_ID, all_qr)


def _awb_fetch_process_one(order, secrets, token, shopify_cfg):
    """Process single pending order; return (result_row, track_upd, qr_ok, qr_fail)."""
    order_id = order["order_id"]
    track_upd = qr_ok = qr_fail = None

    if not row_indicates_fulfilled_for_mcf_lookup(order.get("fulfilled", "")):
        return ({
            "Order ID": order_id,
            "Customer": order["customer"],
            "Status": "Skipped — column R has no 'ful'",
            "Tracking ID": "—",
            "Carrier": "—",
            "Shopify": "—",
            "Sheet": "— (unchanged)",
        }, None, None, None)

    tn, cc, src_label, detail_status = _lookup_awb_with_timeout(
        order_id, secrets=secrets, mcf_token=token
    )

    if tn:
        remark = f"Tracking Added {datetime.now().strftime('%d/%m %H:%M')}"
        if detail_status and detail_status not in (
            "Found on iThink", "Found on Delhivery", "Found on MCF"
        ):
            remark = f"{remark} | {detail_status}"
        db.update_order_tracking(order_id, cc or src_label, tn, "")
        t_info = {
            "number": tn,
            "company": cc or src_label,
            "url": build_tracking_url(cc or src_label, tn),
        }
        try:
            s_ok, s_msg = _shopify_fulfill(order_id, shopify_cfg, tracking_info=t_info)
        except Exception as e:
            s_ok, s_msg = False, str(e)[:80]

        sheet_src = src_label or infer_sheet_source_q(cc, tn)
        track_upd = {
            "row": order["row_number"],
            "carrier": cc or src_label,
            "tracking_no": tn,
            "url": t_info.get("url", ""),
            "remark": remark,
        }
        qr_ok = {"row": order["row_number"], "source": sheet_src, "status": "FULFILLED"}
        return ({
            "Order ID": order_id,
            "Customer": order["customer"],
            "Status": detail_status or src_label,
            "Tracking ID": tn,
            "Carrier": cc or src_label,
            "Shopify": "✅ Fulfilled" if s_ok else f"⚠️ {s_msg}",
            "Sheet": "✅ Updated",
        }, track_upd, qr_ok, qr_fail)

    orig_source = str(order.get("source", "")).upper()
    if "DELHI" in orig_source:
        status_label = "Delhivery: Not Found"
    else:
        status_label = {
            "Planning": "MCF: Planning",
            "Received": "MCF: Received",
            "Processing": "MCF: Processing",
            "Complete": "MCF: Complete",
            "Cancelled": "MCF: Cancelled",
            "NotFound": "MCF: Not Found",
            "Unfulfillable": "MCF: Unfulfillable",
        }.get(detail_status, f"MCF: {detail_status}" if detail_status else "Not Found")

    track_upd = {
        "row": order["row_number"],
        "carrier": "",
        "tracking_no": "",
        "url": "",
        "remark": status_label,
    }
    qr_fail = {
        "row": order["row_number"],
        "source": "Delhivery" if "DELHI" in orig_source else "MCF",
        "status": "",
    }
    return ({
        "Order ID": order_id,
        "Customer": order["customer"],
        "Status": detail_status or status_label,
        "Tracking ID": "—",
        "Carrier": "—",
        "Shopify": "—",
        "Sheet": f"✅ V: {status_label}",
    }, track_upd, qr_ok, qr_fail)


def _awb_fetch_run_batch(secrets, progress_slot, status_slot):
    """Process next AWB_FETCH_BATCH_SIZE orders; auto-continue via session state."""
    queue = st.session_state.get("awb_fetch_queue") or []
    idx = int(st.session_state.get("awb_fetch_idx") or 0)
    total = len(queue)
    if total == 0 or idx >= total:
        st.session_state.awb_fetch_running = False
        return True

    token, err = get_fresh_token()
    if err:
        st.error(f"Auth failed: {err}")
        st.session_state.awb_fetch_running = False
        return True

    shopify_cfg = get_shopify_config(secrets)
    try:
        sheets_svc = init_sheets_service()
    except Exception as e:
        st.error(f"Sheets init failed: {e}")
        st.session_state.awb_fetch_running = False
        return True

    results = st.session_state.setdefault("awb_fetch_results", [])
    track_buf = []
    qr_ok_buf = []
    qr_fail_buf = []

    end = min(idx + AWB_FETCH_BATCH_SIZE, total)
    for i in range(idx, end):
        order = queue[i]
        status_slot.text(
            f"Order {order['order_id']} ({i + 1}/{total}) — iThink → Delhivery → MCF..."
        )
        progress_slot.progress((i + 1) / total)
        row, tr, qok, qfail = _awb_fetch_process_one(order, secrets, token, shopify_cfg)
        results.append(row)
        if tr:
            track_buf.append(tr)
        if qok:
            qr_ok_buf.append(qok)
        if qfail:
            qr_fail_buf.append(qfail)

    try:
        _awb_fetch_flush_buffers(sheets_svc, track_buf, qr_ok_buf, qr_fail_buf)
    except Exception as e:
        st.warning(f"Sheet batch save failed: {e}")

    st.session_state.awb_fetch_idx = end
    return end >= total


def _sync_shopify_fulfilled_row_to_sheet(order_id, row_number, row_source, shopify_cfg, sheets_service):
    """Shopify already fulfilled: pull fulfillments, sync S/T/U/V and Q/R (R=FULFILLED). Returns (ok, message, tracking_or_none)."""
    s_order = get_shopify_order(order_id, shopify_cfg["headers"], shopify_cfg["shop_url"])
    if not s_order:
        return False, "Order not found on Shopify", None
    shop_url = shopify_cfg["shop_url"]
    headers = shopify_cfg["headers"]
    f_url = f"{shop_url}/admin/api/2024-01/orders/{s_order['id']}/fulfillments.json"
    try:
        fr = requests.get(f_url, headers=headers, timeout=30)
        fr.raise_for_status()
        fulfillments = fr.json().get("fulfillments", [])
    except Exception as e:
        return False, str(e)[:120], None

    tracking_no = ""
    carrier = ""
    for f in reversed(fulfillments):
        if f.get("status") not in ("success", "pending", "open"):
            continue
        tn = (f.get("tracking_number") or "").strip()
        if tn:
            tracking_no = tn
            carrier = (f.get("tracking_company") or "").strip()
            break
    if not tracking_no:
        for f in reversed(fulfillments):
            if f.get("status") not in ("success", "pending", "open"):
                continue
            tracking_no = (f.get("tracking_number") or "").strip()
            carrier = (f.get("tracking_company") or "").strip()
            break

    db.mark_shopify_fulfilled(order_id)
    if tracking_no:
        src = infer_sheet_source_q(carrier, tracking_no)
    else:
        src = (row_source or "").strip() or "Shopify"
    update_sheet_remarks(sheets_service, SHEET_ID, [{"row": row_number, "source": src, "status": "FULFILLED"}])

    now_r = datetime.now().strftime("%d/%m %H:%M")
    if tracking_no:
        db.update_order_tracking(order_id, carrier or "Shopify", tracking_no, "")
        update_sheet_tracking(sheets_service, SHEET_ID, [{
            "row": row_number,
            "carrier": carrier or "",
            "tracking_no": tracking_no,
            "url": "",
            "remark": f"Shopify sync {now_r}",
            "fill_source_q": False,
        }])
        return True, f"Synced tracking {tracking_no}", tracking_no

    update_sheet_tracking(sheets_service, SHEET_ID, [{
        "row": row_number,
        "carrier": "",
        "tracking_no": "",
        "url": "",
        "remark": "Shopify fulfilled — no tracking on fulfillment",
        "fill_source_q": False,
    }])
    return True, "Shopify fulfilled — no tracking on fulfillment", None


def _compute_planning_stale_rows(sheets_service, min_age_days: int = 2):
    """MCF rows with Planning in R/V and order date (col C or header) older than min_age_days."""
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Sheet1!A:AF"
    ).execute()
    rows = result.get("values", [])
    if len(rows) <= 1:
        return []

    headers = [str(h).strip().lower() for h in rows[0]]

    def get_idx(*names):
        for name in names:
            key = name.strip().lower()
            if key in headers:
                return headers.index(key)
        return -1

    source_idx = get_idx("source")
    order_id_idx = get_idx("ord_serial", "order id", "order")
    date_idx = get_idx("date", "order date")
    if date_idx == -1:
        date_idx = 2
    fulfilled_idx = get_idx("fulfilled", "fulfillment", "column r")
    remark_idx = get_idx("remark", "remarks", "v")

    today = datetime.now().date()
    stale = []
    for i, row in enumerate(rows[1:], start=2):

        def sg(idx):
            if idx >= 0 and idx < len(row):
                return str(row[idx]).strip()
            return ""

        source = sg(source_idx)
        if "MCF" not in source.upper():
            continue

        r_text = sg(fulfilled_idx) if fulfilled_idx >= 0 else ""
        v_text = sg(remark_idx) if remark_idx >= 0 else ""
        status_blob = f"{r_text} {v_text}"
        if "planning" not in status_blob.lower():
            continue

        date_raw = sg(date_idx)
        if not date_raw:
            continue
        try:
            c = date_raw.strip()
            if "T" not in c and len(c) >= 10:
                c = c.replace(" ", "T", 1)
            order_date = datetime.fromisoformat(c.replace("Z", "+00:00")).date()
        except ValueError:
            try:
                order_date = datetime.strptime(c[:10], "%Y-%m-%d").date()
            except ValueError:
                continue

        age = (today - order_date).days
        if age <= min_age_days:
            continue

        oid = sg(order_id_idx).replace("#", "").strip()
        stale.append({
            "order_id": oid,
            "row": i,
            "order_date": date_raw[:32],
            "age_days": age,
            "column_r": r_text,
            "column_v": v_text,
        })
    stale.sort(key=lambda x: -x["age_days"])
    return stale


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
        "ℹ️ **Live Tracker:** Sheet ke column T (AWB) se live status track karta hai — "
        "Delhivery / iThink / Amazon. 1000+ rows ke liye andar se batch mein chalega "
        "(timeout / sheet API limit se bachne ke liye)."
    )
    batch_size = st.selectbox(
        "Rows per internal batch",
        options=[100, 150, 200, 250],
        index=1,
        help="Har batch ke baad sheet save hoti hai — beech mein ruke to bhi data safe.",
    )

    if st.button("▶ Run Full Live Tracking Update", type="primary"):
        prog = st.progress(0)
        status_txt = st.empty()
        all_res = []
        start = 0

        with st.spinner("Fetching data and querying carrier APIs..."):
            while True:
                def make_cb(offset):
                    def cb(idx, total, no):
                        prog.progress(min(1.0, (idx + 1) / max(total, 1)))
                        status_txt.text(f"Tracking: {no} ({idx + 1}/{total})")
                    return cb

                chunk = run_live_tracking_update(
                    progress_callback=make_cb(start),
                    start_index=start,
                    max_count=batch_size,
                )

                if chunk and str(chunk[0].get("Order ID", chunk[0].get("order_id", ""))).lower() == "error":
                    st.error(chunk[0].get("Status", chunk[0].get("status", "Unknown error")))
                    break

                if not chunk:
                    break

                all_res.extend(chunk)
                if len(chunk) < batch_size:
                    break
                start += len(chunk)
                time.sleep(0.4)

        status_txt.text(f"✅ Done — {len(all_res)} row(s) processed")
        prog.progress(1.0)
        if all_res:
            st.dataframe(pd.DataFrame(all_res), width="stretch", hide_index=True)
        else:
            st.warning("No tracking numbers found to update.")

def _render_awb_fetch():
    st.info(
        "ℹ️ **AWB lookup order:** iThink → Delhivery → MCF (last). "
        "Jo ship ho chuke hain unka AWB milega + Shopify + Sheet update."
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

    with st.expander("📅 Planning stale (MCF + Planning, order date > 2 days)", expanded=False):
        st.caption("Uses column **C** for order date if no Date / Order date header is found; checks Source = MCF and **Planning** in column R and/or remark column.")
        if st.button("Refresh planning-stale list", key="plan_stale_refresh"):
            st.session_state.pop("_planning_stale_cache", None)
        try:
            if "_planning_stale_cache" not in st.session_state:
                st.session_state._planning_stale_cache = _compute_planning_stale_rows(init_sheets_service())
            stale_df = st.session_state._planning_stale_cache
            if stale_df:
                st.dataframe(pd.DataFrame(stale_df), width="stretch", hide_index=True)
            else:
                st.info("No matching rows.")
        except Exception as e:
            st.error(f"Could not load planning-stale rows: {e}")

    # ── Tabs: Pending | Already have tracking ────────────────────────────
    t1, t2 = st.tabs([f"⏳ Pending ({len(need_trk)})", f"✅ Tracking Added ({len(has_trk)})"])

    with t1:
        if not need_trk:
            st.success("Sab orders ka tracking add ho chuka hai!")
        else:
            df_need = pd.DataFrame(need_trk)[["order_id", "customer", "amount", "row_number"]]
            st.dataframe(df_need, width='stretch', hide_index=True)

            st.markdown("---")
            btn_col1, btn_col2 = st.columns([2, 3])
            st.caption(
                f"Har {AWB_FETCH_BATCH_SIZE} orders ke baad auto-save + page reload (Streamlit timeout se bachne ke liye). "
                "Atka dikhe to **Stop** → refresh → dubara Fetch."
            )

            if st.session_state.get("awb_fetch_running"):
                prog_run = st.progress(
                    int(st.session_state.get("awb_fetch_idx") or 0)
                    / max(len(st.session_state.get("awb_fetch_queue") or []), 1)
                )
                status_run = st.empty()
                done = _awb_fetch_run_batch(secrets, prog_run, status_run)
                if done:
                    st.session_state.awb_fetch_running = False
                    results = st.session_state.get("awb_fetch_results") or []
                    total_done = len(st.session_state.get("awb_fetch_queue") or [])
                    found_count = sum(
                        1 for r in results if r.get("Tracking ID") not in ("—", "")
                    )
                    status_run.text("✅ Done!")
                    prog_run.progress(1.0)
                    st.subheader("Tracking Fetch Results")
                    rc1, rc2 = st.columns(2)
                    rc1.metric("Tracking Found", found_count)
                    rc2.metric("Still Pending", total_done - found_count)
                    if results:
                        st.dataframe(pd.DataFrame(results), width="stretch", hide_index=True)
                    st.session_state.tracking_sheet_orders = None
                    st.info("Reload ke liye upar 'Load MCF Orders from Sheet' dobara click karo.")
                else:
                    time.sleep(0.3)
                    st.rerun()

            if btn_col1.button(
                "🔍 Fetch Tracking for All",
                type="primary",
                key="fetch_all_btn",
                disabled=bool(st.session_state.get("awb_fetch_running")),
            ):
                st.session_state.awb_fetch_running = True
                st.session_state.awb_fetch_idx = 0
                st.session_state.awb_fetch_results = []
                st.session_state.awb_fetch_queue = list(need_trk)
                st.rerun()

            if st.session_state.get("awb_fetch_running") and btn_col2.button(
                "⏹ Cancel Fetch", key="cancel_awb_fetch"
            ):
                st.session_state.awb_fetch_running = False
                st.session_state.pop("awb_fetch_queue", None)
                st.rerun()

            # ── SINGLE ORDER manual check ─────────────────────────────────
            with btn_col2:
                with st.expander("🔎 Single Order Check"):
                    manual_id = st.text_input("Order ID (e.g. 2860)", key="manual_track_id")
                    if st.button("Check", key="manual_check_btn") and manual_id:
                        token2, err2 = get_fresh_token()
                        if token2:
                            oid = manual_id.strip().replace("#", "")
                            st.write("**Lookup:** iThink → Delhivery → MCF")
                            tn, cc, src_label, detail = _lookup_awb_with_timeout(
                                oid, secrets=secrets, mcf_token=token2
                            )
                            st.write(f"**Result:** `{detail or 'Not found'}` | Source: **{src_label or '—'}**")
                            if tn:
                                st.success(f"✅ Tracking: **{tn}** | Carrier: {cc}")
                                db.update_order_tracking(oid, cc or src_label, tn, "")
                                s_ok, s_msg = _shopify_fulfill(
                                    oid,
                                    get_shopify_config(secrets),
                                    tracking_info={
                                        "number": tn,
                                        "company": cc or src_label,
                                        "url": build_tracking_url(cc or src_label, tn),
                                    },
                                )
                                st.info(f"Shopify: {'✅ Fulfilled' if s_ok else '⚠️ ' + s_msg}")
                                o_meta = next((o for o in need_trk if o["order_id"] == oid), None)
                                if o_meta and o_meta.get("row_number"):
                                    try:
                                        from datetime import datetime as _dt
                                        svc = init_sheets_service()
                                        sheet_src = src_label or infer_sheet_source_q(cc, tn)
                                        update_sheet_remarks(svc, SHEET_ID, [{
                                            "row": o_meta["row_number"],
                                            "source": sheet_src,
                                            "status": "FULFILLED",
                                        }])
                                        update_sheet_tracking(svc, SHEET_ID, [{
                                            "row": o_meta["row_number"],
                                            "carrier": cc or src_label,
                                            "tracking_no": tn,
                                            "url": build_tracking_url(cc or src_label, tn),
                                            "remark": f"Tracking Added {_dt.now().strftime('%d/%m %H:%M')}",
                                            "fill_source_q": False,
                                        }])
                                        st.success("✅ Sheet updated (Q/R + S/T/U/V)")
                                    except Exception as e:
                                        st.warning(f"Sheet failed: {e}")
                            else:
                                st.warning(f"⏳ No tracking yet. Detail: `{detail}`")
                        else:
                            st.error(f"Auth: {err2}")

    with t2:
        if not has_trk:
            st.info("Abhi kisi order ka tracking nahi aaya.")
        else:
            df_has = pd.DataFrame(has_trk)[["order_id", "customer", "tracking_no", "carrier", "fulfilled"]]
            st.dataframe(df_has, width='stretch', hide_index=True)


# ─────────────────────────────────────────────
# PAGE 4: REPORTS
# ─────────────────────────────────────────────
def page_reports():
    st.title("📈 Reports & Export")
    ensure_sheet_orders_loaded()
    if st.session_state.get("_endpoint_snapshot_error"):
        st.warning(f"Sheet: {st.session_state._endpoint_snapshot_error}")

    st.caption(
        "**Merged:** Google Sheet (Apps Script, ~2 min cache) + app **SQLite** `oms.db`. "
        "Same `order_id` par DB row sheet ko override karti hai (fulfillment trail)."
    )

    with st.expander("🔍 Filters", expanded=True):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            status_opts = ["All", "NEW", "PROCESSING", "SHIPPED", "DELIVERED", "FAILED", "CANCELLED"]
            sel_status = st.multiselect(
                "Status",
                status_opts,
                default=["All"],
                help="Agar **All** ke saath koi status chunein to sirf wahi statuses lagenge — `All` ignore ho jata hai.",
            )
        with fc2:
            ch_opts = ["All", "MCF", "DELHIVERY"]
            sel_channel = st.multiselect(
                "Channel",
                ch_opts,
                default=["All"],
                help="`All` + `MCF` = sirf MCF rows.",
            )
        with fc3:
            search = st.text_input("Search (Order ID / Customer / Tracking / R)")

        dc1, dc2, dc3 = st.columns(3)
        with dc1:
            apply_from = st.checkbox(
                "From date limit",
                value=False,
                key="rep_apply_from",
                help="✓ = niche wali date se filter. **Bina ✓ = shuru se** (minimum limit nahi).",
            )
            date_from = st.date_input(
                "From Date",
                value=datetime.now().date().replace(day=1),
                key="report_date_from",
                disabled=not apply_from,
                help="Sirf jab upar 'From date limit' tick ho.",
            )
        with dc2:
            apply_to = st.checkbox(
                "To date limit",
                value=False,
                key="rep_apply_to",
                help="✓ = niche wali date tak filter. **Bina ✓ = koi end limit nahi**.",
            )
            date_to = st.date_input(
                "To Date",
                value=datetime.now().date(),
                key="report_date_to",
                disabled=not apply_to,
                help="Sirf jab upar 'To date limit' tick ho.",
            )
        with dc3:
            r_col_opts = [
                "All",
                "Blank R",
                "Non-blank R",
                "FULFILLED (exact)",
                "Includes 'Planning'",
                "Includes 'ful'",
                "Includes 'MCF:'",
                "Includes 'cancel'",
                "Includes 'error' or 'fail'",
            ]
            sel_r = st.multiselect(
                "Column R (fulfilled)",
                r_col_opts,
                default=["All"],
                help="Sheet ka **R** — merged row mein sheet se aata hai. Ek se zyada = **OR** (koi bhi match).",
            )

        st.caption(
            "**Dono dates:** Limit tabhi lagti hai jab **From date limit** / **To date limit** tick ho. "
            "Checkbox **bina tick** = *shuru se* aur *koi end cap nahi*."
        )

    status_filter = normalize_report_multiselect(sel_status)
    channel_filter = normalize_report_multiselect(sel_channel)
    r_filter = normalize_report_multiselect(sel_r)

    eff_from = date_from if apply_from else None
    eff_to = date_to if apply_to else None

    p_df = st.session_state.pending_df
    r_df = st.session_state.processed_df
    p_list = p_df.to_dict("records") if p_df is not None and not p_df.empty else []
    r_list = r_df.to_dict("records") if r_df is not None and not r_df.empty else []
    sheet_reports = [sheet_row_to_report_dict(row) for row in p_list + r_list]

    db_pool = db.get_orders_filtered(
        status=None,
        channel=None,
        date_from=None,
        date_to=None,
        search=None,
        limit=8000,
    )
    merged = merge_db_and_sheet_reports(db_pool, sheet_reports)
    orders = apply_report_filters_python(
        merged,
        status_filter,
        channel_filter,
        eff_from,
        eff_to,
        search or None,
        r_column_filter=r_filter,
    )

    st.metric("Results", len(orders))
    st.caption(
        f"Merged before filters: **{len(merged)}** rows (sheet + DB deduped). "
        f"Showing up to **5000** after filters."
    )

    if orders:
        df = pd.DataFrame(orders)
        st.dataframe(df, width='stretch', hide_index=True)

        csv_buf = io.StringIO()
        df.to_csv(csv_buf, index=False)
        st.download_button(
            "📥 Download CSV",
            data=csv_buf.getvalue(),
            file_name=f"orders_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
        )

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
        if not merged:
            st.warning(
                "Sheet se orders load nahi ho paaye aur DB bhi khaali hai. "
                "Streamlit **Secrets** (Amazon/Delhivery/Shopify + endpoint) check karein; **Orders → Refresh** try karein."
            )
        else:
            st.info(
                f"Filters se koi row match nahi. (Merged **{len(merged)}** — Status/Channel/R ya dates adjust karein.)"
            )


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
                            "qty": _report_safe_int_qty(o.get("qty", 1), 1),
                            "row_number": _report_safe_row_number(o.get("row_number", 0)),
                            "source_channel": "SHOPIFY",
                            "items": [{
                                "seller_sku": o.get("seller_sku", ""),
                                "title": o.get("title", ""),
                                "quantity": _report_safe_int_qty(o.get("qty", 1), 1),
                                "price": o.get("amount", 0),
                            }],
                        }
                        
                        is_cod_flag_sync = str(o.get("is_cod", "")).lower() in ["true", "yes", "1", "cod"]
                        order_data["paymentInformationList"] = [] if is_cod_flag_sync else [{
                            "paymentMethod": "Prepaid",
                            "paymentAmount": {
                                "currencyCode": "INR",
                                "value": str(round(float(o.get("amount", 0) or 0), 2))
                            }
                        }]
                        if db.save_order(order_data):
                            added += 1

                    st.success(f"✅ Synced {added} new orders to local DB")
                    db.log_sync("MANUAL_SYNC", "SUCCESS", f"Added {added} orders")
                    _cached_endpoint_snapshot.clear()
                    st.session_state.pending_df = None
                    st.session_state.processed_df = None
                    st.session_state._endpoint_snapshot_error = None

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
        st.dataframe(df_logs[show_log], width='stretch', hide_index=True)
    else:
        st.info("No sync logs yet.")


# ─────────────────────────────────────────────
# PAGE 6: SHOPIFY TOOLS
# ─────────────────────────────────────────────
def page_shopify_tools():
    st.title("🛠️ Shopify Manual Tools")
    st.info("💡 Yeh tool Shopify orders ko manually fulfill ya update karne ke liye hai, aur Google Sheet mein bhi automatically details push karta hai.")

    def get_row_map():
        try:
            service = init_sheets_service()
            result = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range='Sheet1!A:C').execute()
            rows = result.get('values', [])
            if not rows: return {}, service
            
            headers = [str(h).strip().lower() for h in rows[0]]
            oid_idx = -1
            for col in ["ord=", "ord_serial", "order id", "order"]:
                if col in headers:
                    oid_idx = headers.index(col)
                    break
            
            if oid_idx == -1:
                oid_idx = 1 # Fallback to col B
                
            row_map = {}
            for i, row in enumerate(rows):
                if len(row) > oid_idx:
                    oid = str(row[oid_idx]).replace("#", "").strip()
                    if oid:
                        row_map[oid] = i + 1
            return row_map, service
        except Exception as e:
            return {}, None

    tab1, tab2 = st.tabs(["✅ MCF Fulfill", "🚚 Add / Update Tracking"])

    shopify_cfg = get_shopify_config(secrets)

    with tab1:
        st.subheader("MCF — Bulk Mark Fulfilled (Shopify + Sheet)")
        st.write(
            "Order IDs daalein. Shopify par fulfill hoga aur sheet mein "
            "**Q = MCF**, **R = FULFILLED** likha jayega (tracking optional)."
        )
        order_ids_input = st.text_area("Order IDs", height=150, key="tab1_oids", placeholder="2860\n2861\n5862")
        
        if st.button("MCF Fulfill on Shopify", type="primary"):
            if not order_ids_input.strip():
                st.warning("Please enter at least one Order ID.")
            else:
                import re
                order_ids = [oid.strip() for oid in re.split(r'[,\n]+', order_ids_input) if oid.strip()]
                
                with st.spinner(f"Processing {len(order_ids)} orders..."):
                    row_map, service = get_row_map()
                    
                    results = []
                    sheet_updates_qr = []
                    progress_bar = st.progress(0)
                    
                    for i, oid in enumerate(order_ids):
                        s_order = get_shopify_order(oid, shopify_cfg["headers"], shopify_cfg["shop_url"])
                        if s_order:
                            success, msg = _shopify_fulfill(oid, shopify_cfg)
                            results.append({"Order ID": oid, "Status": "✅ Success" if success else "⚠️ Failed", "Message": msg})
                            
                            if success and service and oid in row_map:
                                sheet_updates_qr.append({
                                    "row": row_map[oid],
                                    "source": "MCF",
                                    "status": "FULFILLED",
                                })
                            elif success and service and oid not in row_map:
                                results[-1]["Message"] = f"{msg} | Sheet: order not in row map"
                        else:
                            results.append({"Order ID": oid, "Status": "❌ Error", "Message": "Not found on Shopify"})
                        
                        progress_bar.progress((i + 1) / len(order_ids))
                    
                    if sheet_updates_qr and service:
                        try:
                            update_sheet_remarks(service, SHEET_ID, sheet_updates_qr)
                            st.success(
                                f"✅ Sheet updated: {len(sheet_updates_qr)} row(s) — Q=MCF, R=FULFILLED."
                            )
                        except Exception as e:
                            st.warning(f"Sheet update failed: {e}")
                    
                    st.success("✅ Done!")
                    st.dataframe(pd.DataFrame(results), width='stretch', hide_index=True)

    with tab2:
        st.subheader("Bulk Add or Update Tracking Information")
        st.write("Excel se data copy-paste karein. Format: **Order ID [Tab/Comma] Tracking ID [Tab/Comma] Carrier** (Har order nayi line mein)")
        bulk_tracking_input = st.text_area("Paste Data Here", height=200, key="tab2_bulk", placeholder="2860\t123456789\tDelhivery\n2861\t987654321\tAmazon")
        
        if st.button("Bulk Fulfill & Add Tracking", type="primary"):
            if not bulk_tracking_input.strip():
                st.warning("Please paste some data.")
            else:
                lines = bulk_tracking_input.strip().split("\n")
                parsed_data = []
                for line in lines:
                    parts = [p.strip() for p in line.split("\t") if p.strip()]
                    if len(parts) < 3:
                        parts = [p.strip() for p in line.split(",") if p.strip()]
                    
                    if len(parts) >= 3:
                        parsed_data.append({
                            "order_id": parts[0],
                            "tracking_id": parts[1],
                            "carrier": parts[2]
                        })
                
                if not parsed_data:
                    st.error("Sahi format mein data nahi mila. Make sure format is: Order ID, Tracking ID, Carrier")
                else:
                    with st.spinner(f"Processing {len(parsed_data)} tracking updates..."):
                        row_map, service = get_row_map()
                        
                        results = []
                        sheet_updates_qr = []
                        sheet_updates_st = []
                        progress_bar = st.progress(0)
                        
                        for i, item in enumerate(parsed_data):
                            oid = item["order_id"]
                            tid = item["tracking_id"]
                            car = item["carrier"]
                            
                            s_order = get_shopify_order(oid, shopify_cfg["headers"], shopify_cfg["shop_url"])
                            if s_order:
                                t_info = {"number": tid, "company": car, "url": ""}
                                success, msg = _shopify_fulfill(oid, shopify_cfg, tracking_info=t_info)
                                results.append({"Order ID": oid, "Tracking": tid, "Carrier": car, "Status": "✅ Success" if success else "⚠️ Failed", "Message": msg})
                                
                                if success and service and oid in row_map:
                                    row_num = row_map[oid]
                                    sheet_updates_qr.append({
                                        "row": row_num,
                                        "source": infer_sheet_source_q(car, tid),
                                        "status": "FULFILLED"
                                    })
                                    sheet_updates_st.append({
                                        "row": row_num,
                                        "carrier": car,
                                        "tracking_no": tid,
                                        "url": "",
                                        "remark": "Manual Update",
                                        "fill_source_q": False,
                                    })
                            else:
                                results.append({"Order ID": oid, "Tracking": tid, "Carrier": car, "Status": "❌ Error", "Message": "Not found on Shopify"})
                            
                            progress_bar.progress((i + 1) / len(parsed_data))
                        
                        if service:
                            if sheet_updates_qr:
                                try:
                                    update_sheet_remarks(service, SHEET_ID, sheet_updates_qr)
                                except Exception as e:
                                    st.warning(f"Sheet Q/R update failed: {e}")
                            if sheet_updates_st:
                                try:
                                    update_sheet_tracking(service, SHEET_ID, sheet_updates_st)
                                    st.success(f"✅ Updated {len(sheet_updates_st)} rows in Google Sheet (Q/R/S/T columns).")
                                except Exception as e:
                                    st.warning(f"Sheet S/T update failed: {e}")
                        
                        st.success("✅ Done!")
                        st.dataframe(pd.DataFrame(results), width='stretch', hide_index=True)

# ─────────────────────────────────────────────
# PAGE 7: LABELS
# ─────────────────────────────────────────────
def page_labels():
    st.title("🏷️ Generate Labels & Update Sheet")
    st.markdown("Paste data in format: **Order ID [Tab/Space] Tracking ID (AWB) [Tab/Space] SKU [Tab/Space] Title (Optional)**")
    
    input_data = st.text_area("Paste Data Here:", height=200, placeholder="5060\t52799210020160\tWC_Back_Rest_Black")
    
    if st.button("Update Sheet & Generate Labels", type="primary"):
        if not input_data.strip():
            st.warning("Please paste some data first.")
            return
            
        with st.spinner("Processing labels and updating sheet..."):
            import subprocess
            import re
            import os
            import sys
            
            script_path = os.path.join(os.path.dirname(__file__), "update_sheet_awb.py")
            
            try:
                # Add extra newline to simulate pressing Enter on empty line
                input_str = input_data.strip() + "\n\n"
                
                # Run the script and feed the input text
                process = subprocess.run(
                    [sys.executable, script_path],
                    input=input_str,
                    text=True,
                    capture_output=True,
                    cwd=os.path.dirname(__file__)
                )
                
                output = process.stdout
                err_output = process.stderr
                
                # Show the textual output in an expander
                with st.expander("Show Script Execution Logs"):
                    st.text(output)
                    if err_output:
                        st.text("Errors:")
                        st.text(err_output)
                        
                # Extract PDF path from the output
                pdf_match = re.search(r"PDF saved:\s*(.*?\.pdf)", output)
                if pdf_match:
                    pdf_path = pdf_match.group(1).strip()
                    import os
                    if os.path.exists(pdf_path):
                        st.success(f"✅ Labels Generated Successfully!")
                        
                        with open(pdf_path, "rb") as f:
                            pdf_bytes = f.read()
                            
                        st.download_button(
                            label="📥 Download Labels (PDF)",
                            data=pdf_bytes,
                            file_name=os.path.basename(pdf_path),
                            mime="application/pdf",
                            type="primary"
                        )
                    else:
                        st.error(f"Generated PDF file not found at: {pdf_path}")
                else:
                    if "✅ Google Sheet successfully updated!" in output:
                        st.warning("Google Sheet updated, but failed to find the generated PDF path in output.")
                    else:
                        st.error("Failed to process data. Check the logs above.")
                        
            except Exception as e:
                st.error(f"Error running the script: {e}")

# ─────────────────────────────────────────────
# ROUTER
# ─────────────────────────────────────────────
ensure_sheet_orders_loaded()
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
elif page == "Shopify Tools":
    page_shopify_tools()
elif page == "Labels":
    page_labels()
else:
    page_dashboard()
