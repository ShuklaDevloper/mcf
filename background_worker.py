"""
background_worker.py — Daemon Scheduler
Run this separately: python background_worker.py

Jobs:
  Every 10 min  → sync pending orders from Apps Script endpoint to local DB
  Every 30 min  → poll Amazon MCF for tracking on PROCESSING orders
"""
import time
import schedule
from datetime import datetime, timedelta

import db
import requests
from utils import (
    read_secret,
    get_access_token,
    get_shopify_config,
    get_shopify_order,
    fulfill_order,
    clean_phone_number,
    validate_address,
    validate_pincode,
    init_sheets_service,
    update_sheet_tracking,
    APPS_SCRIPT_URL,
    SHEET_ID,
)
from w import fetch_mcf_data

secrets = read_secret()
shopify_cfg = get_shopify_config(secrets)

_token_cache = {"token": None, "time": None}


def _get_token():
    now = datetime.now()
    if _token_cache["token"] and _token_cache["time"] and now - _token_cache["time"] < timedelta(minutes=50):
        return _token_cache["token"], None
    token, err = get_access_token(secrets)
    if token:
        _token_cache["token"] = token
        _token_cache["time"] = now
    return token, err


# ─────────────────────────────────────────────
# JOB 1: Fetch pending orders from Apps Script → DB
# ─────────────────────────────────────────────
def auto_fetch_source_orders():
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] Syncing orders from Apps Script...")
    try:
        resp = requests.get(APPS_SCRIPT_URL, timeout=30)
        data = resp.json()

        if not data.get("success") or not data.get("orders"):
            db.log_sync("FETCH_SOURCE", "EMPTY", "No orders or success=false")
            return

        added = 0
        for o in data["orders"]:
            status_raw = str(o.get("status", "")).lower()
            fulfilled = str(o.get("fulfilled", "")).strip()
            # Only sync new pending orders
            if fulfilled or status_raw not in ["pending", ""]:
                continue

            order_id = str(o.get("ord_serial", "")).replace("#", "").strip()
            if not order_id:
                continue

            phone = clean_phone_number(o.get("phone", ""))
            full_addr = f"{o.get('address1', '')} {o.get('address2', '')}".strip()
            addr1, addr2, addr3, _ = validate_address(full_addr)

            is_cod_flag = str(o.get("is_cod", "")).lower() in ["true", "yes", "1", "cod"]
            if is_cod_flag:
                payment_info = []
            else:
                payment_info = [{
                    "PaymentMethod": "Prepaid",
                    "PaymentAmount": float(o.get("amount", 0)),
                    "CurrencyCode": "INR"
                }]

            order_data = {
                "order_id": order_id,
                "date": o.get("date", ""),
                "customer": o.get("customer", ""),
                "phone": phone,
                "amount": o.get("amount", 0),
                "addr_line1": addr1,
                "addr_line2": addr2,
                "addr_line3": addr3,
                "pincode": str(o.get("pincode", "")),
                "state_code": o.get("state_code", ""),
                "city": o.get("city", ""),
                "is_cod": str(o.get("is_cod", "")),
                "seller_sku": o.get("seller_sku", ""),
                "title": o.get("title", "")[:200],
                "qty": int(o.get("qty", 1) or 1),
                "row_number": int(o.get("row_number", 0) or 0),
                "source_channel": "SHOPIFY",
                "payment_info": payment_info,
                "items": [{
                    "seller_sku": o.get("seller_sku", ""),
                    "title": o.get("title", ""),
                    "quantity": int(o.get("qty", 1) or 1),
                    "price": o.get("amount", 0),
                }],
            }

            saved = db.save_order(order_data)
            if saved:
                added += 1

        msg = f"Added {added} new orders"
        print(f"  → {msg}")
        db.log_sync("FETCH_SOURCE", "SUCCESS", msg)

    except Exception as e:
        print(f"  [ERROR] auto_fetch: {e}")
        db.log_sync("FETCH_SOURCE", "ERROR", str(e)[:300])


# ─────────────────────────────────────────────
# JOB 2: Poll MCF tracking for PROCESSING orders
# ─────────────────────────────────────────────
def poll_mcf_tracking():
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] Polling MCF tracking...")

    processing = db.get_orders(filters={"status": "PROCESSING", "fulfillment_channel": "MCF"})
    if not processing:
        print("  → No MCF orders pending tracking")
        return

    print(f"  → {len(processing)} orders to check")
    token, err = _get_token()
    if not token:
        print(f"  [ERROR] Token failed: {err}")
        db.log_sync("POLL_TRACKING", "ERROR", f"Token error: {err}")
        return

    sheets_service = None
    sheet_updates = []

    for order in processing:
        order_id = order["order_id"]
        try:
            tn, cc, mcf_status, raw = fetch_mcf_data(order_id, token)
            if tn:
                print(f"  ✓ {order_id} → {tn} ({cc})")

                # 1. Update local DB
                db.update_order_tracking(order_id, cc or "", tn, "")

                # 2. Update Shopify
                if shopify_cfg["shop_url"]:
                    try:
                        s_order = get_shopify_order(order_id, shopify_cfg["headers"], shopify_cfg["shop_url"])
                        if s_order:
                            ok = fulfill_order(
                                s_order, shopify_cfg["headers"], shopify_cfg["shop_url"],
                                tracking_info={"number": tn, "company": cc or "", "url": ""}
                            )
                            if ok:
                                db.mark_shopify_fulfilled(order_id)
                    except Exception as se:
                        print(f"    [Shopify] {order_id}: {se}")

                # 3. Queue sheet update (S=carrier, T=tracking, U=url)
                if order.get("row_number"):
                    sheet_updates.append({
                        "row": order["row_number"],
                        "carrier": cc or "",
                        "tracking_no": tn,
                        "url": "",
                    })
            else:
                print(f"  … {order_id} → no tracking yet")

        except Exception as e:
            print(f"  [ERROR] {order_id}: {e}")
            db.log_sync("POLL_TRACKING", "ERROR", f"{order_id}: {e}")

        time.sleep(2)  # SP-API rate limit respect

    # Batch sheet update
    if sheet_updates:
        try:
            if sheets_service is None:
                sheets_service = init_sheets_service()
            update_sheet_tracking(sheets_service, SHEET_ID, sheet_updates)
            print(f"  → Updated {len(sheet_updates)} rows in Google Sheet (S/T/U)")
        except Exception as e:
            print(f"  [Sheet ERROR] {e}")
            db.log_sync("SHEET_UPDATE", "ERROR", str(e)[:300])

    db.log_sync("POLL_TRACKING", "SUCCESS", f"Checked {len(processing)}, found tracking for {len(sheet_updates)}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def run_worker():
    print("=" * 50)
    print("Background Worker Started")
    print("  • Every 10 min: Sync orders from source")
    print("  • Every 30 min: Poll MCF tracking")
    print("  Press Ctrl+C to stop")
    print("=" * 50)

    # Run once at startup
    auto_fetch_source_orders()
    poll_mcf_tracking()

    schedule.every(10).minutes.do(auto_fetch_source_orders)
    schedule.every(30).minutes.do(poll_mcf_tracking)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    run_worker()
