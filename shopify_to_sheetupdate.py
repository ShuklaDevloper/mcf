import os
import time

import pandas as pd
import requests

from utils import (
    SHEET_ID,
    fetch_orders_from_apps_script,
    build_order_row_map,
    get_access_token,
    get_delhivery_tracking,
    get_shopify_config,
    init_sheets_service,
    read_secret,
    update_sheet_fulfilled_only,
    update_sheet_remarks,
    update_sheet_tracking,
    infer_sheet_source_q,
    build_tracking_url,
)

SKIP_SHEET_STATUSES = {"unfulfilled", "unfulfillable"}


def fetch_shopify_order_details(order_id, shopify_cfg):
    """Fetch order + fulfillment details from Shopify. Returns dict or None."""
    oid = str(order_id).replace("#", "").strip()
    shop_url = shopify_cfg["shop_url"]
    headers = shopify_cfg["headers"]
    search_url = f"{shop_url}/admin/api/2024-01/orders.json"

    s_order = None
    for search_name in [f"#{oid}", oid]:
        resp = requests.get(search_url, headers=headers, params={"name": search_name, "status": "any"}, timeout=30)
        orders = resp.json().get("orders", [])
        if orders:
            s_order = orders[0]
            break

    if not s_order:
        return None

    tracking_no = ""
    carrier = ""
    tracking_url = ""
    f_url = f"{shop_url}/admin/api/2024-01/orders/{s_order['id']}/fulfillments.json"
    try:
        fr = requests.get(f_url, headers=headers, timeout=30)
        for f in fr.json().get("fulfillments", []):
            if f.get("status") in ("success", "pending", "open"):
                tracking_no = (f.get("tracking_number") or "").strip()
                carrier = (f.get("tracking_company") or "Shopify").strip()
                tracking_url = (f.get("tracking_url") or "").strip()
                if tracking_no:
                    break
    except Exception:
        pass

    cancelled = bool(s_order.get("cancelled_at"))
    fulfillment_status = s_order.get("fulfillment_status") or "unfulfilled"
    line_items = s_order.get("line_items") or []
    fulfillable_qty = sum(int(li.get("fulfillable_quantity") or 0) for li in line_items)

    if cancelled:
        status = "Cancelled"
    elif tracking_no:
        status = "Fulfilled"
    elif fulfillment_status == "fulfilled":
        status = "Fulfilled"
    elif fulfillable_qty == 0 and line_items:
        status = "Unfulfillable"
    else:
        status = "Unfulfilled"

    return {
        "order_id": oid,
        "shopify_name": s_order.get("name", ""),
        "customer": (s_order.get("customer") or {}).get("first_name", "") + " " + (s_order.get("customer") or {}).get("last_name", ""),
        "email": s_order.get("email", ""),
        "phone": s_order.get("phone", "") or (s_order.get("shipping_address") or {}).get("phone", ""),
        "total": s_order.get("total_price", ""),
        "tracking_no": tracking_no,
        "carrier": carrier,
        "tracking_url": tracking_url,
        "status": status,
        "fulfillment_status": fulfillment_status,
        "cancelled": cancelled,
    }


def _require_sheet_credentials(secrets):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_file = (
        secrets.get("GOOGLE_CREDS_FILE")
        or secrets.get("HIDE_JSON")
        or secrets.get("GCP_CREDS_FILE")
        or "hide.json"
    )
    if not os.path.isabs(creds_file):
        creds_file = os.path.join(script_dir, creds_file)
    if os.path.exists(creds_file):
        return creds_file
    if (secrets.get("GCP_SERVICE_ACCOUNT_JSON") or "").strip().startswith("{"):
        return None
    return None


def run_temp_update(order_ids):
    secrets = read_secret()
    shopify_cfg = get_shopify_config(secrets)
    del_keys = [secrets.get("DELHIVERY_API_KEY", ""), secrets.get("DELHIVERY_API_KEY2", "")]
    del_keys = [k for k in del_keys if k]

    creds_path = _require_sheet_credentials(secrets)
    if not creds_path and not (secrets.get("GCP_SERVICE_ACCOUNT_JSON") or "").strip().startswith("{"):
        print("=" * 60)
        print("[STOP] Google Sheet update IMPOSSIBLE — hide.json missing!")
        print("")
        print("  Shopify se data aa raha hai, lekin sheet mein likhne ke liye")
        print("  hide.json (Google service account) file chahiye.")
        print("")
        print("  Steps:")
        print("  1. Google Cloud se service account JSON download karo")
        print("  2. File ko rename karke rakho: d:\\mcf-main\\mcf-main\\hide.json")
        print("  3. Sheet par us service account email ko Editor access do")
        print("=" * 60)
        return

    service = None
    try:
        print("Initializing Sheets Service (for updates)...")
        service = init_sheets_service(secrets)
        print("[OK] Google Sheets write access ready.")
    except Exception as e:
        print(f"[ERROR] Sheet auth failed: {e}")
        return

    print("Loading order -> row mapping from Apps Script...")
    try:
        row_map = build_order_row_map(fetch_orders_from_apps_script())
        print(f"[OK] {len(row_map)} orders mapped to sheet rows.")
    except Exception as e:
        print(f"Error loading sheet mapping: {e}")
        return

    qr_updates = []
    st_updates = []
    fulfilled_only_updates = []
    results = []

    print(f"\nProcessing {len(order_ids)} order(s)...\n")
    for oid in order_ids:
        oid_clean = str(oid).replace("#", "").strip()
        print(f"--- Order {oid_clean} ---")

        info = fetch_shopify_order_details(oid_clean, shopify_cfg)
        if not info:
            print("  [!] Not found on Shopify")
            results.append({"Order ID": oid_clean, "Status": "Not Found", "Tracking": "", "Carrier": "", "Sheet Row": ""})
            continue

        tracking_no = info["tracking_no"]
        carrier = info["carrier"]
        final_status = info["status"]

        # MCF / Delhivery fallback only for active orders
        if final_status.lower() not in SKIP_SHEET_STATUSES and not info["cancelled"]:
            if not tracking_no:
                from w import fetch_mcf_data
                token, _ = get_access_token(secrets)
                if token:
                    tn, cc, mcf_status, _ = fetch_mcf_data(oid_clean, token)
                    if tn:
                        tracking_no, carrier, final_status = tn, cc or "MCF", "Fulfilled"
                        print(f"  [MCF] Found tracking: {tracking_no} ({carrier})")
                    elif mcf_status and mcf_status not in ("NotFound", "Planning", "Received", "Processing"):
                        final_status = mcf_status

            if not tracking_no and del_keys:
                del_found, del_awb, del_status, _ = get_delhivery_tracking(del_keys, oid_clean)
                if del_found and del_awb:
                    tracking_no, carrier, final_status = del_awb, "Delhivery", "Fulfilled"
                    print(f"  [Delhivery] Found AWB: {del_awb}")

        row_num = row_map.get(oid_clean)
        print(f"  Customer : {info['customer'].strip() or '-'}")
        print(f"  Phone    : {info['phone'] or '-'}")
        print(f"  Total    : {info['total']}")
        print(f"  Status   : {final_status}")
        print(f"  Tracking : {tracking_no or '-'}")
        print(f"  Carrier  : {carrier or '-'}")
        if info["tracking_url"]:
            print(f"  URL      : {info['tracking_url']}")
        print(f"  Sheet Row: {row_num or 'NOT IN SHEET'}")

        results.append({
            "Order ID": oid_clean,
            "Customer": info["customer"].strip(),
            "Status": final_status,
            "Tracking": tracking_no,
            "Carrier": carrier,
            "Sheet Row": row_num or "",
        })

        if not row_num:
            continue

        status_key = final_status.lower()
        if status_key in SKIP_SHEET_STATUSES:
            print("  [SKIP] Unfulfilled/Unfulfillable — sheet unchanged")
            continue

        if status_key == "cancelled":
            fulfilled_only_updates.append({
                "row": row_num,
                "fulfilled": "Cancelled",
                "status": "Cancelled",
            })
            print("  [SHEET] Will mark R=Cancelled, V=Cancelled")
        elif tracking_no:
            source_q = infer_sheet_source_q(carrier, tracking_no) or carrier or "Shopify"
            qr_updates.append({
                "row": row_num,
                "source": source_q,
                "status": "FULFILLED",
            })
            st_updates.append({
                "row": row_num,
                "carrier": carrier,
                "tracking_no": tracking_no,
                "url": info.get("tracking_url", "") or build_tracking_url(carrier, tracking_no),
                "status": "Intransit",
                "fill_source_q": False,
            })
            print("  [SHEET] Will update Q–V (Source, FULFILLED, Carrier, Tracking, URL, Status)")
        elif final_status.lower() == "fulfilled":
            qr_updates.append({"row": row_num, "source": carrier or "Shopify", "status": "FULFILLED"})
            print("  [SHEET] Will mark Q/R FULFILLED (no tracking)")

        time.sleep(0.3)

    if results:
        out = "Order_Shopify_Sync_Results.xlsx"
        pd.DataFrame(results).to_excel(out, index=False)
        print(f"\n[INFO] Saved results to {out}")

    if not service:
        pending = len(qr_updates) + len(st_updates) + len(fulfilled_only_updates)
        if pending:
            print(f"\n[ERROR] {pending} sheet update(s) could NOT run — hide.json missing.")
        return

    if fulfilled_only_updates:
        print("\nUpdating sheet (Cancelled)...")
        update_sheet_fulfilled_only(service, SHEET_ID, fulfilled_only_updates)
    if qr_updates:
        print("Updating sheet Q/R (Source + FULFILLED)...")
        update_sheet_remarks(service, SHEET_ID, qr_updates)
    if st_updates:
        print("Updating sheet S/V (Carrier + Tracking + URL + Status)...")
        update_sheet_tracking(service, SHEET_ID, st_updates)

    total = len(fulfilled_only_updates) + len(qr_updates)
    print(f"\n[DONE] Google Sheet updated for {total} order(s).")


if __name__ == "__main__":
    print("Paste Order IDs (space or newline separated). Type DONE when finished:\n")
    target_ids = []
    while True:
        try:
            line = input()
            if line.strip().upper() == "DONE":
                break
            target_ids.extend([oid.strip() for oid in line.split() if oid.strip()])
        except EOFError:
            break

    if target_ids:
        run_temp_update(list(dict.fromkeys(target_ids)))
    else:
        print("No Order IDs provided.")
