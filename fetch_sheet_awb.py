import time
from datetime import datetime
import pandas as pd
import db
from utils import (
    SHEET_ID,
    fetch_orders_from_apps_script,
    get_access_token,
    get_delhivery_tracking,
    get_shopify_config,
    fulfill_order,
    get_shopify_order,
    get_order_awb,
    init_sheets_service,
    read_secret,
    update_sheet_remarks,
    update_sheet_tracking,
    build_tracking_url,
)
from w import fetch_mcf_data
def row_indicates_fulfilled_for_mcf_lookup(fulfilled_str: str, status_str: str = "") -> bool:
    s1 = (fulfilled_str or "").lower()
    s2 = (status_str or "").lower()
    return ("ful" in s1 or "plan" in s1 or "process" in s1 or "mcf" in s1) or \
           ("ful" in s2 or "plan" in s2 or "process" in s2 or "mcf" in s2)
def shopify_fulfill(order_id, shopify_cfg, tracking_info=None):
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
        return True, "Already fulfilled (no change)"
    except Exception as e:
        return False, str(e)[:80]
def load_pending_orders():
    mcf_orders = []
    for o in fetch_orders_from_apps_script():
        source = str(o.get("source", "")).strip().upper()
        if "MCF" in source or "DELHI" in source:
            mcf_orders.append({
                "row_number": int(o.get("row_number", 0) or 0),
                "order_id": str(o.get("ord_serial", "")).replace("#", "").strip(),
                "customer": o.get("customer", ""),
                "tracking_no": get_order_awb(o),
                "source": source,
                "fulfilled": str(o.get("fulfilled", "")).strip(),
            })
    return [o for o in mcf_orders if not o["tracking_no"]]
def fetch_all():
    secrets = read_secret()
    token, err = get_access_token(secrets)
    if not token:
        print(f"[ERROR] Amazon auth failed: {err}")
        return
    shopify_cfg = get_shopify_config(secrets)
    try:
        sheets_svc = init_sheets_service(secrets)
    except Exception as e:
        sheets_svc = None
        print(f"[WARN] Sheet update disabled: {e}")
    del_keys = [secrets.get("DELHIVERY_API_KEY", ""), secrets.get("DELHIVERY_API_KEY2", "")]
    del_keys = [k for k in del_keys if k]
    need_trk = load_pending_orders()
    total = len(need_trk)
    print(f"[INFO] {total} orders pending tracking in sheet")
    sheet_updates = []
    no_trk_remark_updates = []
    fulfilled_qr_updates = []
    found_count = 0
    skipped_count = 0
    result_rows = []
    for i, order in enumerate(need_trk):
        order_id = order["order_id"]
        orig_source = str(order.get("source", "")).upper()
        print(f"[{i+1}/{total}] Checking {order_id}...", end=" ")
        if not row_indicates_fulfilled_for_mcf_lookup(order.get("fulfilled", ""), order.get("status", "")):
            print("skipped (column R not ready)")
            skipped_count += 1
            result_rows.append({
                "Order ID": order_id, "Customer": order["customer"], "Status": "Skipped",
                "Tracking ID": "", "Carrier": "", "Shopify": "", "Sheet": "unchanged",
            })
            continue
        tn, cc, mcf_status = "", "", ""
        is_delhivery_first = "DELHI" in orig_source
        if not is_delhivery_first:
            tn, cc, mcf_status, _ = fetch_mcf_data(order_id, token)
        if tn:
            remark = f"Tracking Added {datetime.now().strftime('%d/%m %H:%M')}"
            db.update_order_tracking(order_id, cc or "", tn, "")
            s_ok, s_msg = shopify_fulfill(
                order_id, shopify_cfg, tracking_info={"number": tn, "company": cc or "Amazon", "url": ""}
            )
            sheet_updates.append({
                "row": order["row_number"], "carrier": cc or "Amazon", "tracking_no": tn,
                "url": build_tracking_url(cc or "Amazon", tn), "status": "Intransit",
            })
            fulfilled_qr_updates.append({"row": order["row_number"], "source": "MCF", "status": "FULFILLED"})
            found_count += 1
            print(f"FOUND {tn} ({cc or 'Amazon'}) | Shopify: {'OK' if s_ok else s_msg}")
            result_rows.append({
                "Order ID": order_id, "Customer": order["customer"], "Status": mcf_status,
                "Tracking ID": tn, "Carrier": cc or "Amazon",
                "Shopify": "OK" if s_ok else s_msg, "Sheet": "pending update" if not sheets_svc else "updated",
            })
        else:
            if is_delhivery_first or mcf_status == "NotFound":
                del_found, del_awb, del_status, _ = get_delhivery_tracking(del_keys, order_id)
                if del_found and del_awb:
                    remark = f"Delhivery AWB {datetime.now().strftime('%d/%m %H:%M')}"
                    if del_status:
                        remark = f"{remark} | {del_status}"
                    db.update_order_tracking(order_id, "Delhivery", del_awb, "")
                    s_ok, s_msg = shopify_fulfill(
                        order_id, shopify_cfg, tracking_info={"number": del_awb, "company": "Delhivery", "url": ""}
                    )
                    sheet_updates.append({
                        "row": order["row_number"], "carrier": "Delhivery", "tracking_no": del_awb,
                        "url": build_tracking_url("Delhivery", del_awb), "status": "Intransit",
                    })
                    fulfilled_qr_updates.append({"row": order["row_number"], "source": "Delhivery", "status": "FULFILLED"})
                    found_count += 1
                    print(f"FOUND Delhivery {del_awb} | Shopify: {'OK' if s_ok else s_msg}")
                    result_rows.append({
                        "Order ID": order_id, "Customer": order["customer"], "Status": "Found on Delhivery",
                        "Tracking ID": del_awb, "Carrier": "Delhivery",
                        "Shopify": "OK" if s_ok else s_msg, "Sheet": "pending update" if not sheets_svc else "updated",
                    })
                    time.sleep(0.4)
                    continue
            if is_delhivery_first:
                status_label = "Delhivery: Not Found"
            else:
                status_label = {
                    "Planning": "MCF: Planning", "Received": "MCF: Received", "Processing": "MCF: Processing",
                    "Complete": "MCF: Complete", "Cancelled": "MCF: Cancelled", "NotFound": "MCF: Not Found",
                }.get(mcf_status, f"MCF: {mcf_status}")
            sheet_updates.append({
                "row": order["row_number"], "carrier": "", "tracking_no": "", "url": "", "remark": status_label,
            })
            no_trk_remark_updates.append({
                "row": order["row_number"], "source": "Delhivery" if is_delhivery_first else "MCF", "status": "FULFILLED",
            })
            print(f"not found ({status_label})")
            result_rows.append({
                "Order ID": order_id, "Customer": order["customer"], "Status": status_label,
                "Tracking ID": "", "Carrier": "", "Shopify": "", "Sheet": status_label,
            })
        time.sleep(0.4)
    if result_rows:
        out_file = "Sheet_AWB_Fetch_Results.xlsx"
        pd.DataFrame(result_rows).to_excel(out_file, index=False)
        print(f"\n[INFO] Saved results to {out_file}")
    if not sheets_svc:
        print(f"\n[DONE] Found tracking: {found_count} | Still pending: {total - found_count - skipped_count} | Skipped: {skipped_count}")
        print("[NOTE] Add hide.json or GOOGLE_CREDS_FILE=path in secret.txt for sheet updates.")
        return
    print("\n[INFO] Updating Google Sheet...")
    if sheet_updates:
        for su in sheet_updates:
            su["fill_source_q"] = False
        update_sheet_tracking(sheets_svc, SHEET_ID, sheet_updates)
        print(f"  -> Updated tracking columns for {len(sheet_updates)} rows")
    all_qr = fulfilled_qr_updates + no_trk_remark_updates
    if all_qr:
        update_sheet_remarks(sheets_svc, SHEET_ID, all_qr)
        print(f"  -> Updated remarks for {len(all_qr)} rows")
    print(f"\n[DONE] Found tracking: {found_count} | Still pending: {total - found_count - skipped_count} | Skipped: {skipped_count}")
if __name__ == "__main__":
    fetch_all()
