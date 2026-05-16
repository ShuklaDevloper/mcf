import os
import requests
import json
import time
from datetime import datetime

from utils import init_sheets_service, read_secret, SHEET_ID

# ---------------- API SETUP ----------------
SWISHIP_URL = "https://www.swiship.co.uk/api/getPackageTrackingDetails"
DELHIVERY_URL = "https://track.delhivery.com/api/v1/packages/json/"

COOKIES = {
    "__Host-mons-sid": "261-1328087-9578911",
    "__Host-mons-ubid": "258-4175316-0560720",
    "__Host-mons-st": "isOb/IaXNR1zfUSTCdOxUHL0yY4n0V43mg2quN9+mNXQlQql/pQckmWAT9edzf5J89Jgso9v2ph34Vp4AwhBdc6IPfCPzWrmhFiq9Dbcv2w2pi/eckKEz9rVZaXHMk2tBrqULnTjixsFtj3e91tF6i9w4XQ5mrEtpOCVmVV/uKm6Z+oJO5eeBc+V66QK0gS5Qtm9p1N9EJtp+Guag9NYGbDFhYLG+Y3xxdgjlpawQ4g4R891vF4k4k/ycBY8GfVESWmW1UE4KbxcG9SGQarfwZk3almziwNxyvaQdkOXRb264VOjNXmcStqO++cBgUPfwCMIqX8zHyl/gRiFBjr+97nVZuYJT1HQWkKGWmhmCPg="
}

SWISHIP_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://www.swiship.co.uk",
    "Referer": "https://www.swiship.co.uk",
    "User-Agent": "Mozilla/5.0"
}

def safe_date(val):
    if not val:
        return ""
    return str(val)[:10]

def format_dt(value):
    if not value:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime("%d/%m %H:%M")
    except ValueError:
        return text

def normalize_status(raw_state, raw_event=""):
    text = f"{raw_state} {raw_event}".strip().lower()
    if "lost" in text or "damage" in text or "missing" in text:
        return "Lost"
    if "rto" in text or "return" in text:
        return "RTO"
    if "deliver" in text:
        return "Delivered"
    return "Intransit"


def looks_like_tracking_number(value) -> bool:
    """True if column T likely holds a carrier AWB / tracking id (not a status placeholder)."""
    if value is None:
        return False
    s = str(value).strip()
    if not s or len(s) < 8:
        return False
    low = s.lower()
    if "mcf:" in low:
        return False
    if "processing" in low:
        return False
    for bad in ("pending", "not assigned", "n/a", "na", "tbd", "—", "-", "none", "placeholder"):
        if low == bad:
            return False
    if not any(c.isdigit() for c in s):
        return False
    return True


def col_num_to_a1(col_num):
    # col_num is 1-based index to A, B, C...
    result = ""
    while col_num > 0:
        col_num, rem = divmod(col_num - 1, 26)
        result = chr(65 + rem) + result
    return result

# ─── ITHINK LOGISTICS API ─────────────────────────────────────────────
def get_ithink_shipment(awb, ithink_token, ithink_secret):
    if not ithink_token or not ithink_secret:
        return None
    url = "https://api.ithinklogistics.com/api_v3/order/track.json"
    headers = {"Content-Type": "application/json"}
    payload = {
        "data": {
            "awb_number_list": str(awb).strip(),
            "access_token": ithink_token,
            "secret_key": ithink_secret
        }
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=15)
        if r.status_code == 200:
            j = r.json()
            if str(j.get("status", "")).strip().lower() == "success":
                data = j.get("data", {})
                if str(awb).strip() in data:
                    return data[str(awb).strip()]
    except Exception as e:
        pass
    return None

def parse_ithink_datetime(dt_str):
    if not dt_str:
        return ""
    try:
        return datetime.strptime(str(dt_str)[:11].strip(), "%d %b %Y").strftime("%Y-%m-%d")
    except Exception:
        return str(dt_str)[:10]

def parse_ithink(order):
    if not order:
        return None

    status = str(order.get("current_status", ""))
    last_scan = order.get("last_scan_details", {}) or {}
    location = str(last_scan.get("scan_location", ""))
    remark = str(last_scan.get("remark", ""))
    
    combined = (status + " " + remark).upper()

    eta = order.get('expected_delivery_date', '') or order.get('promise_delivery_date', '')
    if eta:
        eta = parse_ithink_datetime(eta)

    delivery_date = ""
    if "DELIVERED" in status.upper() or "DELIVERED" in combined:
        delivery_date = str(last_scan.get("status_date_time", ""))

    pickup_date = ""
    scan_details = order.get("scan_details", [])
    for event in scan_details:
        ev_status = str(event.get('status', '')).lower()
        ev_remark = str(event.get('status_remark', '')).lower()
        event_date = event.get('status_date_time', '')
        if any(keyword in ev_status + " " + ev_remark for keyword in ['picked', 'pickup', 'dispatched', 'booked', 'manifested']):
            pickup_date = str(event_date)
            break

    if not pickup_date:
        pickup_date = str(order.get('order_date_time', ''))

    if "DELIVERED" in combined and "RTO" not in combined and "RETURN" not in combined:
        final_v = "Delivered"
    elif "RTO" in combined or "RETURN" in combined:
        final_v = "RTO"
    elif "UNDELIVERED" in combined or "FAILED" in combined or "NDR" in combined:
        final_v = "Undelivered"
    else:
        final_v = "Intransit"

    parts = [p for p in [status, remark, location] if p]
    last_update = " | ".join(parts)

    return {
        "status": final_v,
        "eta": format_dt(eta) if eta else "",
        "pickup": format_dt(pickup_date) if pickup_date else "",
        "delivery": format_dt(delivery_date) if delivery_date else "",
        "last_update": last_update,
        "rto": final_v if final_v == "RTO" else "",
    }

def run_live_tracking_update(progress_callback=None):
    """
    Downloads Sheet via API, checks tracking for all MCF/Delhivery rows,
    updates sheet locally and flushes to remote in chunks.
    Returns: list of dicts with summary results.
    """
    secrets = read_secret()
    delhivery_keys = [
        secrets.get("DELHIVERY_API_KEY", ""),
        secrets.get("DELHIVERY_API_KEY2", "")
    ]
    delhivery_keys = [k for k in delhivery_keys if k]
    ithink_token = secrets.get("Ithink_access_token", "")
    ithink_secret = secrets.get("Ithink_secret_key", "")

    try:
        service = init_sheets_service()
    except Exception as e:
        return [{"order_id": "Error", "status": f"Sheets service init failed: {str(e)}", "carrier": "", "desc": ""}]

    # Fetch rows
    range_name = 'Sheet1!A:AF'
    try:
        result = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=range_name).execute()
        rows = result.get('values', [])
    except Exception as e:
         return [{"order_id": "Error", "status": f"Sheets service fetch failed: {str(e)}", "carrier": "", "desc": ""}]

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
    order_id_idx = get_idx("ord_serial", "order id")
    tracking_no_idx = get_idx("tracking no", "tracking no.", "tracking number")
    tracking_url_idx = get_idx("tracking url")
    status_idx = get_idx("status")
    carrier_idx = get_idx("carrier")
    eta_idx = get_idx("eta")
    pickup_idx = get_idx("pickup date")
    delivery_idx = get_idx("delivery date", "deliverydate", "delevrey date")
    last_status_idx = get_idx("last status", "last update", "last_update")
    rto_idx = get_idx("rto")

    if source_idx == -1 or tracking_no_idx == -1:
        return [{"order_id": "Error", "status": f"Could not find required columns (Source/Tracking). Headers: {headers}", "carrier": "", "desc": ""}]

    pending_updates = []
    summary_results = []
    
    col_tracking_url = (tracking_url_idx + 1) if tracking_url_idx != -1 else 20
    col_status = (status_idx + 1) if status_idx != -1 else 21
    
    orders_to_check = []
    for i in range(1, len(rows)):
        row = rows[i]
        
        def safe_get(idx):
            if idx != -1 and idx < len(row):
                return str(row[idx]).strip()
            return ""

        source = safe_get(source_idx)
        tracking_no = safe_get(tracking_no_idx)
        carrier = safe_get(carrier_idx)
        existing_rto = safe_get(rto_idx)
        existing_status = safe_get(status_idx)
        order_id = safe_get(order_id_idx).replace("#", "")

        is_mcf = source.upper() == "MCF"
        is_delhivery = "delhivery" in carrier.lower()
        is_ithink = "ithink" in carrier.lower()

        if tracking_no and looks_like_tracking_number(tracking_no):
            # Skip if already delivered or RTO is delivered
            if existing_status.lower() == "delivered":
                continue
            if existing_status.lower() == "rto" and existing_rto.lower() == "delivered":
                continue
            if existing_rto.lower() == "delivered":
                continue
            
            orders_to_check.append({
                "row_index": i,
                "order_id": order_id,
                "tracking_no": tracking_no,
                "carrier": carrier,
                "existing_rto": existing_rto,
                "existing_status": existing_status
            })

    total = len(orders_to_check)
    print(f"\n[INFO] Found {total} orders to check tracking for...")
    if total == 0:
        return []

    def flush_updates(updates):
        if not updates: return
        try:
            # Re-init service to avoid ConnectionAbortedError
            srv = init_sheets_service()
            srv.spreadsheets().values().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"valueInputOption": "RAW", "data": updates}
            ).execute()
        except Exception as e:
            print(f"Batch update error: {e}")

    for idx, item in enumerate(orders_to_check):
        order_id = item["order_id"]
        tracking_no = item["tracking_no"]
        carrier = item["carrier"]
        existing_rto = item["existing_rto"]
        existing_status = item["existing_status"]
        row_num = item["row_index"] + 1

        if progress_callback:
            progress_callback(idx, total, tracking_no)

        eta_value, pickup_value, delivery_value = "", "", ""
        last_update_value, rto_value, tracking_url = "", "", ""
        status = existing_status or "Intransit"
        info = None
        low_carrier = carrier.lower()

        try:
            if delhivery_keys and not info:
                for del_key in delhivery_keys:
                    try:
                        resp = requests.get(DELHIVERY_URL, params={"waybill": tracking_no, "token": del_key}, timeout=15)
                        if resp.status_code == 200:
                            res = resp.json()
                            shipment_data = res.get("ShipmentData", [])
                            shipment = shipment_data[0].get("Shipment", {}) if shipment_data else {}
                            
                            if shipment:
                                status_obj = shipment.get("Status", {})
                                raw_state = status_obj.get("Status", "")
                                raw_event = status_obj.get("Instructions", "")
                                raw_date = status_obj.get("StatusDateTime", "") or status_obj.get("StatusDate", "")
                                location = status_obj.get("StatusLocation", "")
                                
                                combined = f"{raw_state} {raw_event}".upper()
                                if "DELIVERED" in combined and "RTO" not in combined and "RETURN" not in combined:
                                    status = "Delivered"
                                elif "RTO" in combined or "RETURN" in combined:
                                    status = "RTO"
                                elif "UNDELIVERED" in combined or "FAILED" in combined or "NDR" in combined:
                                    status = "Undelivered"
                                else:
                                    status = "Intransit"
                                
                                tracking_url = f"https://www.delhivery.com/track-v2/package/{tracking_no}"
                                eta_value = format_dt(shipment.get("ExpectedDeliveryDate", "") or shipment.get("EDD", ""))
                                pickup_value = format_dt(shipment.get("PickUpDate", "") or shipment.get("PickupDate", ""))
                                last_update_value = f"{raw_state} | {raw_event} | {location}".strip(" |")

                                if status == "Delivered":
                                    delivery_value = format_dt(shipment.get("DeliveryDate", "") or raw_date)
                                    if not delivery_value:
                                        for scan in (shipment.get("Scans", []) or []):
                                            sd = scan.get("ScanDetail") or {}
                                            if "DELIVERED" in (sd.get("Scan") or "").upper():
                                                delivery_value = format_dt(sd.get("ScanDateTime", ""))

                                rto_tracking_active = ("rto" in existing_rto.lower()) or ("return" in existing_rto.lower())
                                if status == "RTO":
                                    rto_value = last_update_value or "RTO Intransit"
                                elif rto_tracking_active and status == "Delivered":
                                    rto_value = "Delivered"
                                    
                                info = True
                                break  # Found data, stop trying other keys
                    except Exception:
                        pass
                        
            if not info:
                # iThink fallback
                ithink_order = get_ithink_shipment(tracking_no, ithink_token, ithink_secret)
                if ithink_order:
                    pinfo = parse_ithink(ithink_order)
                    if pinfo:
                        status = pinfo["status"]
                        eta_value = pinfo["eta"]
                        pickup_value = pinfo["pickup"]
                        delivery_value = pinfo["delivery"]
                        last_update_value = pinfo["last_update"]
                        rto_value = pinfo["rto"]
                        tracking_url = f"https://ithinklogistics.com/track/{tracking_no}" # Generic
                        info = True

            if not info:
                # Swiship / Amazon 
                payload = {"trackingNumber": tracking_no, "shipMethod": "ATS_STANDARD"}
                resp = requests.post(SWISHIP_URL, headers=SWISHIP_HEADERS, cookies=COOKIES, json=payload, timeout=10)
                if resp.status_code == 200:
                    try:
                        res = resp.json()
                        transit_state = res.get("transitState", "")
                        tracking_events = res.get("trackingEvents", [])
                        latest_event = tracking_events[0].get("eventDescription", "") if tracking_events else ""
                        latest_event_date = tracking_events[0].get("eventDate", "") if tracking_events else ""
                        
                        status = normalize_status(transit_state, latest_event)
                        tracking_url = f"https://www.swiship.co.uk/track?id={tracking_no}"

                        eta_value = format_dt(res.get("estimatedArrivalDate", ""))
                        pickup_value = format_dt(tracking_events[-1].get("eventDate", "")) if tracking_events else ""
                        last_update_value = latest_event
                        if latest_event_date:
                            last_update_value = f"{last_update_value} | {format_dt(latest_event_date)}".strip(" |")

                        delivered_event = next(
                            (ev for ev in tracking_events if "deliver" in ev.get("eventDescription", "").lower()), None
                        )
                        if delivered_event:
                            delivery_value = format_dt(delivered_event.get("eventDate", ""))

                        if status == "RTO":
                            rto_value = last_update_value or "RTO Intransit"
                        elif existing_rto and existing_rto.lower() != "delivered" and status == "Delivered":
                            rto_value = "Delivered"
                        
                        info = True
                    except Exception:
                        pass
                        
            # MCF SP-API Fallback
            if not info:
                from utils import get_access_token
                from w import fetch_mcf_data
                token, _ = get_access_token(secrets)
                if token:
                    tn, cc, mcf_status, raw = fetch_mcf_data(tracking_no, token)
                    if mcf_status and mcf_status != "NotFound":
                        status = mcf_status
                        carrier = cc or "MCF"
                        tracking_url = f"Tracking: {tn}" if tn else ""
                        info = True
                        
            if info and status not in {"Delivered", "RTO", "Intransit", "Lost", "Undelivered"}:
                status = "Intransit"
                
        except Exception:
            status = existing_status or "Intransit"

        # Push to batch payload
        if tracking_url: pending_updates.append({"range": f"{col_num_to_a1(col_tracking_url)}{row_num}", "values": [[tracking_url]]})
        pending_updates.append({"range": f"{col_num_to_a1(col_status)}{row_num}", "values": [[status]]})
        if eta_idx != -1 and eta_value: pending_updates.append({"range": f"{col_num_to_a1(eta_idx + 1)}{row_num}", "values": [[eta_value]]})
        if pickup_idx != -1 and pickup_value: pending_updates.append({"range": f"{col_num_to_a1(pickup_idx + 1)}{row_num}", "values": [[pickup_value]]})
        if delivery_idx != -1 and delivery_value: pending_updates.append({"range": f"{col_num_to_a1(delivery_idx + 1)}{row_num}", "values": [[delivery_value]]})
        if last_status_idx != -1 and last_update_value: pending_updates.append({"range": f"{col_num_to_a1(last_status_idx + 1)}{row_num}", "values": [[last_update_value]]})
        if rto_idx != -1 and rto_value: pending_updates.append({"range": f"{col_num_to_a1(rto_idx + 1)}{row_num}", "values": [[rto_value]]})

        summary_results.append({
            "Order ID": order_id,
            "Tracking ID": tracking_no,
            "Carrier": carrier,
            "Status": status,
            "ETA": eta_value,
            "Last Scan": last_update_value,
            "RTO": rto_value
        })

        print(f"  [{idx+1}/{total}] Row {row_num} | T: {tracking_no} | Carrier: {carrier} | Status: {status}")

        # Chunk updates to avoid connection drop
        if len(pending_updates) >= 150:
            flush_updates(pending_updates)
            pending_updates = []
            time.sleep(1)

    if pending_updates:
        flush_updates(pending_updates)

    print("\n[INFO] Live Tracking Update Finished.")
    return summary_results

if __name__ == "__main__":
    print("Starting Live Tracker (Terminal Mode)...")
    res = run_live_tracking_update()
    print(f"\n[DONE] Checked and updated tracking for {len(res)} items.")
