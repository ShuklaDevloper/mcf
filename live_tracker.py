import os
import requests
import json
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

def col_num_to_a1(col_num):
    # col_num is 1-based index to A, B, C...
    result = ""
    while col_num > 0:
        col_num, rem = divmod(col_num - 1, 26)
        result = chr(65 + rem) + result
    return result

def run_live_tracking_update(progress_callback=None):
    """
    Downloads Sheet via API, checks tracking for all MCF/Delhivery rows,
    updates sheet locally and flushes to remote.
    Returns: list of dicts with summary results.
    """
    secrets = read_secret()
    delhivery_api_key = secrets.get("DELHIVERY_API_KEY", "")

    try:
        service = init_sheets_service()
    except Exception as e:
        return [{"order_id": "Error", "status": f"Sheets service init failed: {str(e)}", "carrier": "", "desc": ""}]

    # Fetch rows
    range_name = 'Sheet1!A:W'
    result = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=range_name).execute()
    rows = result.get('values', [])

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
    delivery_idx = get_idx("delivery date", "deliverydate")
    last_status_idx = get_idx("last status", "last update", "last_update")
    rto_idx = get_idx("rto")

    if source_idx == -1 or tracking_no_idx == -1:
        return [{"order_id": "Error", "status": f"Could not find required columns (Source/Tracking). Headers: {headers}", "carrier": "", "desc": ""}]

    pending_updates = []
    summary_results = []
    
    # Pre-calculated 1-based column indices for A1 notation
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
        order_id = safe_get(order_id_idx).replace("#", "")

        is_mcf = source.upper() == "MCF"
        is_delhivery = "delhivery" in carrier.lower()

        if tracking_no and (is_mcf or is_delhivery):
            if existing_rto.lower() == "delivered":
                continue # Already at end state
            
            orders_to_check.append({
                "row_index": i,
                "order_id": order_id,
                "tracking_no": tracking_no,
                "carrier": carrier,
                "existing_rto": existing_rto
            })

    total = len(orders_to_check)
    if total == 0:
        return []

    for idx, item in enumerate(orders_to_check):
        order_id = item["order_id"]
        tracking_no = item["tracking_no"]
        carrier = item["carrier"]
        existing_rto = item["existing_rto"]
        row_num = item["row_index"] + 1

        if progress_callback:
            progress_callback(idx, total, tracking_no)

        eta_value, pickup_value, delivery_value = "", "", ""
        last_update_value, rto_value, tracking_url = "", "", ""

        try:
            if "delhivery" in carrier.lower() and delhivery_api_key:
                resp = requests.get(DELHIVERY_URL, params={"waybill": tracking_no, "token": delhivery_api_key}, timeout=15)
                if resp.status_code == 200:
                    try:
                        res = resp.json()
                        shipment_data = res.get("ShipmentData", [])
                        shipment = shipment_data[0].get("Shipment", {}) if shipment_data else {}
                        status_obj = shipment.get("Status", {})
                        
                        raw_state = status_obj.get("Status", "")
                        raw_event = status_obj.get("Instructions", "")
                        raw_date = status_obj.get("StatusDateTime", "") or status_obj.get("StatusDate", "")
                        status = normalize_status(raw_state, raw_event)
                        tracking_url = f"https://www.delhivery.com/track-v2/package/{tracking_no}"

                        eta_value = format_dt(shipment.get("ExpectedDeliveryDate", "") or shipment.get("EDD", ""))
                        pickup_value = format_dt(shipment.get("PickUpDate", "") or shipment.get("PickupDate", ""))
                        last_update_value = f"{raw_state} {raw_event}".strip()
                        if raw_date:
                            last_update_value = f"{last_update_value} | {format_dt(raw_date)}".strip(" |")

                        if status == "Delivered":
                            delivery_value = format_dt(shipment.get("DeliveryDate", "") or raw_date)

                        rto_tracking_active = ("rto" in existing_rto.lower()) or ("return" in existing_rto.lower())
                        current_text = f"{raw_state} {raw_event}".lower()
                        if status == "RTO" or "rto" in current_text or "return" in current_text:
                            rto_value = last_update_value or "RTO Intransit"
                        elif rto_tracking_active and status == "Delivered":
                            rto_value = "Delivered"
                    except Exception:
                        status = "Intransit"
                else:
                    status = "Intransit"
            else:
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
                        
                    except Exception:
                        status = "Intransit"
                else:
                    status = "Intransit"

            if status not in {"Delivered", "RTO", "Intransit", "Lost"}:
                status = "Intransit"
                
        except Exception:
            status = "Intransit"

        # Push to batch payload
        pending_updates.append({"range": f"{col_num_to_a1(col_tracking_url)}{row_num}", "values": [[tracking_url]]})
        pending_updates.append({"range": f"{col_num_to_a1(col_status)}{row_num}", "values": [[status]]})
        if eta_idx != -1: pending_updates.append({"range": f"{col_num_to_a1(eta_idx + 1)}{row_num}", "values": [[eta_value]]})
        if pickup_idx != -1: pending_updates.append({"range": f"{col_num_to_a1(pickup_idx + 1)}{row_num}", "values": [[pickup_value]]})
        if delivery_idx != -1: pending_updates.append({"range": f"{col_num_to_a1(delivery_idx + 1)}{row_num}", "values": [[delivery_value]]})
        if last_status_idx != -1: pending_updates.append({"range": f"{col_num_to_a1(last_status_idx + 1)}{row_num}", "values": [[last_update_value]]})
        if rto_idx != -1: pending_updates.append({"range": f"{col_num_to_a1(rto_idx + 1)}{row_num}", "values": [[rto_value]]})

        summary_results.append({
            "Order ID": order_id,
            "Tracking ID": tracking_no,
            "Carrier": carrier,
            "Status": status,
            "ETA": eta_value,
            "Last Scan": last_update_value,
            "RTO": rto_value
        })

    if pending_updates:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "RAW", "data": pending_updates}
        ).execute()

    return summary_results
