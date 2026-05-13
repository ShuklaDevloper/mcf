import os
import requests
import pandas as pd
from datetime import datetime
import json
import time

from utils import read_secret, init_sheets_service, SHEET_ID
from live_tracker import get_ithink_shipment, parse_ithink, format_dt, normalize_status
from live_tracker import SWISHIP_URL, DELHIVERY_URL, COOKIES, SWISHIP_HEADERS

def main():
    secrets = read_secret()
    delhivery_api_key = secrets.get("DELHIVERY_API_KEY", "")
    ithink_token = secrets.get("Ithink_access_token", "")
    ithink_secret = secrets.get("Ithink_secret_key", "")

    print("Initializing Sheets Service...")
    service = init_sheets_service()
    
    print("Fetching Sheet mapping (Tracking Numbers)...")
    try:
        result = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range='Sheet1!A:W').execute()
        rows = result.get('values', [])
    except Exception as e:
        print(f"Error fetching sheet: {e}")
        return

    if not rows:
        print("No rows found in sheet.")
        return

    headers = [str(h).strip().lower() for h in rows[0]]
    tracking_col_idx = -1
    for col in ["tracking no", "tracking no.", "tracking number"]:
        if col in headers:
            tracking_col_idx = headers.index(col)
            break
    
    if tracking_col_idx == -1:
        tracking_col_idx = 19 # Default to T
        print("Tracking header not found, defaulting to Column T")

    row_map = {}
    for i, row in enumerate(rows):
        if len(row) > tracking_col_idx:
            t_no = str(row[tracking_col_idx]).strip()
            if t_no:
                row_map[t_no] = i + 1

    print("Paste Tracking Numbers (separated by space or newline). Type 'DONE' on a new line when finished:")
    target_ids = []
    while True:
        try:
            line = input()
            if line.strip().upper() == "DONE":
                break
            target_ids.extend([oid.strip() for oid in line.split() if oid.strip()])
        except EOFError:
            break
            
    if not target_ids:
        print("No Tracking Numbers provided.")
        return

    numbers = list(dict.fromkeys(target_ids))
    results = []
    v_updates = []
    
    try:
        for idx, tracking_no in enumerate(numbers):
            status = "Intransit"
            eta_value, pickup_value, delivery_value = "", "", ""
            last_update_value, tracking_url = "", ""
            carrier = ""
            info = False
            
            # 1. Delhivery
            if delhivery_api_key and not info:
                try:
                    resp = requests.get(DELHIVERY_URL, params={"waybill": tracking_no, "token": delhivery_api_key}, timeout=15)
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
                            carrier = "Delhivery"
                            info = True
                except Exception as e:
                    pass
                    
            # 2. iThink
            if not info:
                try:
                    ithink_order = get_ithink_shipment(tracking_no, ithink_token, ithink_secret)
                    if ithink_order:
                        pinfo = parse_ithink(ithink_order)
                        if pinfo:
                            status = pinfo["status"]
                            eta_value = pinfo["eta"]
                            pickup_value = pinfo["pickup"]
                            delivery_value = pinfo["delivery"]
                            last_update_value = pinfo["last_update"]
                            tracking_url = f"https://ithinklogistics.com/track/{tracking_no}"
                            carrier = "iThink Logistics"
                            info = True
                except Exception as e:
                    pass

            # 3. Swiship
            if not info:
                try:
                    payload = {"trackingNumber": tracking_no, "shipMethod": "ATS_STANDARD"}
                    resp = requests.post(SWISHIP_URL, headers=SWISHIP_HEADERS, cookies=COOKIES, json=payload, timeout=10)
                    if resp.status_code == 200:
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
                        
                        delivered_event = next((ev for ev in tracking_events if "deliver" in ev.get("eventDescription", "").lower()), None)
                        if delivered_event:
                            delivery_value = format_dt(delivered_event.get("eventDate", ""))
                        
                        carrier = "Swiship (Amazon)"
                        info = True
                except Exception as e:
                    pass
                    
            # 4. MCF SP-API Fallback
            if not info:
                from utils import get_access_token
                from w import fetch_mcf_data
                try:
                    token, _ = get_access_token(secrets)
                    if token:
                        tn, cc, mcf_status, raw = fetch_mcf_data(tracking_no, token)
                        if mcf_status and mcf_status != "NotFound":
                            status = mcf_status
                            carrier = cc or "MCF (Amazon)"
                            tracking_url = f"Tracking: {tn}" if tn else ""
                            info = True
                except Exception as e:
                    pass

            if not info:
                status = "Not Found"
                
            results.append({
                "Tracking Number": tracking_no,
                "Carrier": carrier,
                "Status": status,
                "Last Update": last_update_value,
                "ETA": eta_value,
                "Pickup Date": pickup_value,
                "Delivery Date": delivery_value,
                "Tracking URL": tracking_url
            })
            
            row_num = row_map.get(tracking_no)
            if row_num:
                v_updates.append({
                    "range": f"Sheet1!V{row_num}",
                    "values": [[status]]
                })
                sheet_msg = f" | Updated Row {row_num}"
            else:
                sheet_msg = " | Not in Sheet"
                
            print(f"Tracking {idx+1}/{len(numbers)}: {tracking_no} -> {status} ({carrier}){sheet_msg}")
            
            # Small delay to prevent rate limits
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nProcess interrupted by user! Saving current progress...")
    finally:
        if v_updates:
            print("\nUpdating Google Sheet Column V...")
            try:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=SHEET_ID,
                    body={"valueInputOption": "RAW", "data": v_updates}
                ).execute()
                print("✅ Google Sheet Updated!")
            except Exception as e:
                print(f"Error updating sheet: {e}")
                
        if results:
            df = pd.DataFrame(results)
            df.to_excel("Tracking_Results.xlsx", index=False)
            print(f"Saved {len(results)} results to Tracking_Results.xlsx")
        else:
            print("No results to save.")

if __name__ == "__main__":
    main()
