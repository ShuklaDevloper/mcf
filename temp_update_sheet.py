import requests
import json
import time
from utils import (
    init_sheets_service, 
    read_secret, 
    get_shopify_config, 
    get_shopify_order, 
    update_sheet_remarks, 
    update_sheet_tracking, 
    SHEET_ID
)

def run_temp_update(order_ids):
    secrets = read_secret()
    shopify_cfg = get_shopify_config(secrets)
    
    print(f"Initializing Sheets Service...")
    service = init_sheets_service()
    
    print("Fetching Sheet mapping (Column B)...")
    try:
        # Fetching first 5000 rows of A and B
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, 
            range='Sheet1!A:C'
        ).execute()
        rows = result.get('values', [])
    except Exception as e:
        print(f"Error fetching sheet: {e}")
        return

    if not rows:
        print("No rows found in sheet.")
        return

    # Find Order ID column
    headers = [str(h).strip().lower() for h in rows[0]]
    oid_idx = -1
    for col in ["ord=", "ord_serial", "order id", "order"]:
        if col in headers:
            oid_idx = headers.index(col)
            break
    if oid_idx == -1:
        oid_idx = 1 # Default to B
        print(f"Header not found, defaulting to Column Index {oid_idx}")
    else:
        print(f"Found Order ID header at index {oid_idx}")

    row_map = {}
    for i, row in enumerate(rows):
        if len(row) > oid_idx:
            oid = str(row[oid_idx]).replace("#", "").strip()
            if oid:
                row_map[oid] = i + 1

    qr_updates = []
    st_updates = []

    print(f"Processing {len(order_ids)} orders...")
    for oid in order_ids:
        oid_clean = str(oid).strip()
        print(f"\nChecking Order ID: {oid_clean}...")
        
        if oid_clean not in row_map:
            print(f"  [!] Order ID {oid_clean} not found in Google Sheet.")
            continue
            
        row_num = row_map[oid_clean]
        
        # 1. Get from Shopify (Search by name specifically)
        shop_url = shopify_cfg["shop_url"]
        headers = shopify_cfg["headers"]
        search_url = f"{shop_url}/admin/api/2024-01/orders.json"
        
        s_order = None
        # Try search by name (Shopify API name filter)
        try:
            # We try both #3775 and 3775
            for search_name in [f"#{oid_clean}", oid_clean]:
                resp = requests.get(search_url, headers=headers, params={"name": search_name, "status": "any"})
                orders = resp.json().get("orders", [])
                if orders:
                    s_order = orders[0]
                    break
        except Exception as e:
            print(f"  [!] Shopify search error: {e}")

        if not s_order:
            print(f"  [!] Order {oid_clean} not found on Shopify after search.")
            continue
            
        # 2. Get Fulfillments
        f_url = f"{shop_url}/admin/api/2024-01/orders/{s_order['id']}/fulfillments.json"
        
        tracking_no = ""
        carrier = ""
        final_status = "Pending"
        
        if s_order.get("cancelled_at"):
            final_status = "Cancelled"
            
        try:
            fr = requests.get(f_url, headers=headers)
            fulfillments = fr.json().get("fulfillments", [])
            for f in fulfillments:
                if f.get("status") in ["success", "pending"]:
                    tracking_no = f.get("tracking_number") or ""
                    carrier = f.get("tracking_company") or "Shopify"
                    if tracking_no:
                        final_status = "Fulfilled"
                    break
        except Exception as e:
            print(f"  [!] Error fetching fulfillments: {e}")

        # If tracking_no is still empty, check MCF
        if not tracking_no:
            from w import fetch_mcf_data
            from utils import get_access_token
            try:
                token, _ = get_access_token(secrets)
                if token:
                    tn, cc, mcf_status, raw = fetch_mcf_data(oid_clean, token)
                    if tn:
                        tracking_no = tn
                        carrier = cc or "MCF"
                        final_status = "Fulfilled"
                    elif mcf_status and mcf_status != "NotFound":
                        final_status = mcf_status
            except Exception as e:
                pass
                
        if not tracking_no and final_status == "Pending":
            if s_order.get("fulfillment_status") == "fulfilled":
                final_status = "Fulfilled"
            elif not s_order.get("cancelled_at"):
                final_status = "Unfulfilled"

        # 3. Prepare Updates
        if final_status.lower() == "cancelled":
            qr_updates.append({
                "row": row_num,
                "source": "Cancelled",
                "status": "Cancelled"
            })
            st_updates.append({
                "row": row_num,
                "carrier": "",
                "tracking_no": "",
                "url": "",
                "remark": "Cancelled"
            })
        else:
            qr_updates.append({
                "row": row_num,
                "source": carrier if carrier else "Manual",
                "status": final_status
            })
            st_updates.append({
                "row": row_num,
                "carrier": carrier if tracking_no else "",
                "tracking_no": tracking_no,
                "url": "",
                "remark": "Script Update" if tracking_no else final_status
            })
        print(f"  [+] {carrier} | {tracking_no} | Status: {final_status} | Row: {row_num}")

    # 4. Push to Sheet
    if qr_updates:
        print("\nUpdating Q/R columns...")
        update_sheet_remarks(service, SHEET_ID, qr_updates)
    if st_updates:
        print("Updating S/T/U/V columns...")
        update_sheet_tracking(service, SHEET_ID, st_updates)

    print("\n[DONE] All updates pushed to Google Sheet.")

if __name__ == "__main__":
    print("Paste Order IDs (separated by space or newline). Type 'DONE' on a new line when finished:")
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
        unique_ids = list(dict.fromkeys(target_ids))
        run_temp_update(unique_ids)
    else:
        print("No Order IDs provided.")
