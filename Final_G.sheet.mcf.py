import os
import requests
import json
import urllib3
import re
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# 🔐 Read Secrets
# =========================
def read_secret():
    secrets = {}
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secret_path = os.path.join(script_dir, "secret.txt")
    if os.path.exists(secret_path):
        with open(secret_path, "r", encoding="utf-8") as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    secrets[k.strip()] = v.strip()
    
    try:
        import streamlit as st
        for k, v in st.secrets.items():
            if k not in secrets and isinstance(v, str):
                secrets[k] = v
    except Exception:
        pass
        
    return secrets

def get_access_token(config):
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": config.get('IN_LWA_REFRESH_TOKEN'),
        "client_id": config.get('SP_API_LWA_APP_ID'),
        "client_secret": config.get('SP_API_LWA_CLIENT_SECRET')
    }
    response = requests.post(url, data=payload)
    if response.status_code != 200:
        print(f"Amazon Auth Error: {response.text}")
    return response.json().get('access_token')

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").isoformat() + "Z"
    except ValueError:
        return datetime.utcnow().isoformat() + "Z"

# =========================
# ✅ Data Sanitization
# =========================
def clean_phone_number(phone_str):
    if not phone_str: return ""
    phone_str = str(phone_str)
    # Remove all non-digit characters (+, -, spaces)
    digits = re.sub(r'\D', '', phone_str)
    # Extract only the last 10 digits
    if len(digits) >= 10:
        return digits[-10:]
    return digits # Fallback

def format_address(full_address):
    if not full_address: return "", "", ""
    words = str(full_address).split()
    line1, line2, line3 = "", "", ""
    for word in words:
        if len(line1) + len(word) + 1 <= 60:
            line1 += (word + " ")
        elif len(line2) + len(word) + 1 <= 60:
            line2 += (word + " ")
        elif len(line3) + len(word) + 1 <= 60:
            line3 += (word + " ")
    return line1.strip()[:60], line2.strip()[:60], line3.strip()[:60]

# =========================
# ✅ Google Sheets Actions
# =========================
def update_sheet_status(service, sheet_id, updates):
    if not updates: return
    data = []
    for u in updates:
        data.append({
            "range": f"Sheet1!Q{u['row']}:R{u['row']}",
            "values": [[u['source'], u['status']]]
        })
    body = {"valueInputOption": "RAW", "data": data}
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id,
        body=body
    ).execute()

# =========================
# ✅ Shopify Actions
# =========================
def get_shopify_order(order_number, headers, shop_url):
    url = f"{shop_url}/admin/api/2024-01/orders.json"
    params = {"status": "any", "limit": 250}
    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()
    for o in r.json().get("orders", []):
        if o.get("name") == f"#{order_number}":
            return o
    return None

def fulfill_order(order, headers, shop_url):
    order_id = order["id"]
    url = f"{shop_url}/admin/api/2024-01/orders/{order_id}/fulfillment_orders.json"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    fulfillment_orders = r.json().get("fulfillment_orders", [])
    for fo in fulfillment_orders:
        if fo["status"] == "open":
            payload = {
                "fulfillment": {
                    "line_items_by_fulfillment_order": [{"fulfillment_order_id": fo["id"]}]
                }
            }
            f_url = f"{shop_url}/admin/api/2024-01/fulfillments.json"
            fr = requests.post(f_url, headers=headers, json=payload)
            return fr.status_code in [200, 201]
    return False

# =========================
# 🚀 MAIN PROCESS
# =========================
def process_orders():
    secrets = read_secret()
    token = get_access_token(secrets)
    if not token:
        print("🛑 Amazon Auth Failed")
        return

    # Google Sheets Setup
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, 'hide.json')
    
    if os.path.exists(creds_path):
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    else:
        try:
            import streamlit as st
            creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        except Exception as e:
            print("Google Service Account auth failed. Please provide hide.json or st.secrets['gcp_service_account']: " + str(e))
            return
            
    service = build('sheets', 'v4', credentials=creds)
    
    # NEW SHEET ID
    sheet_id = '1OvtzHInl8viaLG6f2ZLG3u5h6YQpfID2UAbI64cYhF4'

    # Shopify Setup (Use .get to avoid error since keys were removed from secret.txt)
    shop_url = secrets.get("monozo_url", "").rstrip("/")
    headers_shopify = {
        "X-Shopify-Access-Token": secrets.get("monozo_shopi_assesstoken", ""),
        "Content-Type": "application/json"
    }

    # Read Sheet1 (Columns A to R)
    range_name = 'Sheet1!A:R'
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
    rows = result.get('values', [])

    if len(rows) <= 1:
        print("No orders found in Sheet1.")
        return

    url_amazon = "https://sellingpartnerapi-eu.amazon.com/fba/outbound/2020-07-01/fulfillmentOrders"
    headers_amazon = {'x-amz-access-token': token, 'Content-Type': 'application/json'}

    # 1. Group Orders by order_id
    grouped_orders = {}
    for idx, row in enumerate(rows[1:], start=2):
        if not row: continue
        order_id = row[0].replace("#", "").strip()
        if not order_id: continue
        
        # Parse basic fields (take from first matching row)
        if order_id not in grouped_orders:
            order_date = row[2] if len(row) > 2 else ""   # C: Date
            name = row[3] if len(row) > 3 else ""         # D: Customer Name
            phone = row[4] if len(row) > 4 else ""        # E: Phone Num
            amount = row[5] if len(row) > 5 else "0"      # F: Amount
            addr1 = row[6] if len(row) > 6 else ""        # G: Address 1
            addr2 = row[7] if len(row) > 7 else ""        # H: Address 2
            pin = row[8] if len(row) > 8 else ""          # I: Pin code
            state_code = row[9] if len(row) > 9 else ""   # J: Stat (state code)
            city = row[10] if len(row) > 10 else ""       # K: city
            status = row[17] if len(row) > 17 else ""     # R: status
            
            # Skip if already fulfilled or has error
            if "fulfi" in status.lower() or "error" in status.lower():
                continue

            grouped_orders[order_id] = {
                "all_row_indices": [idx], # All rows for this order to update Q/R later
                "date": order_date,
                "name": name,
                "phone": clean_phone_number(phone),
                "amount": amount,
                "addr1": addr1,
                "addr2": addr2,
                "pin": pin,
                "city": city,
                "state_code": state_code,
                "items": []
            }
        else:
            # Add row index to the group
            grouped_orders[order_id]["all_row_indices"].append(idx)

        # Parse item
        sku = row[13] if len(row) > 13 else ""   # N: sellerSku
        try:
            qty = int(row[15]) if len(row) > 15 else 1  # P: quantity
        except (ValueError, IndexError):
            qty = 1

        # Add item to the group list
        if sku:
            amount_val = row[5] if len(row) > 5 else "0"  # F: Amount
            grouped_orders[order_id]["items"].append({
                "sellerSku": sku,
                "sellerFulfillmentOrderItemId": f"{sku}-{order_id}-{qty}-{idx}",
                "quantity": qty,
                "perUnitDeclaredValue": {"currencyCode": "INR", "value": amount_val}
            })

    print(f"📄 Total Unique Orders to Process: {len(grouped_orders)}\n")

    sheet_updates = []

    for order_id, o_data in grouped_orders.items():
        if not o_data["items"]:
            # Could skip entirely if there's no item found for an order
            continue
            
        full_address = f"{o_data['addr1']} {o_data['addr2']}"
        add1, add2, add3 = format_address(full_address)

        # Amazon Payload
        payload = {
            "marketplaceId": "A21TJRUUN4KGV",
            "sellerFulfillmentOrderId": order_id,
            "displayableOrderId": order_id,
            "displayableOrderDate": parse_date(o_data['date']),
            "displayableOrderComment": "Shopify Order - Fulfilled by Amazon",
            "shippingSpeedCategory": "Standard",
            "destinationAddress": {
                "name": o_data['name'],
                "addressLine1": add1,
                "addressLine2": add2,
                "addressLine3": add3,
                "city": o_data['city'],
                "stateOrRegion": o_data['state_code'],
                "postalCode": o_data['pin'],
                "countryCode": "IN",
                "phone": o_data['phone']
            },
            "fulfillmentAction": "Ship",
            "fulfillmentPolicy": "FillOrKill",
            "codSettings": {
                "isCodRequired": True,
                "codCharge": {"currencyCode": "INR", "value": o_data['amount']}
            },
            "items": o_data["items"]
        }

        # Amazon API Call
        response = requests.post(url_amazon, headers=headers_amazon, json=payload, verify=False)
        amazon_success = response.status_code in [200, 201]
        error_msg = ""
        
        if not amazon_success:
            try:
                error_msg = response.json().get("errors", [{}])[0].get("message", "")
            except:
                error_msg = response.text

        is_processed = amazon_success or "already exists" in error_msg.lower()

        if is_processed:
            if shop_url:
                shopify_status = "⏩ Checking Shopify..."
                order = get_shopify_order(order_id, headers_shopify, shop_url)
                if order:
                    if fulfill_order(order, headers_shopify, shop_url):
                        shopify_status = "✅ Fulfilled"
                    else:
                        shopify_status = "❌ Shopify Fail"
                else:
                    shopify_status = "❓ Not Found"
            else:
                shopify_status = "⏩ Shopify Skipped (no URL)"
            
            # Queue successful updates for Q and R columns
            for r_idx in o_data["all_row_indices"]:
                sheet_updates.append({'row': r_idx, 'source': 'MCF', 'status': 'Fulfilled'})
        else:
            shopify_status = "⏩ Skipped"
            # Queue error updates for Q and R columns
            for r_idx in o_data["all_row_indices"]:
                sheet_updates.append({'row': r_idx, 'source': '', 'status': 'Error'})

        # Console Summary
        amazon_str = "✅ Success" if amazon_success else ("✅ Already Exists" if "already exists" in error_msg.lower() else f"❌ Failed ({error_msg})")
        total_qty = sum(item["quantity"] for item in o_data["items"])
        print(f"Order: {order_id} | Multi-items: {len(o_data['items'])} | Total Qty: {total_qty} | Amazon: {amazon_str} | Shopify: {shopify_status}")

    # Process all Google Sheet updates via batch request
    if sheet_updates:
        print("\nUpdating Google Sheet (Batch Update)...")
        update_sheet_status(service, sheet_id, sheet_updates)
        print("✅ Google Sheet Updated Successfully in Columns Q & R!")

if __name__ == "__main__":
    process_orders()