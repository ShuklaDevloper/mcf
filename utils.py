"""
utils.py — Shared utilities for Order Fulfillment System
All API integrations: Amazon SP-API (MCF), Delhivery, Shopify, Google Sheets
"""
import os
import re
import json
import requests
import urllib3
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
SHEET_ID = "1OvtzHInl8viaLG6f2ZLG3u5h6YQpfID2UAbI64cYhF4"

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxDNOr8zmH97UcLeF52AZ5O_uscSpe7tyTxrChbCpIuFnhpVSYZ-xlqreuOOvrBuH3O/exec"
MCF_API_URL = "https://sellingpartnerapi-eu.amazon.com/fba/outbound/2020-07-01/fulfillmentOrders"
MARKETPLACE_ID = "A21TJRUUN4KGV"  # India

# ─────────────────────────────────────────────
# SECRETS & AUTH
# ─────────────────────────────────────────────
def read_secret(file_name="secret.txt"):
    """Read KEY=VALUE pairs from secret.txt"""
    secrets = {}
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, file_name)
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if "=" in line and not line.strip().startswith("#"):
                        k, v = line.strip().split("=", 1)
                        secrets[k.strip()] = v.strip()
    except Exception as e:
        print(f"[utils] secret.txt read error: {e}")

    try:
        import streamlit as st
        for k, v in st.secrets.items():
            if k not in secrets and isinstance(v, str):
                secrets[k] = v
    except Exception:
        pass

    return secrets


def get_access_token(config):
    """Get Amazon SP-API access token via LWA refresh token.
    Returns (token_str, error_str) — one will be None."""
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": config.get("IN_LWA_REFRESH_TOKEN"),
        "client_id": config.get("SP_API_LWA_APP_ID"),
        "client_secret": config.get("SP_API_LWA_CLIENT_SECRET"),
    }
    try:
        r = requests.post(url, data=payload, timeout=30)
        data = r.json()
        if "access_token" in data:
            return data["access_token"], None
        return None, data.get("error_description", "Unknown auth error")
    except Exception as e:
        return None, str(e)


def get_shopify_config(secrets):
    """Return Shopify shop_url and auth headers."""
    return {
        "shop_url": secrets.get("shop_url", "").rstrip("/"),
        "headers": {
            "X-Shopify-Access-Token": secrets.get("shop_assesstoken", ""),
            "Content-Type": "application/json",
        },
    }


# ─────────────────────────────────────────────
# DATA SANITIZATION
# ─────────────────────────────────────────────
def clean_phone_number(phone_str):
    """Extract last 10 digits — removes +91, dashes, spaces, etc."""
    if not phone_str:
        return ""
    digits = re.sub(r"\D", "", str(phone_str))
    return digits[-10:] if len(digits) >= 10 else digits


def validate_address(full_address):
    """Split address into 3 lines of max 60 chars each.
    Returns (line1, line2, line3, is_valid).
    is_valid=False if words overflow or address is empty."""
    if not full_address or not str(full_address).strip():
        return "", "", "", False
    words = str(full_address).split()
    line1, line2, line3, overflow = "", "", "", []
    for word in words:
        if len(line1) + len(word) + 1 <= 60:
            line1 += word + " "
        elif len(line2) + len(word) + 1 <= 60:
            line2 += word + " "
        elif len(line3) + len(word) + 1 <= 60:
            line3 += word + " "
        else:
            overflow.append(word)
    is_valid = len(overflow) == 0 and len(line1.strip()) > 0
    return line1.strip()[:60], line2.strip()[:60], line3.strip()[:60], is_valid


def validate_pincode(pincode):
    """Check if pincode is exactly 6 digits."""
    return bool(re.fullmatch(r"\d{6}", re.sub(r"\D", "", str(pincode or ""))))


def parse_date(date_str):
    """Convert various date formats to ISO 8601 UTC string."""
    if not date_str:
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    for fmt in ["%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# ─────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────
def init_sheets_service():
    """Build Google Sheets API service from hide.json service account."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(script_dir, "hide.json")
    
    if os.path.exists(creds_path):
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    else:
        try:
            import streamlit as st
            creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        except Exception as e:
            raise Exception("hide.json missing, and st.secrets['gcp_service_account'] not found: " + str(e))
            
    return build("sheets", "v4", credentials=creds)


def ensure_sheet_capacity(service, sheet_id, max_row_needed):
    """Automatically add rows to the sheet if the requested row exceeds current grid capacity."""
    try:
        sheet_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        for sheet in sheet_metadata.get('sheets', []):
            if sheet.get("properties", {}).get("title") == "Sheet1":
                grid = sheet.get("properties", {}).get("gridProperties", {})
                current_rows = grid.get("rowCount", 0)
                sheet_id_int = sheet.get("properties", {}).get("sheetId", 0)
                if max_row_needed > current_rows:
                    add_rows = max_row_needed - current_rows + 500
                    body = {
                        "requests": [{
                            "appendDimension": {
                                "sheetId": sheet_id_int,
                                "dimension": "ROWS",
                                "length": add_rows
                            }
                        }]
                    }
                    service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()
                break
    except Exception as e:
        print("[utils] ensure_sheet_capacity error:", e)

def update_sheet_remarks(service, sheet_id, updates):
    """Batch update columns Q (source) and R (status) for given row numbers.
    updates = [{"row": 2, "source": "MCF", "status": "Fulfilled"}, ...]
    """
    if not updates:
        return
    max_row = max(u['row'] for u in updates)
    ensure_sheet_capacity(service, sheet_id, max_row)
    data = [
        {
            "range": f"Sheet1!Q{u['row']}:R{u['row']}",
            "values": [[u.get("source", ""), u.get("status", "")]],
        }
        for u in updates
    ]
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


def update_sheet_tracking(service, sheet_id, updates):
    """Batch update columns S (carrier), T (tracking_no), U (tracking_url), V (remark).
    updates = [{"row": 2, "carrier": "...", "tracking_no": "...", "url": "...", "remark": "..."}, ...]
    """
    if not updates:
        return
    max_row = max(u['row'] for u in updates)
    ensure_sheet_capacity(service, sheet_id, max_row)
    from datetime import datetime
    now_str = datetime.now().strftime("%d/%m %H:%M")
    data = []
    for u in updates:
        tn = u.get("tracking_no", "")
        remark = u.get("remark") or (f"Tracking Added {now_str}" if tn else u.get("mcf_status", "Pending"))
        data.append({
            "range": f"Sheet1!S{u['row']}:V{u['row']}",
            "values": [[u.get("carrier", ""), tn, u.get("url", ""), remark]],
        })
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


# ─────────────────────────────────────────────
# SHOPIFY
# ─────────────────────────────────────────────
def get_shopify_order(order_number, headers, shop_url):
    """Find a Shopify order by order number.
    Shopify stores name as '2916' (no #). We try both formats to be safe.
    """
    clean_num = str(order_number).replace("#", "").strip()
    url = f"{shop_url}/admin/api/2024-01/orders.json"
    params = {"status": "any", "limit": 250}

    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()

    # Match against clean number (Shopify name = '2916') AND with # (just in case)
    for o in r.json().get("orders", []):
        name = str(o.get("name", ""))
        if name == clean_num or name == f"#{clean_num}":
            return o
    return None


def fulfill_order(order, headers, shop_url, tracking_info=None):
    """Mark a Shopify order as fulfilled, with optional tracking info.
    - If open fulfillment_order exists → create fulfillment WITH tracking.
    - If already fulfilled → update tracking on existing fulfillment via update_tracking API.
    Returns True if action taken or tracking updated, False if nothing to do.
    """
    order_id = order["id"]
    order_name = order.get("name", str(order_id))

    # ── Step 1: Try to create a new fulfillment (for open orders) ──────────
    fo_url = f"{shop_url}/admin/api/2024-01/orders/{order_id}/fulfillment_orders.json"
    r = requests.get(fo_url, headers=headers)
    r.raise_for_status()

    for fo in r.json().get("fulfillment_orders", []):
        if fo["status"] == "open":
            payload = {
                "fulfillment": {
                    "line_items_by_fulfillment_order": [
                        {"fulfillment_order_id": fo["id"]}
                    ]
                }
            }
            if tracking_info and tracking_info.get("number"):
                payload["fulfillment"]["tracking_info"] = {
                    "number":  tracking_info["number"],
                    "company": tracking_info.get("company", ""),
                    "url":     tracking_info.get("url", ""),
                }
                payload["fulfillment"]["notify_customer"] = False

            fr = requests.post(
                f"{shop_url}/admin/api/2024-01/fulfillments.json",
                headers=headers, json=payload
            )
            fr.raise_for_status()
            print(f"✅ Fulfilled: {order_name}"
                  + (f" | Tracking: {tracking_info['number']}" if tracking_info and tracking_info.get("number") else ""))
            return True

    # ── Step 2: Already fulfilled — update tracking on existing fulfillment ─
    if tracking_info and tracking_info.get("number"):
        f_url = f"{shop_url}/admin/api/2024-01/orders/{order_id}/fulfillments.json"
        fr = requests.get(f_url, headers=headers)
        fr.raise_for_status()
        for f in fr.json().get("fulfillments", []):
            if f.get("status") in ["success", "pending"]:
                upd_url = f"{shop_url}/admin/api/2024-01/fulfillments/{f['id']}/update_tracking.json"
                upd_payload = {
                    "fulfillment": {
                        "tracking_info": {
                            "number":  tracking_info["number"],
                            "company": tracking_info.get("company", ""),
                            "url":     tracking_info.get("url", ""),
                        },
                        "notify_customer": False,
                    }
                }
                ur = requests.post(upd_url, headers=headers, json=upd_payload)
                ur.raise_for_status()
                print(f"✅ Tracking updated on Shopify: {order_name} → {tracking_info['number']}")
                return True

    print(f"⏭️ Already fulfilled / no action needed: {order_name}")
    return False


# ─────────────────────────────────────────────
# AMAZON MCF
# ─────────────────────────────────────────────
def create_mcf_order(token, order_data):
    """Submit a fulfillment order to Amazon MCF (SP-API).
    Returns (success: bool, message: str)
    """
    headers = {"x-amz-access-token": token, "Content-Type": "application/json"}
    is_cod = "cod" in str(order_data.get("is_cod", "")).lower()

    payload = {
        "marketplaceId": MARKETPLACE_ID,
        "sellerFulfillmentOrderId": order_data["order_id"],
        "displayableOrderId": order_data["order_id"],
        "displayableOrderDate": parse_date(order_data.get("date", "")),
        "displayableOrderComment": "Shopify Order - Fulfilled by Amazon MCF",
        "shippingSpeedCategory": "Standard",
        "destinationAddress": {
            "name": order_data.get("customer", ""),
            "addressLine1": order_data.get("addr_line1", ""),
            "addressLine2": order_data.get("addr_line2", ""),
            "addressLine3": order_data.get("addr_line3", ""),
            "city": order_data.get("city", ""),
            "stateOrRegion": order_data.get("state_code", ""),
            "postalCode": order_data.get("pincode", ""),
            "countryCode": "IN",
            "phone": order_data.get("phone", ""),
        },
        "fulfillmentAction": "Ship",
        "fulfillmentPolicy": "FillOrKill",
        "items": order_data.get("items", []),
    }

    if is_cod:
        payload["codSettings"] = {
            "isCodRequired": True,
            "codCharge": {"currencyCode": "INR", "value": str(order_data.get("amount", "0"))},
        }
    else:
        # FBA India Prepaid orders workaround
        payment_info = order_data.get("payment_info")
        if not payment_info:
            payment_info = [{
                "PaymentMethod": "Prepaid",
                "PaymentAmount": float(order_data.get("amount", 0)),
                "CurrencyCode": "INR"
            }]
        payload["paymentInformationList"] = payment_info

    try:
        r = requests.post(MCF_API_URL, headers=headers, json=payload, verify=False, timeout=30)
        if r.status_code in [200, 201]:
            return True, "Success"
        try:
            err = r.json().get("errors", [{}])[0].get("message", r.text)
        except Exception:
            err = r.text
        if "already exists" in err.lower():
            return True, "Already exists"
        return False, err
    except Exception as e:
        return False, str(e)


def get_mcf_tracking_info(token, order_id):
    """Fetch tracking info for an MCF order from SP-API.
    Tries clean_id first, then #clean_id if 404.
    Returns (found: bool, tracking_dict_or_error_str)
    """
    import urllib.parse

    headers = {"x-amz-access-token": token, "Accept": "application/json"}
    base = "https://sellingpartnerapi-eu.amazon.com/fba/outbound/2020-07-01/fulfillmentOrders/"
    clean_id = str(order_id).replace("#", "").strip()

    def _parse(j):
        payload = j.get("payload", {}) or {}
        shipments = (
            payload.get("fulfillmentShipments")
            or payload.get("shipments")
            or payload.get("fulfillmentOrder", {}).get("fulfillmentShipments")
            or []
        )
        for s in shipments:
            pkgs = (
                s.get("fulfillmentShipmentPackage")
                or s.get("packages")
                or s.get("shipmentPackages")
                or []
            )
            if not pkgs:
                tn = s.get("trackingNumber") or s.get("trackingId")
                cc = s.get("carrierCode") or s.get("carrier", "")
                if tn:
                    return tn, cc
            for p in pkgs:
                tn = p.get("trackingNumber") or p.get("trackingId") or p.get("awb")
                cc = p.get("carrierCode") or p.get("carrierName") or p.get("carrier", "")
                if tn:
                    return tn, cc
        return None, None

    for attempt_id in [clean_id, f"#{clean_id}"]:
        try:
            url = base + urllib.parse.quote(attempt_id, safe="")
            r = requests.get(url, headers=headers, timeout=30, verify=False)
            if r.status_code == 200:
                tn, cc = _parse(r.json())
                if tn:
                    return True, {"number": tn, "company": cc or "", "url": ""}
            elif r.status_code not in [400, 404]:
                break  # non-retryable error
        except Exception as e:
            return False, str(e)

    return False, "No tracking info available yet"


# ─────────────────────────────────────────────
# DELHIVERY TRACKING LOOKUP
# ─────────────────────────────────────────────
def get_delhivery_tracking(api_key, order_id):
    """Look up an order on Delhivery by reference number (order ID).
    Tries ref_ids, then #ref_ids as fallback.
    Returns (found: bool, awb: str, status: str, error: str)
    """
    base = "https://track.delhivery.com/api/v1/packages/json/"
    headers = {"Authorization": f"Token {api_key}"}
    clean_id = str(order_id).replace("#", "").strip()

    for ref in [clean_id, f"#{clean_id}"]:
        try:
            r = requests.get(base, headers=headers, params={"ref_ids": ref}, timeout=15)
            data = r.json()
            shipments = data.get("ShipmentData", [])
            if shipments:
                s = shipments[0].get("Shipment", {})
                awb = s.get("AWB", "")
                status = s.get("Status", {}).get("Status", "")
                if awb:
                    return True, awb, status, ""
        except Exception as e:
            return False, "", "", str(e)

    return False, "", "", "Not found on Delhivery"


# ─────────────────────────────────────────────
# DELHIVERY ORDER CREATION
# ─────────────────────────────────────────────
def create_delhivery_order(api_key, order_data, pickup_location="emaar"):
    """Submit a shipment to Delhivery.
    Returns (success: bool, response_data: dict, error_msg: str)
    """
    is_cod = "cod" in str(order_data.get("is_cod", "")).lower()
    shipment = {
        "name": order_data.get("customer", ""),
        "add": f"{order_data.get('addr_line1', '')} {order_data.get('addr_line2', '')}".strip(),
        "pin": order_data.get("pincode", ""),
        "city": order_data.get("city", ""),
        "state": order_data.get("state_code", ""),
        "country": "India",
        "phone": order_data.get("phone", ""),
        "order": order_data.get("order_id", ""),
        "payment_mode": "COD" if is_cod else "Prepaid",
        "cod_amount": str(order_data.get("amount", "0")) if is_cod else "0",
        "products_desc": order_data.get("title", "Product")[:100],
        "total_amount": str(order_data.get("amount", "0")),
        "quantity": order_data.get("total_qty", 1),
        "waybill": "",
        "shipment_width": "",
        "shipment_height": "",
        "weight": "",
    }
    payload = {
        "shipments": [shipment],
        "pickup_location": {"name": pickup_location},
    }
    try:
        r = requests.post(
            "https://track.delhivery.com/api/cmu/create.json",
            headers={"Authorization": f"Token {api_key}", "Content-Type": "application/json"},
            data={"data": json.dumps(payload)},
            timeout=30,
        )
        resp = r.json() if r.status_code in [200, 201] else {}
        if r.status_code in [200, 201]:
            pkgs = resp.get("packages", [])
            if pkgs and pkgs[0].get("waybill"):
                return True, resp, ""
            remarks = pkgs[0].get("remarks", ["Unknown"]) if pkgs else ["Unknown"]
            return False, resp, str(remarks)
        return False, {}, f"HTTP {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, {}, str(e)
