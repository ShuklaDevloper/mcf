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

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbxDNOr8zmH97UcLeF52AZ5O_uscSpe7tyTxrChbCpIuFnhpVSYZ-xlqreuOOvrBuH3O/exec?secret=shopify2025&action=get_orders&status=all"
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
                    line_str = line.strip()
                    if line_str.startswith("#"):
                        continue
                    if ":" in line_str and "=" not in line_str:
                        k, v = line_str.split(":", 1)
                        secrets[k.strip()] = v.strip()
                    elif "=" in line_str:
                        k, v = line_str.split("=", 1)
                        secrets[k.strip()] = v.strip()
    except Exception as e:
        print(f"[utils] secret.txt read error: {e}")

    try:
        import streamlit as st
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        if get_script_run_ctx() is not None:
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
def _load_google_credentials(secrets=None):
    """Load Google service account creds from hide.json, secret.txt path, or Streamlit."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if secrets is None:
        secrets = read_secret()

    creds_file = (
        secrets.get("GOOGLE_CREDS_FILE")
        or secrets.get("HIDE_JSON")
        or secrets.get("GCP_CREDS_FILE")
        or "hide.json"
    )
    if not os.path.isabs(creds_file):
        creds_file = os.path.join(script_dir, creds_file)
    if os.path.exists(creds_file):
        return Credentials.from_service_account_file(creds_file, scopes=scopes)

    json_str = (secrets.get("GCP_SERVICE_ACCOUNT_JSON") or "").strip()
    if json_str.startswith("{"):
        return Credentials.from_service_account_info(json.loads(json_str), scopes=scopes)

    try:
        import streamlit as st
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        if get_script_run_ctx() is not None and "gcp_service_account" in st.secrets:
            return Credentials.from_service_account_info(
                dict(st.secrets["gcp_service_account"]), scopes=scopes
            )
    except Exception:
        pass

    raise FileNotFoundError(
        "Google Sheets credentials missing. Place hide.json in the project folder, "
        "or add GOOGLE_CREDS_FILE=path/to/key.json in secret.txt."
    )


def init_sheets_service(secrets=None):
    """Build Google Sheets API service."""
    creds = _load_google_credentials(secrets)
    return build("sheets", "v4", credentials=creds)


def fetch_orders_from_apps_script(timeout=30):
    """Load all orders from the Apps Script endpoint (no Sheets API creds needed)."""
    resp = requests.get(APPS_SCRIPT_URL, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        return []
    return data.get("orders", [])


def get_order_awb(order):
    """Return AWB/tracking number from Apps Script order payload."""
    awb = str(order.get("carrier", "")).strip()
    label = str(order.get("tracking_no", "")).strip()
    # Apps Script maps AWB -> carrier, carrier company -> tracking_no
    if awb and len(awb) >= 8 and awb.lower() not in {
        "delhivery", "amazon transportation services", "ithink logistics", "mcf",
    }:
        return awb
    if label and len(label) >= 8 and label.lower() not in {
        "delhivery", "amazon transportation services", "ithink logistics", "mcf",
    }:
        return label
    return ""


def build_order_row_map(orders):
    """Map order ID -> sheet row number from Apps Script order list."""
    row_map = {}
    for o in orders:
        oid = str(o.get("ord_serial", "")).replace("#", "").strip()
        row_num = int(o.get("row_number", 0) or 0)
        if oid and row_num:
            row_map[oid] = row_num
    return row_map


def build_tracking_row_map(orders):
    """Map tracking number -> sheet row number from Apps Script order list."""
    row_map = {}
    for o in orders:
        awb = get_order_awb(o)
        row_num = int(o.get("row_number", 0) or 0)
        if awb and row_num:
            row_map[awb] = row_num
    return row_map


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

def infer_sheet_source_q(carrier: str, tracking_no: str = "") -> str:
    """Map carrier + AWB to column Q (Source). Amazon / MCF logistics → ``MCF``; else courier name or Manual."""
    car = (carrier or "").strip()
    c = car.lower()
    tn = (tracking_no or "").strip()

    for needle in (
        "amazon",
        "mcf",
        "swiship",
        "amzl",
        "amzn",
        "fulfillment by amazon",
        "fba",
    ):
        if needle in c:
            return "MCF"

    if "delhi" in c:
        return "Delhivery"
    if "ithink" in c:
        return "iThink"
    if "bluedart" in c.replace(" ", ""):
        return "Blue Dart"
    if "ekart" in c:
        return "Ekart"
    if "xpressbees" in c.replace(" ", ""):
        return "XpressBees"
    if "shiprocket" in c.replace(" ", ""):
        return "Shiprocket"

    # Shopify often leaves carrier blank; infer from AWB pattern.
    if not car or c in ("shopify", "other", "custom"):
        up = tn.upper()
        if up.startswith(("ILS", "ILSC", "ILSP", "I79")):
            return "iThink"
        if tn.isdigit() and len(tn) == 12:
            return "MCF"
        if tn.isdigit() and 10 <= len(tn) <= 15:
            return "Delhivery"
        return "Manual"

    return car[:50]


def build_tracking_url(carrier: str, tracking_no: str) -> str:
    """Build public tracking URL from carrier + AWB (Delhivery / iThink / Swiship)."""
    tn = (tracking_no or "").strip()
    if not tn:
        return ""
    src = infer_sheet_source_q(carrier, tn)
    if src == "Delhivery":
        return f"https://www.delhivery.com/track/package/{tn}"
    if src == "iThink":
        return f"https://ithinklogistics.com/track/{tn}"
    if src == "MCF":
        return f"https://www.swiship.co.uk/track?id={tn}"
    up = tn.upper()
    if tn.isdigit() and len(tn) >= 14:
        return f"https://www.delhivery.com/track/package/{tn}"
    if up.startswith(("ILS", "ILSC", "ILSP", "I79")):
        return f"https://ithinklogistics.com/track/{tn}"
    if tn.isdigit() and len(tn) == 12:
        return f"https://www.swiship.co.uk/track?id={tn}"
    return ""


def format_sheet_cell_value(val):
    """Preserve full numeric AWBs from Sheets (avoid 1.32E+12 corruption)."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return str(val)
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        if val == int(val):
            return str(int(val))
        return format(val, "f").rstrip("0").rstrip(".")
    s = str(val).strip()
    low = s.lower()
    if re.match(r"^\d+\.?\d*e[+-]\d+$", low):
        try:
            f = float(s)
            if f == int(f):
                return str(int(f))
        except (ValueError, OverflowError):
            pass
    return s


def batch_update_sheet_ranges(service, sheet_id, data, chunk_size=60, retries=3, pause_sec=0.35):
    """Write many A1 range updates with chunking + retry (429/503 safe for 1000+ rows)."""
    import time

    if not data:
        return 0
    written = 0
    for i in range(0, len(data), chunk_size):
        chunk = data[i : i + chunk_size]
        last_err = None
        for attempt in range(retries):
            try:
                srv = service
                if srv is None:
                    srv = init_sheets_service()
                srv.spreadsheets().values().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={"valueInputOption": "RAW", "data": chunk},
                ).execute()
                written += len(chunk)
                last_err = None
                break
            except Exception as e:
                last_err = e
                if attempt < retries - 1:
                    time.sleep(min(8, 1.5 * (2**attempt)))
                else:
                    raise last_err
        if pause_sec:
            time.sleep(pause_sec)
    return written


def batch_update_tracking_rows(service, sheet_id, updates):
    """Write tracking row to fixed columns Q–AB.

    Q=Source, R=FULFILLED, S=Carrier, T=tracking no, U=url, V=Status,
    X=ETA, Y=Pickup, Z=Delivery, AA=Last Status, AB=RTO
    """
    if not updates:
        return
    max_row = max(u["row"] for u in updates)
    ensure_sheet_capacity(service, sheet_id, max_row)
    data = []
    for u in updates:
        row = u["row"]
        if u.get("source"):
            data.append({"range": f"Sheet1!Q{row}", "values": [[u["source"]]]})
        if u.get("fulfilled"):
            data.append({"range": f"Sheet1!R{row}", "values": [[u["fulfilled"]]]})
        if u.get("carrier"):
            data.append({"range": f"Sheet1!S{row}", "values": [[u["carrier"]]]})
        if u.get("tracking_no"):
            data.append({"range": f"Sheet1!T{row}", "values": [[u["tracking_no"]]]})
        if u.get("url"):
            data.append({"range": f"Sheet1!U{row}", "values": [[u["url"]]]})
        if u.get("status"):
            data.append({"range": f"Sheet1!V{row}", "values": [[u["status"]]]})
        if u.get("eta"):
            data.append({"range": f"Sheet1!X{row}", "values": [[u["eta"]]]})
        if u.get("pickup"):
            data.append({"range": f"Sheet1!Y{row}", "values": [[u["pickup"]]]})
        if u.get("delivery"):
            data.append({"range": f"Sheet1!Z{row}", "values": [[u["delivery"]]]})
        if u.get("last_status"):
            data.append({"range": f"Sheet1!AA{row}", "values": [[u["last_status"]]]})
        if u.get("rto"):
            data.append({"range": f"Sheet1!AB{row}", "values": [[u["rto"]]]})
    if data:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()


def update_sheet_remarks(service, sheet_id, updates):
    """Batch update columns Q (Source) and R (FULFILLED).
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
    """Batch update S (carrier), T (tracking), U (url), V (status).
    Also writes Q (Source) when ``tracking_no`` is set unless ``fill_source_q=False``.
    """
    if not updates:
        return
    max_row = max(u['row'] for u in updates)
    ensure_sheet_capacity(service, sheet_id, max_row)
    data = []
    for u in updates:
        tn = u.get("tracking_no", "")
        status_val = u.get("status") or u.get("remark") or ""
        url = u.get("url") or build_tracking_url(u.get("carrier", ""), tn)
        data.append({
            "range": f"Sheet1!S{u['row']}:V{u['row']}",
            "values": [[u.get("carrier", ""), tn, url, status_val]],
        })
        fill_q = u.get("fill_source_q", True)
        if fill_q and str(tn).strip():
            src = (u.get("source") or "").strip()
            if not src:
                src = infer_sheet_source_q(u.get("carrier", ""), tn)
            if src:
                data.append({
                    "range": f"Sheet1!Q{u['row']}",
                    "values": [[src]],
                })
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


def update_sheet_fulfilled_only(service, sheet_id, updates):
    """Update R (FULFILLED) and V (Status) — keeps Q Source unchanged."""
    if not updates:
        return
    max_row = max(u["row"] for u in updates)
    ensure_sheet_capacity(service, sheet_id, max_row)
    data = []
    for u in updates:
        row = u["row"]
        if u.get("fulfilled"):
            data.append({"range": f"Sheet1!R{row}", "values": [[u["fulfilled"]]]})
        if u.get("status"):
            data.append({"range": f"Sheet1!V{row}", "values": [[u["status"]]]})
    if data:
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

    # India MCF API requires codSettings for ALL orders.
    # For COD: isCodRequired=True with order amount
    # For Prepaid: isCodRequired=False with zero charge
    # All item values must be in perUnitDeclaredValue in items array.
    if is_cod:
        payload["codSettings"] = {
            "isCodRequired": True,
            "codCharge": {"currencyCode": "INR", "value": str(order_data.get("amount", "0"))},
        }
    pi = order_data.get("paymentInformationList")
    if pi:  # list non-empty
        payload["paymentInformationList"] = pi

    import json
    with open("payload_debug.json", "w") as f:
        json.dump(payload, f, indent=2)

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
    keys = api_key if isinstance(api_key, list) else [api_key]
    base = "https://track.delhivery.com/api/v1/packages/json/"
    clean_id = str(order_id).replace("#", "").strip()

    last_err = "Not found on Delhivery"
    for key in keys:
        if not key: continue
        headers = {"Authorization": f"Token {key}"}
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
                last_err = str(e)

    return False, "", "", last_err


# ─────────────────────────────────────────────
# DELHIVERY ORDER CREATION
# ─────────────────────────────────────────────
def create_delhivery_order(api_key, order_data, pickup_location="emaar"):
    """Submit a shipment to Delhivery.
    Returns (success: bool, response_data: dict, error_msg: str)
    """
    keys = api_key if isinstance(api_key, list) else [api_key]
    
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
    
    last_err = ""
    last_resp = {}
    
    for key in keys:
        if not key: continue
        try:
            r = requests.post(
                "https://track.delhivery.com/api/cmu/create.json",
                headers={"Authorization": f"Token {key}", "Content-Type": "application/json"},
                data={"data": json.dumps(payload)},
                timeout=30,
            )
            resp = r.json() if r.status_code in [200, 201] else {}
            if r.status_code in [200, 201]:
                pkgs = resp.get("packages", [])
                if pkgs and pkgs[0].get("waybill"):
                    return True, resp, ""
                remarks = pkgs[0].get("remarks", ["Unknown"]) if pkgs else ["Unknown"]
                last_err = str(remarks)
                last_resp = resp
            else:
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                last_resp = {}
        except Exception as e:
            last_err = str(e)
            last_resp = {}

    return False, last_resp, last_err
