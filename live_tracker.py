import os
import requests
import json
import time
from datetime import datetime, timedelta

from utils import (
    batch_update_sheet_ranges,
    format_sheet_cell_value,
    get_access_token,
    get_delhivery_tracking,
    init_sheets_service,
    read_secret,
    SHEET_ID,
)

from w import fetch_mcf_data

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
    # Excel/Sheets corrupts long numeric AWBs as 1.32E+12
    if "e+" in low or "e-" in low:
        return False
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

    rto_val = ""
    if final_v == "RTO":
        rto_val = last_update or "RTO Intransit"
        for event in scan_details:
            ev_combined = f"{event.get('status', '')} {event.get('status_remark', '')}".upper()
            if ("RTO" in ev_combined or "RETURN" in ev_combined) and "DELIVERED" in ev_combined:
                rto_val = "Delivered"
                break

    return {
        "status": final_v,
        "eta": format_dt(eta) if eta else "",
        "pickup": format_dt(pickup_date) if pickup_date else "",
        "delivery": format_dt(delivery_date) if delivery_date else "",
        "last_update": last_update,
        "rto": rto_val,
    }


ITHINK_STORE_DETAILS_URL = (
    "https://my.ithinklogistics.com/api_v3/store/get-order-details.json"
)
ITHINK_STORE_LIST_URL = (
    "https://my.ithinklogistics.com/api_v3/store/get-order-list.json"
)


def _ithink_platform_id(secrets=None):
    if secrets:
        pid = str(secrets.get("ITHINK_PLATFORM_ID", "") or "").strip()
        if pid:
            return pid
    return "2"


def _shopify_stores_from_secrets(secrets):
    """Return configured Shopify shops (default + monozo) for iThink store lookup."""
    stores = []
    shop_url = (secrets or {}).get("shop_url", "")
    token = (secrets or {}).get("shop_assesstoken", "")
    if shop_url and token:
        stores.append(
            {
                "shop_url": shop_url.rstrip("/"),
                "headers": {
                    "Content-Type": "application/json",
                    "X-Shopify-Access-Token": token,
                },
            }
        )
    mono_url = (secrets or {}).get("monozo_url", "")
    mono_token = (secrets or {}).get("monozo_shopi_assesstoken", "")
    if mono_url and mono_token and mono_url.rstrip("/") not in {
        s["shop_url"] for s in stores
    }:
        stores.append(
            {
                "shop_url": mono_url.rstrip("/"),
                "headers": {
                    "Content-Type": "application/json",
                    "X-Shopify-Access-Token": mono_token,
                },
            }
        )
    return stores


def _shopify_order_id_by_name(order_no, shop_url, headers):
    """Resolve Shopify internal order ID from display order number (e.g. 5908)."""
    clean = str(order_no).replace("#", "").strip()
    if not clean or not shop_url:
        return ""
    search_url = f"{shop_url.rstrip('/')}/admin/api/2024-01/orders.json"
    for search_name in (f"#{clean}", clean):
        try:
            resp = requests.get(
                search_url,
                headers=headers,
                params={"name": search_name, "status": "any"},
                timeout=20,
            )
            if resp.status_code != 200:
                continue
            orders = resp.json().get("orders", [])
            for order in orders:
                name = str(order.get("name", "")).replace("#", "").strip()
                if name == clean:
                    return str(order.get("id", "")).strip()
        except Exception:
            continue
    return ""


def _ithink_store_details_by_ids(
    shopify_ids, ithink_token, ithink_secret, platform_id="2"
):
    """Fetch AWB from iThink store/get-order-details using Shopify internal IDs."""
    ids = [str(i).strip() for i in shopify_ids if str(i).strip()]
    if not ids or not ithink_token or not ithink_secret:
        return None, ""
    payload = {
        "data": {
            "order_no_list": ",".join(ids),
            "platform_id": str(platform_id),
            "access_token": ithink_token,
            "secret_key": ithink_secret,
        }
    }
    try:
        r = requests.post(
            ITHINK_STORE_DETAILS_URL,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=25,
        )
        if r.status_code != 200:
            return None, ""
        j = r.json()
        if str(j.get("status", "")).strip().lower() != "success":
            return None, ""
        data = j.get("data", {}) or {}
        if not isinstance(data, dict):
            return None, ""
        for detail in data.values():
            if not isinstance(detail, dict):
                continue
            awb = str(detail.get("awb_no") or detail.get("awb_number") or "").strip()
            if awb and len(awb) >= 6:
                carrier = str(
                    detail.get("logistic") or detail.get("courier") or "iThink Logistics"
                ).strip()
                return awb, carrier
    except Exception:
        pass
    return None, ""


def _ithink_scan_store_orders(
    order_no, ithink_token, ithink_secret, days_back=90, platform_id="2"
):
    """Fallback: scan iThink store order list and match order_number field."""
    clean = str(order_no).replace("#", "").strip()
    if not clean:
        return None, ""
    end = datetime.now().date()
    start = end - timedelta(days=days_back)
    list_payload = {
        "data": {
            "platform_id": str(platform_id),
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "access_token": ithink_token,
            "secret_key": ithink_secret,
        }
    }
    try:
        r = requests.post(
            ITHINK_STORE_LIST_URL,
            headers={"Content-Type": "application/json"},
            json=list_payload,
            timeout=30,
        )
        if r.status_code != 200:
            return None, ""
        j = r.json()
        if str(j.get("status", "")).strip().lower() != "success":
            return None, ""
        ids = [str(x).strip() for x in (j.get("data") or []) if str(x).strip()]
        batch_size = 25
        for i in range(0, len(ids), batch_size):
            chunk = ids[i : i + batch_size]
            detail_payload = {
                "data": {
                    "order_no_list": ",".join(chunk),
                    "platform_id": str(platform_id),
                    "access_token": ithink_token,
                    "secret_key": ithink_secret,
                }
            }
            dr = requests.post(
                ITHINK_STORE_DETAILS_URL,
                headers={"Content-Type": "application/json"},
                json=detail_payload,
                timeout=30,
            )
            if dr.status_code != 200:
                continue
            dj = dr.json()
            if str(dj.get("status", "")).strip().lower() != "success":
                continue
            for detail in (dj.get("data") or {}).values():
                if not isinstance(detail, dict):
                    continue
                on = str(
                    detail.get("order_number")
                    or detail.get("order")
                    or detail.get("order_no")
                    or ""
                ).replace("#", "").strip()
                if on != clean:
                    continue
                awb = str(detail.get("awb_no") or detail.get("awb_number") or "").strip()
                if awb and len(awb) >= 6:
                    carrier = str(
                        detail.get("logistic")
                        or detail.get("courier")
                        or "iThink Logistics"
                    ).strip()
                    return awb, carrier
    except Exception:
        pass
    return None, ""


def get_ithink_awb_by_order_no(
    order_id, ithink_token, ithink_secret, days_back=90, secrets=None
):
    """Look up iThink AWB by Shopify display order number (e.g. 5908).

    iThink store/get-order-details expects Shopify internal order IDs, not display
    numbers. We resolve the ID via Shopify name search, then query iThink store API.
    """
    if not ithink_token or not ithink_secret:
        return None, ""
    oid = str(order_id).replace("#", "").strip()
    if not oid:
        return None, ""

    if secrets is None:
        try:
            secrets = read_secret()
        except Exception:
            secrets = {}

    platform_id = _ithink_platform_id(secrets)

    for store in _shopify_stores_from_secrets(secrets):
        shopify_id = _shopify_order_id_by_name(
            oid, store["shop_url"], store["headers"]
        )
        if not shopify_id:
            continue
        awb, carrier = _ithink_store_details_by_ids(
            [shopify_id], ithink_token, ithink_secret, platform_id
        )
        if awb:
            if not carrier or carrier.lower() == "ithink logistics":
                shipment = get_ithink_shipment(awb, ithink_token, ithink_secret)
                if shipment and shipment.get("logistic"):
                    carrier = str(shipment.get("logistic")).strip()
            return awb, carrier or "iThink Logistics"

    return _ithink_scan_store_orders(
        oid, ithink_token, ithink_secret, days_back=days_back, platform_id=platform_id
    )


def lookup_awb_by_order_id(order_id, secrets=None, mcf_token=None, source=""):
    """Find AWB by order ID.

    Lookup order depends on source:
    - MCF source: MCF first → if Unfulfillable, fall back to Delhivery → iThink
    - Others: iThink → Delhivery → MCF

    Returns (awb, carrier, source_label, detail_status).
    """
    secrets = secrets or read_secret()
    oid = str(order_id).replace("#", "").strip()
    if not oid:
        return "", "", "", ""

    ithink_token = secrets.get("Ithink_access_token", "")
    ithink_secret = secrets.get("Ithink_secret_key", "")
    del_keys = [k for k in [
        secrets.get("DELHIVERY_API_KEY", ""),
        secrets.get("DELHIVERY_API_KEY2", ""),
    ] if k]

    if mcf_token is None:
        mcf_token, _ = get_access_token(secrets)

    is_mcf = "MCF" in str(source).upper()

    if is_mcf:
        # MCF source: try MCF first (fast, direct)
        mcf_status = ""
        if mcf_token:
            tn, cc, mcf_status, _raw = fetch_mcf_data(oid, mcf_token)
            if tn:
                return tn, cc or "Amazon", "MCF", mcf_status or "Found on MCF"

        # MCF returned Unfulfillable (or no tracking) — try Delhivery then iThink
        if del_keys:
            d_found, d_awb, d_status, _err = get_delhivery_tracking(del_keys, oid)
            if d_found and d_awb:
                return d_awb, "Delhivery", "Delhivery", d_status or "Found on Delhivery"

        awb, carrier = get_ithink_awb_by_order_no(
            oid, ithink_token, ithink_secret, secrets=secrets
        )
        if awb:
            return awb, carrier or "iThink Logistics", "iThink", "Found on iThink"

        return "", "", "", mcf_status or "Not found"

    # Non-MCF source: iThink → Delhivery → MCF
    awb, carrier = get_ithink_awb_by_order_no(
        oid, ithink_token, ithink_secret, secrets=secrets
    )
    if awb:
        return awb, carrier or "iThink Logistics", "iThink", "Found on iThink"

    if del_keys:
        d_found, d_awb, d_status, _err = get_delhivery_tracking(del_keys, oid)
        if d_found and d_awb:
            return d_awb, "Delhivery", "Delhivery", d_status or "Found on Delhivery"

    mcf_status = ""
    if mcf_token:
        tn, cc, mcf_status, _raw = fetch_mcf_data(oid, mcf_token)
        if tn:
            return tn, cc or "Amazon", "MCF", mcf_status or "Found on MCF"

    return "", "", "", mcf_status or "Not found"


def _apply_rto_return_logic(status, rto_value, existing_rto, existing_status, last_update_value):
    """Normalize RTO column AB when return-to-seller completes."""
    ex_rto = (existing_rto or "").strip().lower()
    ex_st = (existing_status or "").strip().lower()
    rto_active = ex_st == "rto" or ex_rto not in ("", "delivered")
    if status == "RTO":
        return last_update_value or rto_value or "RTO Intransit"
    if rto_active and status == "Delivered":
        return "Delivered"
    return rto_value


def _parse_swiship_response(res, existing_rto="", existing_status=""):
    """Parse Swiship/MCF tracking JSON into standard tracking dict."""
    transit_state = res.get("transitState", "")
    tracking_events = res.get("trackingEvents", []) or []
    latest_event = tracking_events[0].get("eventDescription", "") if tracking_events else ""
    latest_event_date = tracking_events[0].get("eventDate", "") if tracking_events else ""

    status = normalize_status(transit_state, latest_event)
    last_update_value = latest_event
    if latest_event_date:
        last_update_value = f"{last_update_value} | {format_dt(latest_event_date)}".strip(" |")

    rto_value = ""
    returning_events = [
        ev for ev in tracking_events
        if "return" in (ev.get("eventDescription") or "").lower()
        or "returning" in (ev.get("eventDescription") or "").lower()
    ]
    has_return_history = bool(returning_events) or "return" in transit_state.lower()
    if has_return_history:
        status = "RTO"
        rto_value = (
            (returning_events[0].get("eventDescription", "") if returning_events else "")
            or last_update_value
            or "RTO Intransit"
        )
        return_delivered = any(
            "deliver" in (ev.get("eventDescription") or "").lower()
            and (
                "return" in (ev.get("eventDescription") or "").lower()
                or "returning" in (ev.get("eventDescription") or "").lower()
                or "seller" in (ev.get("eventDescription") or "").lower()
            )
            for ev in tracking_events
        )
        if not return_delivered and "deliver" in transit_state.lower():
            return_delivered = True
        if return_delivered:
            rto_value = "Delivered"
    elif status == "RTO":
        rto_value = last_update_value or "RTO Intransit"
    else:
        rto_value = _apply_rto_return_logic(
            status, rto_value, existing_rto, existing_status, last_update_value
        )

    delivery_value = ""
    delivered_event = next(
        (ev for ev in tracking_events if "deliver" in (ev.get("eventDescription") or "").lower()),
        None,
    )
    if delivered_event and status != "RTO":
        delivery_value = format_dt(delivered_event.get("eventDate", ""))

    return {
        "status": status,
        "eta": format_dt(res.get("estimatedArrivalDate", "")),
        "pickup": format_dt(tracking_events[-1].get("eventDate", "")) if tracking_events else "",
        "delivery": delivery_value,
        "last_update": last_update_value,
        "rto": rto_value,
        "url": "",
        "carrier": "Amazon Transportation Services",
    }


def track_awb_live(
    tracking_no,
    secrets=None,
    carrier_hint="",
    source_hint="",
    existing_rto="",
    existing_status="",
):
    """Live-track one AWB across Delhivery → iThink → Swiship → MCF.

    Returns dict with keys: found, status, carrier, eta, pickup, delivery,
    last_update, rto, url.
    """
    tracking_no = str(tracking_no).strip()
    empty = {
        "found": False,
        "status": existing_status or "Intransit",
        "carrier": carrier_hint or "",
        "eta": "",
        "pickup": "",
        "delivery": "",
        "last_update": "",
        "rto": "",
        "url": "",
    }
    if not tracking_no:
        return empty

    secrets = secrets or read_secret()
    delhivery_keys = [
        secrets.get("DELHIVERY_API_KEY", ""),
        secrets.get("DELHIVERY_API_KEY2", ""),
    ]
    delhivery_keys = [k for k in delhivery_keys if k]
    ithink_token = secrets.get("Ithink_access_token", "")
    ithink_secret = secrets.get("Ithink_secret_key", "")

    src = (source_hint or "").lower()
    car = (carrier_hint or "").lower()
    prefer_delhivery = "delhi" in src or "delhi" in car
    prefer_ithink = "ithink" in src or "ithink" in car or tracking_no.upper().startswith(
        ("ILS", "ILSC", "ILSP", "I79")
    )
    prefer_swiship = (
        "mcf" in src
        or "amazon" in car
        or "swiship" in car
        or (tracking_no.isdigit() and len(tracking_no) == 12)
    )

    def try_delhivery():
        for del_key in delhivery_keys:
            try:
                resp = requests.get(
                    DELHIVERY_URL,
                    headers={"Authorization": f"Token {del_key}"},
                    params={"waybill": tracking_no},
                    timeout=15,
                )
                if resp.status_code != 200:
                    resp = requests.get(
                        DELHIVERY_URL,
                        params={"waybill": tracking_no, "token": del_key},
                        timeout=15,
                    )
                if resp.status_code != 200:
                    continue
                shipment_data = resp.json().get("ShipmentData", [])
                shipment = shipment_data[0].get("Shipment", {}) if shipment_data else {}
                if not shipment:
                    continue

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

                last_update_value = f"{raw_state} | {raw_event} | {location}".strip(" |")
                delivery_value = ""
                if status == "Delivered":
                    delivery_value = format_dt(shipment.get("DeliveryDate", "") or raw_date)
                    if not delivery_value:
                        for scan in (shipment.get("Scans", []) or []):
                            sd = scan.get("ScanDetail") or {}
                            if "DELIVERED" in (sd.get("Scan") or "").upper():
                                delivery_value = format_dt(sd.get("ScanDateTime", ""))

                rto_value = ""
                if status == "RTO":
                    rto_value = last_update_value or "RTO Intransit"
                    for scan in (shipment.get("Scans", []) or []):
                        sd = scan.get("ScanDetail") or {}
                        scan_text = f"{sd.get('Scan', '')} {sd.get('Instructions', '')}".upper()
                        if ("RTO" in scan_text or "RETURN" in scan_text) and "DELIVERED" in scan_text:
                            rto_value = "Delivered"
                            break
                else:
                    rto_value = _apply_rto_return_logic(
                        status, rto_value, existing_rto, existing_status, last_update_value
                    )

                return {
                    "found": True,
                    "status": status,
                    "carrier": "Delhivery",
                    "eta": format_dt(shipment.get("ExpectedDeliveryDate", "") or shipment.get("EDD", "")),
                    "pickup": format_dt(shipment.get("PickUpDate", "") or shipment.get("PickupDate", "")),
                    "delivery": delivery_value,
                    "last_update": last_update_value,
                    "rto": rto_value,
                    "url": f"https://www.delhivery.com/track/package/{tracking_no}",
                }
            except Exception:
                continue
        return None

    def try_ithink():
        ithink_order = get_ithink_shipment(tracking_no, ithink_token, ithink_secret)
        if not ithink_order:
            return None
        pinfo = parse_ithink(ithink_order)
        if not pinfo:
            return None
        rto_value = pinfo.get("rto", "")
        if (existing_status or "").lower() == "rto" and pinfo["status"] == "Delivered":
            rto_value = "Delivered"
        return {
            "found": True,
            "status": pinfo["status"],
            "carrier": "iThink Logistics",
            "eta": pinfo["eta"],
            "pickup": pinfo["pickup"],
            "delivery": pinfo["delivery"],
            "last_update": pinfo["last_update"],
            "rto": rto_value,
            "url": f"https://ithinklogistics.com/track/{tracking_no}",
        }

    def try_swiship():
        try:
            payload = {"trackingNumber": tracking_no, "shipMethod": "ATS_STANDARD"}
            resp = requests.post(
                SWISHIP_URL, headers=SWISHIP_HEADERS, cookies=COOKIES, json=payload, timeout=10
            )
            if resp.status_code != 200:
                return None
            parsed = _parse_swiship_response(resp.json(), existing_rto, existing_status)
            parsed["found"] = True
            parsed["url"] = f"https://www.swiship.co.uk/track?id={tracking_no}"
            return parsed
        except Exception:
            return None

    def try_mcf():
        from utils import get_access_token
        from w import fetch_mcf_data
        token, _ = get_access_token(secrets)
        if not token:
            return None
        tn, cc, mcf_status, _raw = fetch_mcf_data(tracking_no, token)
        if not mcf_status or mcf_status == "NotFound":
            return None
        return {
            "found": True,
            "status": mcf_status if mcf_status in {"Delivered", "RTO", "Intransit", "Lost", "Undelivered"} else "Intransit",
            "carrier": cc or "MCF",
            "eta": "",
            "pickup": "",
            "delivery": "",
            "last_update": "",
            "rto": "",
            "url": f"https://www.swiship.co.uk/track?id={tracking_no}",
        }

    order = []
    if prefer_delhivery:
        order = [try_delhivery, try_ithink, try_swiship, try_mcf]
    elif prefer_ithink:
        order = [try_ithink, try_delhivery, try_swiship, try_mcf]
    elif prefer_swiship:
        order = [try_swiship, try_delhivery, try_ithink, try_mcf]
    else:
        order = [try_delhivery, try_ithink, try_swiship, try_mcf]

    for fn in order:
        result = fn()
        if result and result.get("found"):
            if result["status"] not in {"Delivered", "RTO", "Intransit", "Lost", "Undelivered"}:
                result["status"] = "Intransit"
            return result

    return empty


def run_live_tracking_update(
    progress_callback=None, start_index=0, max_count=None, service=None
):
    """Track all eligible sheet rows (column T AWB). Flushes in safe chunks for 1000+ rows.

    Returns summary list. Use start_index/max_count for batch/resume.
    """
    secrets = read_secret()

    try:
        service = service or init_sheets_service()
    except Exception as e:
        return [{"order_id": "Error", "status": f"Sheets service init failed: {str(e)}", "carrier": "", "desc": ""}]

    range_name = "Sheet1!A:AF"
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=range_name,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
        rows = result.get("values", [])
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
                return format_sheet_cell_value(row[idx])
            return ""

        source = safe_get(source_idx)
        tracking_no = safe_get(tracking_no_idx)
        carrier = safe_get(carrier_idx)
        existing_rto = safe_get(rto_idx)
        existing_status = safe_get(status_idx)
        order_id = safe_get(order_id_idx).replace("#", "")

        if not tracking_no or not looks_like_tracking_number(tracking_no):
            continue
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
            "source": source,
            "existing_rto": existing_rto,
            "existing_status": existing_status,
        })

    total_all = len(orders_to_check)
    print(f"\n[INFO] Found {total_all} orders to check tracking for...")
    if total_all == 0:
        return []

    slice_start = max(0, int(start_index or 0))
    work = orders_to_check[slice_start:]
    if max_count is not None:
        work = work[: int(max_count)]

    def flush_pending():
        nonlocal pending_updates, service
        if not pending_updates:
            return
        try:
            batch_update_sheet_ranges(service, SHEET_ID, pending_updates)
        except Exception as e:
            print(f"Batch update error (retrying with fresh service): {e}")
            service = init_sheets_service()
            batch_update_sheet_ranges(service, SHEET_ID, pending_updates)
        pending_updates = []

    for idx, item in enumerate(work):
        global_idx = slice_start + idx
        order_id = item["order_id"]
        tracking_no = item["tracking_no"]
        carrier = item["carrier"]
        row_num = item["row_index"] + 1

        if progress_callback:
            progress_callback(global_idx, total_all, tracking_no)

        track = track_awb_live(
            tracking_no,
            secrets=secrets,
            carrier_hint=carrier,
            source_hint=item.get("source", ""),
            existing_rto=item["existing_rto"],
            existing_status=item["existing_status"],
        )

        if track.get("found"):
            status = track["status"]
            eta_value = track.get("eta", "")
            pickup_value = track.get("pickup", "")
            delivery_value = track.get("delivery", "")
            last_update_value = track.get("last_update", "")
            rto_value = track.get("rto", "")
            tracking_url = track.get("url", "")
        else:
            status = item["existing_status"] or "Intransit"
            eta_value = pickup_value = delivery_value = ""
            last_update_value = rto_value = tracking_url = ""

        if tracking_url:
            pending_updates.append({
                "range": f"{col_num_to_a1(col_tracking_url)}{row_num}",
                "values": [[tracking_url]],
            })
        pending_updates.append({
            "range": f"{col_num_to_a1(col_status)}{row_num}",
            "values": [[status]],
        })
        if eta_idx != -1 and eta_value:
            pending_updates.append({
                "range": f"{col_num_to_a1(eta_idx + 1)}{row_num}",
                "values": [[eta_value]],
            })
        if pickup_idx != -1 and pickup_value:
            pending_updates.append({
                "range": f"{col_num_to_a1(pickup_idx + 1)}{row_num}",
                "values": [[pickup_value]],
            })
        if delivery_idx != -1 and delivery_value:
            pending_updates.append({
                "range": f"{col_num_to_a1(delivery_idx + 1)}{row_num}",
                "values": [[delivery_value]],
            })
        if last_status_idx != -1 and last_update_value:
            pending_updates.append({
                "range": f"{col_num_to_a1(last_status_idx + 1)}{row_num}",
                "values": [[last_update_value]],
            })
        if rto_idx != -1 and rto_value:
            pending_updates.append({
                "range": f"{col_num_to_a1(rto_idx + 1)}{row_num}",
                "values": [[rto_value]],
            })

        summary_results.append({
            "Order ID": order_id,
            "Tracking ID": tracking_no,
            "Carrier": carrier,
            "Status": status,
            "ETA": eta_value,
            "Last Scan": last_update_value,
            "RTO": rto_value,
        })

        print(
            f"  [{global_idx + 1}/{total_all}] Row {row_num} | T: {tracking_no} | "
            f"Carrier: {carrier} | Status: {status}"
        )

        if len(pending_updates) >= 120:
            flush_pending()

        time.sleep(0.15)

    flush_pending()
    print("\n[INFO] Live Tracking Update Finished.")
    return summary_results

if __name__ == "__main__":
    print("Starting Live Tracker (Terminal Mode)...")
    res = run_live_tracking_update()
    print(f"\n[DONE] Checked and updated tracking for {len(res)} items.")
