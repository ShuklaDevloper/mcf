"""
w.py — Pure MCF Tracking Module
Fetch tracking numbers from Amazon SP-API for MCF orders.
No CLI. Import and use functions directly.
"""
import re
import time
import urllib.parse
import requests
from typing import Dict, List, Optional, Tuple


def parse_amazon_response(json_data: Dict) -> Tuple[Optional[str], Optional[str]]:
    """Extract (tracking_number, carrier_code) from Amazon SP-API response."""
    payload = json_data.get("payload", {}) or {}
    shipments = (
        payload.get("fulfillmentShipments")
        or payload.get("shipments")
        or payload.get("fulfillmentOrder", {}).get("fulfillmentShipments")
        or []
    )
    if not shipments:
        for fo in payload.get("fulfillmentOrders", []):
            shipments = fo.get("fulfillmentShipments") or fo.get("shipments") or []
            if shipments:
                break

    for s in shipments:
        pkgs = (
            s.get("fulfillmentShipmentPackage")
            or s.get("packages")
            or s.get("shipmentPackages")
            or []
        )
        if not pkgs:
            tn = s.get("trackingNumber") or s.get("tracking_id") or s.get("trackingId")
            cc = s.get("carrierCode") or s.get("carrierName") or s.get("carrier")
            if tn:
                return tn, cc
        else:
            for p in pkgs:
                tn = (
                    p.get("trackingNumber")
                    or p.get("tracking_id")
                    or p.get("trackingId")
                    or p.get("awb")
                    or p.get("awbNumber")
                )
                cc = p.get("carrierCode") or p.get("carrierName") or p.get("carrier")
                if tn:
                    return tn, cc
    return None, None


def get_mcf_order_status(json_data: Dict) -> str:
    """Extract MCF fulfillment order status: Planning / Received / Processing / Shipped / Complete / Cancelled / etc."""
    payload = json_data.get("payload", {}) or {}
    fo = payload.get("fulfillmentOrder", {}) or {}
    return fo.get("fulfillmentOrderStatus", "Unknown")


def fetch_mcf_data(
    order_id_raw: str,
    access_token: str,
    timeout: int = 30,
) -> Tuple[Optional[str], Optional[str], str, Dict]:
    """Fetch tracking + MCF status for a single order.
    Tries clean_id first, then #clean_id if 400/404.
    Returns: (tracking_number or None, carrier or None, mcf_status str, raw_response_dict)
    """
    headers = {"x-amz-access-token": access_token, "Accept": "application/json"}
    base = "https://sellingpartnerapi-eu.amazon.com/fba/outbound/2020-07-01/fulfillmentOrders/"
    clean_id = str(order_id_raw).replace("#", "").strip()
    last_json = {}

    for attempt_id in [clean_id, f"#{clean_id}"]:
        url = base + urllib.parse.quote(attempt_id, safe="")
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
        except Exception as e:
            return None, None, "NetworkError", {"error": str(e)}

        try:
            j = r.json()
        except Exception:
            j = {"raw_text": r.text[:500]}

        last_json = j

        if r.status_code == 200:
            tn, cc = parse_amazon_response(j)
            mcf_status = get_mcf_order_status(j)
            return tn, cc, mcf_status, j

        if r.status_code not in [400, 404]:
            # Non-retryable error
            return None, None, f"HTTP_{r.status_code}", j

    # Both attempts failed (404/400)
    return None, None, "NotFound", last_json


def bulk_fetch_tracking(
    credentials: Dict[str, str],
    order_ids: List[str],
    delay: float = 0.3,
    max_retries: int = 2,
) -> List[Dict]:
    """Bulk fetch tracking for a list of order IDs.
    Returns list of dicts: {order_id, tracking_number, carrier, mcf_status, status_note}
    """
    from utils import get_access_token

    token, err = get_access_token(credentials)
    if not token:
        return [
            {"order_id": oid, "tracking_number": None, "carrier": None,
             "mcf_status": "AuthError", "status_note": str(err)}
            for oid in order_ids
        ]

    results = []
    for oid in order_ids:
        attempt = 0
        tn, cc, mcf_status, note = None, None, "Unknown", "not_found"

        while attempt <= max_retries:
            attempt += 1
            tn, cc, mcf_status, raw = fetch_mcf_data(oid, token)
            if tn:
                note = "found"
                break
            # Refresh token on 401
            if isinstance(raw, dict) and ("401" in str(raw.get("error", ""))
                    or raw.get("statusCode") in [401, 403]):
                token, _ = get_access_token(credentials)
                time.sleep(0.5)
                continue
            time.sleep(delay * attempt)

        results.append({
            "order_id": oid,
            "tracking_number": tn,
            "carrier": cc,
            "mcf_status": mcf_status,
            "status_note": note,
        })
        time.sleep(delay)

    return results

if __name__ == "__main__":
    import sys
    from utils import read_secret, get_shopify_config, get_shopify_order, fulfill_order
    from db import update_order_tracking
    
    secrets = read_secret()
    order_id = sys.argv[1] if len(sys.argv) > 1 else input("Enter Order ID manually to track & update: ").strip()
    if order_id:
        print(f"Fetching tracking for: {order_id}...")
        res = bulk_fetch_tracking(secrets, [order_id])
        print("Tracking Result:", res)
        
        info = res[0]
        if info.get("tracking_number"):
            tn = info["tracking_number"]
            cc = info["carrier"] or "Amazon"
            print(f"> Found Tracking Number: {tn} via {cc}")
            
            print("> Updating Database...")
            update_order_tracking(order_id, cc, tn, "")
            
            print("> Updating Shopify...")
            shopify_cfg = get_shopify_config(secrets)
            if shopify_cfg.get("shop_url"):
                s_order = get_shopify_order(order_id, shopify_cfg["headers"], shopify_cfg["shop_url"])
                if s_order:
                    t_info = {"number": tn, "company": cc, "url": ""}
                    fulfilled = fulfill_order(s_order, shopify_cfg["headers"], shopify_cfg["shop_url"], tracking_info=t_info)
                    print(f"  Shopify fulfillment status: {'Updated' if fulfilled else 'No action needed/Failed'}")
                else:
                    print("  Order not found on Shopify.")
            else:
                print("  Shopify not configured.")
        else:
            print("> No tracking found yet. Status:", info.get("mcf_status"))
