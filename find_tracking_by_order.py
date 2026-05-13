import os
import requests
import pandas as pd
import time

from utils import read_secret, get_access_token, get_delhivery_tracking, get_shopify_config
from w import fetch_mcf_data

def find_tracking_for_orders(order_ids):
    secrets = read_secret()
    
    delhivery_api_key = secrets.get("DELHIVERY_API_KEY", "")
    token, _ = get_access_token(secrets)
    shopify_cfg = get_shopify_config(secrets)
    shop_url = shopify_cfg["shop_url"]
    headers = shopify_cfg["headers"]

    results = []

    for idx, oid in enumerate(order_ids):
        oid_clean = str(oid).replace("#", "").strip()
        print(f"\nProcessing {idx+1}/{len(order_ids)}: Order ID {oid_clean}")
        
        found = False
        tracking_no = ""
        carrier = ""
        status = ""
        
        # 1. Check MCF SP-API
        if token and not found:
            print("  -> Checking MCF...")
            tn, cc, mcf_status, raw = fetch_mcf_data(oid_clean, token)
            if tn:
                tracking_no = tn
                carrier = cc or "MCF"
                status = mcf_status
                found = True
                print(f"    [+] Found in MCF: {tracking_no} ({carrier})")

        # 2. Check Delhivery API
        if delhivery_api_key and not found:
            print("  -> Checking Delhivery...")
            d_found, d_awb, d_status, err = get_delhivery_tracking(delhivery_api_key, oid_clean)
            if d_found and d_awb:
                tracking_no = d_awb
                carrier = "Delhivery"
                status = d_status
                found = True
                print(f"    [+] Found in Delhivery: {tracking_no}")

        # 3. Check Shopify (covers iThink, Xpressbees, etc. pushed to Shopify)
        if not found and shop_url:
            print("  -> Checking Shopify Fulfillments (iThink/Other)...")
            try:
                search_url = f"{shop_url}/admin/api/2024-01/orders.json"
                s_order = None
                for search_name in [f"#{oid_clean}", oid_clean]:
                    resp = requests.get(search_url, headers=headers, params={"name": search_name, "status": "any"})
                    orders = resp.json().get("orders", [])
                    if orders:
                        s_order = orders[0]
                        break
                
                if s_order:
                    f_url = f"{shop_url}/admin/api/2024-01/orders/{s_order['id']}/fulfillments.json"
                    fr = requests.get(f_url, headers=headers)
                    fulfillments = fr.json().get("fulfillments", [])
                    for f in fulfillments:
                        if f.get("status") in ["success", "pending"] and f.get("tracking_number"):
                            tracking_no = f.get("tracking_number")
                            carrier = f.get("tracking_company") or "Shopify/iThink"
                            status = "Fulfilled"
                            found = True
                            print(f"    [+] Found in Shopify: {tracking_no} ({carrier})")
                            break
            except Exception as e:
                print(f"    [!] Error checking Shopify: {e}")
                
        if not found:
            status = "Not Found"
            print("    [-] Tracking not found in MCF, Delhivery, or Shopify.")
            
        results.append({
            "Order ID": oid_clean,
            "Tracking Number": tracking_no,
            "Carrier": carrier,
            "Status": status
        })
        time.sleep(0.5)

    if results:
        df = pd.DataFrame(results)
        df.to_excel("Order_Tracking_Finder.xlsx", index=False)
        print(f"\n[DONE] Saved {len(results)} results to Order_Tracking_Finder.xlsx")

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
        find_tracking_for_orders(unique_ids)
    else:
        print("No Order IDs provided.")
