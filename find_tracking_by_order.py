"""Find AWB by order ID (MCF → Delhivery → iThink), update Shopify + Google Sheet."""
import time

import pandas as pd
import requests

from live_tracker import lookup_awb_mcf_first, track_awb_live
from utils import (
    SHEET_ID,
    build_tracking_url,
    fulfill_order,
    format_sheet_cell_value,
    get_shopify_config,
    get_shopify_order,
    infer_sheet_source_q,
    init_sheets_service,
    read_secret,
    update_sheet_remarks,
    update_sheet_tracking,
)


def _load_order_row_map(service):
    """Map order ID → sheet row number from ord_serial / order id column."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range="Sheet1!A:L",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    rows = result.get("values", [])
    if not rows:
        return {}

    headers = [str(h).strip().lower() for h in rows[0]]
    oid_idx = -1
    for col in ("ord_serial", "order id", "ord", "order"):
        if col in headers:
            oid_idx = headers.index(col)
            break
    if oid_idx == -1:
        oid_idx = 11  # column L fallback

    row_map = {}
    for i, row in enumerate(rows[1:], start=2):
        if len(row) <= oid_idx:
            continue
        oid = format_sheet_cell_value(row[oid_idx]).replace("#", "").strip()
        if oid:
            row_map[oid] = i
    return row_map


def _find_on_shopify_fulfillments(order_id, shopify_cfg):
    shop_url = shopify_cfg.get("shop_url", "")
    headers = shopify_cfg.get("headers", {})
    if not shop_url:
        return None

    s_order = get_shopify_order(order_id, headers, shop_url)
    if not s_order:
        return None

    try:
        f_url = f"{shop_url}/admin/api/2024-01/orders/{s_order['id']}/fulfillments.json"
        fr = requests.get(f_url, headers=headers, timeout=20)
        fr.raise_for_status()
        for f in fr.json().get("fulfillments", []):
            if f.get("status") in ("success", "pending") and f.get("tracking_number"):
                return {
                    "tracking_no": f.get("tracking_number", ""),
                    "carrier": f.get("tracking_company") or "Shopify",
                    "source": "Shopify",
                    "status": "Intransit",
                }
    except Exception:
        pass
    return None


def _sheet_carrier(carrier, source):
    car = (carrier or "").strip()
    if car.isdigit() and len(car) >= 10:
        car = ""
    if car and car.lower() not in ("fulfilled", "fulfill"):
        return car
    src = (source or "").strip().lower()
    if src == "mcf":
        return "Amazon Transportation Services"
    if src == "delhivery":
        return "Delhivery"
    if src == "ithink":
        return "iThink Logistics"
    return source or "Manual"


def _sheet_source(source, carrier, tracking_no):
    src = (source or "").strip()
    if src and src.lower() != "shopify":
        mapping = {"mcf": "MCF", "delhivery": "Delhivery", "ithink": "iThink"}
        return mapping.get(src.lower(), src)
    return infer_sheet_source_q(carrier, tracking_no) or "Manual"


def _sheet_v_status(detail_status, tracking_no, live_status=""):
    if live_status:
        return live_status
    if not tracking_no:
        ds = (detail_status or "").strip()
        if ds.lower() == "unfulfillable":
            return "MCF: Unfulfillable"
        return ds or "Not Found"
    ds = (detail_status or "").strip()
    if ds in ("Complete", "Found on MCF", "Found on Delhivery", "Found on iThink"):
        return "Intransit"
    if ds in {"Delivered", "RTO", "Intransit", "Undelivered", "Lost"}:
        return ds
    return ds or "Intransit"


def push_tracking_to_shopify(order_id, tracking_no, carrier, source, shopify_cfg):
    shop_url = shopify_cfg.get("shop_url", "")
    if not shop_url or not tracking_no:
        return False, "Shopify not configured or no tracking"

    headers = shopify_cfg.get("headers", {})
    s_order = get_shopify_order(order_id, headers, shop_url)
    if not s_order:
        return False, "Order not found on Shopify"

    company = _sheet_carrier(carrier, source)
    tracking_info = {
        "number": tracking_no,
        "company": company,
        "url": build_tracking_url(company, tracking_no),
    }

    try:
        ok = fulfill_order(s_order, headers, shop_url, tracking_info=tracking_info)
        if ok:
            return True, f"Shopify updated ({company} / {tracking_no})"
        return False, "Shopify: already fulfilled / no action"
    except Exception as e:
        return False, f"Shopify error: {e}"


def find_tracking_for_orders(order_ids):
    secrets = read_secret()
    shopify_cfg = get_shopify_config(secrets)

    sheets_svc = None
    row_map = {}
    try:
        sheets_svc = init_sheets_service(secrets)
        row_map = _load_order_row_map(sheets_svc)
        print(f"[Sheet] Loaded {len(row_map)} order → row mappings")
    except Exception as e:
        print(f"[WARN] Google Sheet update disabled: {e}")

    results = []
    qr_buf = []
    st_buf = []

    for idx, oid in enumerate(order_ids):
        oid_clean = str(oid).replace("#", "").strip()
        print(f"\nProcessing {idx + 1}/{len(order_ids)}: Order ID {oid_clean}")

        tracking_no = ""
        carrier = ""
        detail_status = ""
        source = ""
        shopify_msg = ""
        sheet_msg = ""

        print("  -> MCF → Delhivery → iThink...")
        awb, car, src, detail_status = lookup_awb_mcf_first(oid_clean, secrets=secrets)

        if awb:
            tracking_no = awb
            carrier = car or src
            source = src
            print(f"    [+] Found on {src}: {tracking_no} ({carrier})")
        elif (detail_status or "").strip().lower() == "unfulfillable":
            source = "MCF"
            print("    [-] MCF Unfulfillable (not on Delhivery/iThink)")
        else:
            print("  -> Checking Shopify fulfillments...")
            shop_hit = _find_on_shopify_fulfillments(oid_clean, shopify_cfg)
            if shop_hit:
                tracking_no = shop_hit["tracking_no"]
                carrier = shop_hit["carrier"]
                source = shop_hit["source"]
                detail_status = shop_hit["status"]
                shopify_msg = "Already on Shopify"
                print(f"    [+] Found on Shopify: {tracking_no} ({carrier})")
            else:
                print(f"    [-] Tracking not found ({detail_status or 'Not Found'})")

        live_status = ""
        if tracking_no:
            live = track_awb_live(
                tracking_no,
                secrets=secrets,
                carrier_hint=_sheet_carrier(carrier, source),
                source_hint=_sheet_source(source, carrier, tracking_no),
                fixed_order=True,
            )
            if live.get("found"):
                live_status = live.get("status", "")

        if tracking_no and source.lower() != "shopify":
            print("  -> Updating Shopify...")
            s_ok, shopify_msg = push_tracking_to_shopify(
                oid_clean, tracking_no, carrier, source, shopify_cfg
            )
            print(f"    [{'+' if s_ok else '!'}] {shopify_msg}")

        row_num = row_map.get(oid_clean)
        v_status = _sheet_v_status(detail_status, tracking_no, live_status)
        sheet_carrier = _sheet_carrier(carrier, source)
        sheet_src = _sheet_source(source, sheet_carrier, tracking_no)
        track_url = build_tracking_url(sheet_carrier, tracking_no) if tracking_no else ""

        if sheets_svc and row_num:
            if tracking_no:
                qr_buf.append({
                    "row": row_num,
                    "source": sheet_src,
                    "status": "FULFILLED",
                })
                st_buf.append({
                    "row": row_num,
                    "source": sheet_src,
                    "carrier": sheet_carrier,
                    "tracking_no": tracking_no,
                    "url": track_url,
                    "remark": v_status,
                    "fill_source_q": False,
                })
                sheet_msg = f"Sheet row {row_num}"
            elif (detail_status or "").strip().lower() == "unfulfillable":
                qr_buf.append({
                    "row": row_num,
                    "source": "MCF",
                    "status": "FULFILLED",
                })
                st_buf.append({
                    "row": row_num,
                    "carrier": "",
                    "tracking_no": "",
                    "url": "",
                    "remark": "MCF: Unfulfillable",
                    "fill_source_q": False,
                })
                sheet_msg = f"Sheet row {row_num} (Unfulfillable)"
            print(f"  -> Sheet: {sheet_msg or 'row not found in sheet'}")
        elif sheets_svc:
            sheet_msg = "Not in sheet"
            print(f"  -> Sheet: order {oid_clean} not found in sheet")

        results.append({
            "Order ID": oid_clean,
            "Row": row_num or "",
            "Source": sheet_src if row_num else source,
            "Tracking Number": tracking_no,
            "Carrier": sheet_carrier if tracking_no else carrier,
            "Status": v_status,
            "Shopify": shopify_msg,
            "Sheet": sheet_msg,
            "Track URL": track_url,
        })
        time.sleep(0.4)

    if sheets_svc:
        print("\nUpdating Google Sheet (Q/R + S/T/U/V)...")
        try:
            if qr_buf:
                update_sheet_remarks(sheets_svc, SHEET_ID, qr_buf)
            if st_buf:
                update_sheet_tracking(sheets_svc, SHEET_ID, st_buf)
            print(f"  -> Updated {len(st_buf)} row(s) on sheet")
        except Exception as e:
            print(f"  [!] Sheet update error: {e}")

    if results:
        pd.DataFrame(results).to_excel("Order_Tracking_Finder.xlsx", index=False)
        found = sum(1 for r in results if r["Tracking Number"])
        shop_ok = sum(1 for r in results if str(r["Shopify"]).startswith("Shopify updated"))
        sheet_ok = sum(1 for r in results if r["Sheet"] and "row" in str(r["Sheet"]).lower())
        print(
            f"\n[DONE] Found {found}/{len(results)} | "
            f"Shopify {shop_ok} | Sheet {sheet_ok} | Saved to Order_Tracking_Finder.xlsx"
        )


if __name__ == "__main__":
    print(
        "Paste Order IDs (space or newline). Type DONE when finished.\n"
        "Lookup: MCF → Delhivery → iThink | Updates Shopify + Sheet Q–V\n"
    )
    target_ids = []
    while True:
        try:
            line = input()
            if line.strip().upper() == "DONE":
                break
            target_ids.extend(x.strip() for x in line.split() if x.strip())
        except EOFError:
            break

    if target_ids:
        find_tracking_for_orders(list(dict.fromkeys(target_ids)))
    else:
        print("No Order IDs provided.")
