"""Read sheet column V (status) via Apps Script and add Shopify tags live, one order at a time.

- RTO / Undelivered  -> tag: RTO
- Delivered          -> tag: Delivered

Default: LIVE updates (Shopify mein tag lagate jayega). Dry run ke liye: --dry-run
"""
import argparse
import os
import time
from typing import Optional

import pandas as pd
import requests

from utils import fetch_orders_from_apps_script, get_shopify_config, read_secret

OUT_FILE = "Sheet_Status_Shopify_Tags.xlsx"


def status_to_tag(status_raw: str) -> Optional[str]:
    s = str(status_raw or "").strip().lower()
    if not s:
        return None
    if "undelivered" in s or "rto" in s or "return to origin" in s:
        return "RTO"
    if "delivered" in s:
        return "Delivered"
    return None


def find_shopify_order(order_id: str, shopify_cfg: dict):
    oid = str(order_id).replace("#", "").strip()
    if not oid:
        return None
    shop_url = shopify_cfg["shop_url"]
    headers = shopify_cfg["headers"]
    search_url = f"{shop_url}/admin/api/2024-01/orders.json"
    for search_name in (f"#{oid}", oid):
        try:
            resp = requests.get(
                search_url,
                headers=headers,
                params={"name": search_name, "status": "any"},
                timeout=30,
            )
            resp.raise_for_status()
            orders = resp.json().get("orders", [])
            if orders:
                return orders[0]
        except Exception:
            pass
    return None


def add_shopify_tag(s_order: dict, tag: str, shopify_cfg: dict) -> tuple[str, str]:
    existing = s_order.get("tags") or ""
    tags = [t.strip() for t in existing.split(",") if t.strip()]
    if tag in tags:
        return "skipped", f"already has '{tag}'"

    tags.append(tag)
    order_pk = s_order["id"]
    url = f"{shopify_cfg['shop_url']}/admin/api/2024-01/orders/{order_pk}.json"
    try:
        resp = requests.put(
            url,
            headers=shopify_cfg["headers"],
            json={"order": {"id": order_pk, "tags": ", ".join(tags)}},
            timeout=30,
        )
        if resp.status_code == 200:
            return "tagged", f"TAGGED '{tag}' on Shopify"
        return "error", f"HTTP {resp.status_code}: {resp.text[:150]}"
    except Exception as e:
        return "error", str(e)[:150]


def save_progress(results: list) -> None:
    if results:
        pd.DataFrame(results).to_excel(OUT_FILE, index=False)


def run_sync(dry_run: bool = False):
    secrets = read_secret()
    shopify_cfg = get_shopify_config(secrets)
    if not shopify_cfg.get("shop_url") or not shopify_cfg["headers"].get("X-Shopify-Access-Token"):
        print("[ERROR] Shopify not configured in secret.txt (shop_url, shop_assesstoken).")
        return

    print("Loading orders from Google Sheet (Apps Script)...")
    try:
        orders = fetch_orders_from_apps_script()
    except Exception as e:
        print(f"[ERROR] Could not load sheet orders: {e}")
        return

    candidates = []
    for o in orders:
        order_id = str(o.get("ord_serial", "")).replace("#", "").strip()
        sheet_status = str(o.get("status", "")).strip()
        tag = status_to_tag(sheet_status)
        if order_id and tag:
            candidates.append({
                "order_id": order_id,
                "row": int(o.get("row_number", 0) or 0),
                "customer": str(o.get("customer", "")).strip(),
                "sheet_status": sheet_status,
                "tag": tag,
            })

    print(f"[INFO] {len(orders)} sheet rows | {len(candidates)} need RTO/Delivered tag")
    if dry_run:
        print("[DRY RUN] Shopify par koi change nahi hoga.\n")
    else:
        print("[LIVE] Har order par turant Shopify tag update hoga.\n")

    results = []
    tagged_count = skipped_count = error_count = not_found_count = 0

    for i, item in enumerate(candidates):
        order_id = item["order_id"]
        tag = item["tag"]
        prefix = f"[{i + 1}/{len(candidates)}] #{order_id} (V={item['sheet_status']})"

        s_order = find_shopify_order(order_id, shopify_cfg)
        if not s_order:
            not_found_count += 1
            print(f"{prefix} -> NOT FOUND on Shopify")
            results.append({**item, "shopify_name": "", "result": "not_found", "message": "Shopify order not found"})
            save_progress(results)
            time.sleep(0.2)
            continue

        shopify_name = s_order.get("name", "")

        if dry_run:
            existing = s_order.get("tags") or ""
            has_tag = tag in [t.strip() for t in existing.split(",") if t.strip()]
            message = f"would skip (already has '{tag}')" if has_tag else f"would add '{tag}'"
            print(f"{prefix} -> {shopify_name} | {message}")
            results.append({**item, "shopify_name": shopify_name, "result": "dry_run", "message": message})
        else:
            result, message = add_shopify_tag(s_order, tag, shopify_cfg)
            print(f"{prefix} -> {shopify_name} | {message}")
            if result == "tagged":
                tagged_count += 1
            elif result == "skipped":
                skipped_count += 1
            else:
                error_count += 1
            results.append({**item, "shopify_name": shopify_name, "result": result, "message": message})

        save_progress(results)
        if not dry_run:
            print(
                f"         running total: tagged={tagged_count} | skipped={skipped_count} | "
                f"errors={error_count} | not_found={not_found_count}"
            )
        time.sleep(0.35)

    print(f"\n[INFO] Log saved: {os.path.abspath(OUT_FILE)}")
    if dry_run:
        print(f"[DONE] DRY RUN only — Shopify par koi update nahi hua. Live ke liye bina --dry-run chalao.")
    else:
        print(
            f"[DONE] Tagged: {tagged_count} | Already had tag: {skipped_count} | "
            f"Not on Shopify: {not_found_count} | Errors: {error_count}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sheet V status -> Shopify tags")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Sirf preview; Shopify update nahi karega",
    )
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="LIVE mode mein confirm skip karo",
    )
    args = parser.parse_args()

    print("Sheet V Status -> Shopify Tags")
    print("  RTO / Undelivered -> tag 'RTO'")
    print("  Delivered         -> tag 'Delivered'")
    if args.dry_run:
        print("  Mode: DRY RUN\n")
    else:
        print("  Mode: LIVE (Shopify update hoga)\n")
        if not args.yes:
            confirm = input("Start LIVE tagging? (Y/n): ").strip().lower()
            if confirm in ("n", "no"):
                print("Cancelled.")
                raise SystemExit(0)

    run_sync(dry_run=args.dry_run)
