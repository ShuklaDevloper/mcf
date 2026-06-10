"""Find AWB / tracking by order ID: MCF → Delhivery → iThink."""
import argparse
import time

import pandas as pd

from live_tracker import (
    get_ithink_awb_by_order_no,
    get_ithink_shipment,
    lookup_awb_mcf_first,
    parse_ithink,
)
from utils import build_tracking_url, read_secret

OUT_FILE = "iThink_Order_Tracking_Finder.xlsx"


def lookup_order_tracking(order_id, secrets=None, days_back=90, deep_scan=False):
    """MCF → Delhivery → iThink. MCF Unfulfillable still checks Delhivery + iThink."""
    oid = str(order_id).replace("#", "").strip()
    if not oid:
        return {"order_id": order_id, "found": False, "error": "Empty order ID"}

    awb, carrier, source, detail_status = lookup_awb_mcf_first(oid, secrets=secrets)

    if not awb and deep_scan:
        token = secrets.get("Ithink_access_token", "")
        secret = secrets.get("Ithink_secret_key", "")
        awb, carrier = get_ithink_awb_by_order_no(
            oid,
            token,
            secret,
            days_back=days_back,
            secrets=secrets,
            allow_store_scan=True,
            store_scan_verbose=True,
        )
        if awb:
            source, detail_status = "iThink", "Found on iThink"

    unfulfillable = (detail_status or "").strip().lower() == "unfulfillable"

    if not awb:
        return {
            "order_id": oid,
            "found": False,
            "unfulfillable": unfulfillable,
            "awb": "",
            "carrier": "",
            "source": source or ("MCF" if unfulfillable else ""),
            "status": "Unfulfillable" if unfulfillable else (detail_status or "Not found"),
            "last_update": "",
            "eta": "",
            "pickup": "",
            "delivery": "",
            "rto": "",
            "track_url": "",
        }

    row = {
        "order_id": oid,
        "found": True,
        "unfulfillable": False,
        "awb": awb,
        "carrier": carrier or source,
        "source": source,
        "status": detail_status or "",
        "last_update": "",
        "eta": "",
        "pickup": "",
        "delivery": "",
        "rto": "",
        "track_url": build_tracking_url(carrier or source, awb),
    }

    if source == "iThink":
        token = secrets.get("Ithink_access_token", "")
        secret = secrets.get("Ithink_secret_key", "")
        shipment = get_ithink_shipment(awb, token, secret)
        if shipment:
            info = parse_ithink(shipment)
            if info:
                row.update({
                    "status": info.get("status", "") or row["status"],
                    "last_update": info.get("last_update", ""),
                    "eta": info.get("eta", ""),
                    "pickup": info.get("pickup", ""),
                    "delivery": info.get("delivery", ""),
                    "rto": info.get("rto", ""),
                })

    return row


def run(order_ids, days_back=90, deep_scan=False):
    secrets = read_secret()

    unique_ids = list(dict.fromkeys(str(o).replace("#", "").strip() for o in order_ids if str(o).strip()))
    print(
        f"Looking up {len(unique_ids)} order(s): MCF → Delhivery → iThink"
        + (" (+ iThink deep scan if miss)" if deep_scan else "")
        + "...\n"
    )

    results = []
    for idx, oid in enumerate(unique_ids):
        print(f"[{idx + 1}/{len(unique_ids)}] Order #{oid}...", end=" ", flush=True)
        row = lookup_order_tracking(oid, secrets=secrets, days_back=days_back, deep_scan=deep_scan)
        if row.get("found"):
            src = row.get("source", "")
            print(
                f"{src} AWB {row['awb']}"
                + (f" | {row['status']}" if row.get("status") else "")
            )
        elif row.get("unfulfillable"):
            print("Unfulfillable (MCF — not on Delhivery/iThink)")
        else:
            print(row.get("status") or "not found")
        results.append({
            "Order ID": row["order_id"],
            "Source": row.get("source", ""),
            "AWB / Tracking": row.get("awb", ""),
            "Carrier": row.get("carrier", ""),
            "Status": row.get("status", ""),
            "Last Update": row.get("last_update", ""),
            "ETA": row.get("eta", ""),
            "Pickup": row.get("pickup", ""),
            "Delivery": row.get("delivery", ""),
            "RTO": row.get("rto", ""),
            "Track URL": row.get("track_url", ""),
        })
        time.sleep(0.3)

    if results:
        pd.DataFrame(results).to_excel(OUT_FILE, index=False)
        found = sum(1 for r in results if r["AWB / Tracking"])
        unful = sum(1 for r in results if r["Status"] == "Unfulfillable")
        print(
            f"\n[DONE] Found {found}/{len(results)} | "
            f"Unfulfillable {unful} | Saved to {OUT_FILE}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Find AWB by order ID (MCF → Delhivery → iThink)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Date range for iThink order/get_details lookup (default 90)",
    )
    parser.add_argument(
        "--deep-scan",
        action="store_true",
        help="Slow last resort: scan all iThink store orders if direct lookup fails",
    )
    parser.add_argument(
        "order_ids",
        nargs="*",
        help="Order IDs (optional — paste interactively if omitted)",
    )
    args = parser.parse_args()

    if args.deep_scan:
        print("[WARN] --deep-scan is slow. Normal path is MCF → Delhivery → iThink only.\n")

    if args.order_ids:
        run(args.order_ids, days_back=args.days, deep_scan=args.deep_scan)
        return

    print("Paste Order IDs (space or newline). Type DONE when finished:")
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
        run(target_ids, days_back=args.days, deep_scan=args.deep_scan)
    else:
        print("No Order IDs provided.")


if __name__ == "__main__":
    main()
