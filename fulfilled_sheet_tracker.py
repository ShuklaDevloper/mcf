"""Final sheet automation — ek hi script, dono kaam automatic.

1) Column T khali / galat  → Order ID se AWB dhundo:
      MCF → (Unfulfillable ho to bhi) Delhivery → iThink → Shopify
      Sirf jab teeno fail + MCF Unfulfillable → sheet par MCF: Unfulfillable
      AWB mile → Shopify update + sheet Q–T likho

2) Column T mein valid AWB  → Live track (Delhivery → MCF → iThink)
      Status RTO / Cancelled / Delivered / Intransit + ETA, RTO column update

Run:  python fulfilled_sheet_tracker.py
Dry:  python fulfilled_sheet_tracker.py --dry-run
"""
import argparse
import re
import time

import pandas as pd

from live_tracker import (
    lookup_awb_mcf_first,
    looks_like_tracking_number,
    track_awb_live,
)
from shopify_to_sheetupdate import fetch_shopify_order_details
from utils import (
    SHEET_ID,
    batch_update_tracking_rows,
    build_tracking_url,
    fulfill_order,
    get_access_token,
    get_shopify_config,
    get_shopify_order,
    infer_sheet_source_q,
    init_sheets_service,
    read_secret,
)

OUT_FILE = "Fulfilled_Sheet_Tracker_Results.xlsx"
SHEET_RANGE = "Sheet1!A:AF"


def _header_index(headers, *names):
    for name in names:
        key = name.strip().lower()
        if key in headers:
            return headers.index(key)
    return -1


def _format_cell_value(val):
    """Preserve full numeric AWBs (avoid 1.32E+12 from Sheets)."""
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


def load_sheet_rows(service):
    """Return (headers_lower, data_rows) from Sheet1."""
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=SHEET_RANGE,
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    rows = result.get("values", [])
    if len(rows) <= 1:
        return [], []
    headers = [str(h).strip().lower() for h in rows[0]]
    return headers, rows[1:]


def _cell(row, idx):
    if idx < 0 or idx >= len(row):
        return ""
    return _format_cell_value(row[idx])


def row_is_eligible(row, indices):
    """Apply sheet filter rules from plan."""
    fulfilled = _cell(row, indices["fulfilled"])
    tracking_t = _cell(row, indices["tracking"])
    status_v = _cell(row, indices["status"])
    rto_ab = _cell(row, indices["rto"])

    if not fulfilled.lower().startswith("fulf"):
        return False, "R not FULFILLED"
    status_low = status_v.lower()
    if status_low == "delivered":
        return False, "V Delivered"
    if status_low == "rto":
        return False, "V RTO"
    if status_low == "undelivered":
        return False, "V Undelivered"
    if "cancelled" in fulfilled.lower() or "cancelled" in status_v.lower():
        return False, "Cancelled"
    if "unfulfillable" in status_v.lower():
        return False, "Already Unfulfillable"
    if rto_ab.lower() == "delivered":
        return False, "RTO complete (AB Delivered)"
    return True, ""


def find_awb_by_order_id(order_id, secrets, shopify_cfg, token):
    """MCF → Delhivery → iThink, then Shopify fulfillments fallback."""
    oid = str(order_id).replace("#", "").strip()
    if not oid:
        return "", "", "", ""

    awb, carrier, source, detail = lookup_awb_mcf_first(oid, secrets=secrets, mcf_token=token)
    if awb:
        src = source or infer_sheet_source_q(carrier, awb)
        return awb, carrier or src, src, ""

    if (detail or "").strip().lower() == "unfulfillable":
        return "", "", "MCF", "Unfulfillable"

    if shopify_cfg.get("shop_url"):
        info = fetch_shopify_order_details(oid, shopify_cfg)
        if info and info.get("tracking_no"):
            carrier = info.get("carrier") or "Shopify"
            tn = info["tracking_no"]
            return tn, carrier, infer_sheet_source_q(carrier, tn), ""

    return "", "", "", detail or ""


def resolve_awb(order_id, sheet_tracking, sheet_carrier, sheet_source, secrets, shopify_cfg, token):
    """Use sheet T when valid; order ID search only when T empty or placeholder."""
    oid = str(order_id).replace("#", "").strip()
    sheet_t = _format_cell_value(sheet_tracking)

    if looks_like_tracking_number(sheet_t):
        src = sheet_source or infer_sheet_source_q(sheet_carrier, sheet_t)
        return sheet_t, sheet_carrier, src, "sheet_t"

    if oid:
        awb, carrier, source, detail = find_awb_by_order_id(oid, secrets, shopify_cfg, token)
        if awb:
            return awb, carrier, source, "order_id"
        if detail:
            return "", "", "", f"miss:{detail}"

    return "", "", "", ""


def _display_carrier(carrier, source_q, awb):
    car = (carrier or "").strip()
    if car.isdigit() and len(car) >= 10:
        car = ""
    if car and car.lower() not in ("fulfilled", "fulfill"):
        return car
    src = (source_q or "").strip().lower()
    if src == "delhivery":
        return "Delhivery"
    if src == "ithink":
        return "iThink Logistics"
    if src == "mcf":
        return "Amazon Transportation Services"
    return infer_sheet_source_q(carrier, awb) or source_q or "Manual"


def _append_sheet_row(sheet_updates, row_num, source_q, carrier, awb, url, status, track, fulfilled="FULFILLED"):
    sheet_updates.append({
        "row": row_num,
        "source": source_q,
        "fulfilled": fulfilled,
        "carrier": _display_carrier(carrier, source_q, awb),
        "tracking_no": awb,
        "url": url or build_tracking_url(carrier, awb),
        "status": status,
        "eta": (track or {}).get("eta", ""),
        "pickup": (track or {}).get("pickup", ""),
        "delivery": (track or {}).get("delivery", ""),
        "last_status": (track or {}).get("last_update", ""),
        "rto": (track or {}).get("rto", ""),
    })


def push_tracking_to_shopify(order_id, tracking_no, carrier, shopify_cfg):
    """Fulfill / update Shopify when AWB found from carrier APIs."""
    if not shopify_cfg.get("shop_url") or not tracking_no:
        return
    try:
        s_order = get_shopify_order(order_id, shopify_cfg["headers"], shopify_cfg["shop_url"])
        if not s_order:
            return
        fulfill_order(
            s_order,
            shopify_cfg["headers"],
            shopify_cfg["shop_url"],
            tracking_info={
                "number": tracking_no,
                "company": _display_carrier(carrier, "", tracking_no),
                "url": build_tracking_url(carrier, tracking_no),
            },
        )
    except Exception:
        pass


def build_eligible_items(headers, data_rows):
    """Parse sheet into list of work items."""
    indices = {
        "order_id": _header_index(headers, "ord_serial", "order id", "order"),
        "source": _header_index(headers, "source"),
        "fulfilled": _header_index(headers, "fulfilled", "column r"),
        "carrier": _header_index(headers, "carrier"),
        "tracking": _header_index(headers, "tracking no", "tracking no.", "tracking number"),
        "status": _header_index(headers, "status"),
        "rto": _header_index(headers, "rto"),
    }
    if indices["fulfilled"] < 0 or indices["tracking"] < 0:
        raise ValueError(f"Required columns missing. Headers: {headers}")

    items = []
    skip_counts = {}

    for i, row in enumerate(data_rows):
        row_num = i + 2
        ok, reason = row_is_eligible(row, indices)
        if not ok:
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            continue

        order_id = _cell(row, indices["order_id"]).replace("#", "").strip()
        tracking_t = _cell(row, indices["tracking"])
        items.append({
            "row": row_num,
            "order_id": order_id,
            "source": _cell(row, indices["source"]),
            "carrier": _cell(row, indices["carrier"]),
            "tracking_t": tracking_t,
            "has_awb": looks_like_tracking_number(tracking_t),
            "status_v": _cell(row, indices["status"]),
            "rto_ab": _cell(row, indices["rto"]),
        })

    return items, skip_counts, indices


def run(dry_run=False):
    secrets = read_secret()
    shopify_cfg = get_shopify_config(secrets)
    token, _ = get_access_token(secrets)

    try:
        service = init_sheets_service(secrets)
    except Exception as e:
        print(f"[ERROR] Google Sheets auth failed: {e}")
        return

    print("Loading sheet rows...")
    headers, data_rows = load_sheet_rows(service)
    if not headers:
        print("[INFO] Sheet empty.")
        return

    items, skip_counts, _indices = build_eligible_items(headers, data_rows)
    need_find = sum(1 for it in items if not it["has_awb"])
    have_awb = len(items) - need_find
    print(f"[INFO] Eligible rows: {len(items)}")
    print(f"  -> T me AWB hai (sirf track): {have_awb}")
    print(f"  -> T khali/placeholder (order ID se dhundo): {need_find}")
    for reason, cnt in sorted(skip_counts.items(), key=lambda x: -x[1]):
        print(f"  Skipped ({reason}): {cnt}")

    if not items:
        print("[DONE] Nothing to process.")
        return

    results = []
    sheet_updates = []
    found_awb_count = 0
    tracked_count = 0
    track_failed_count = 0

    total = len(items)
    for idx, item in enumerate(items):
        order_id = item["order_id"]
        row_num = item["row"]
        prefix = f"[{idx + 1}/{total}] Row {row_num} | #{order_id}"

        # Phase 1 — sheet T if valid, else order ID search (MCF → Delhivery → iThink)
        effective_awb, carrier, source_q, awb_origin = resolve_awb(
            order_id,
            item["tracking_t"],
            item["carrier"],
            item["source"],
            secrets,
            shopify_cfg,
            token,
        )

        if awb_origin.startswith("miss:"):
            miss = awb_origin.split(":", 1)[-1]
            if miss.lower() == "unfulfillable":
                print(f"{prefix} | MCF Unfulfillable (Delhivery/iThink bhi nahi) — sheet update")
                _append_sheet_row(
                    sheet_updates, row_num, "MCF", "", "", "", "MCF: Unfulfillable", {},
                )
                results.append({
                    "Row": row_num, "Order ID": order_id, "Action": "unfulfillable",
                    "Reason": "MCF: Unfulfillable", "AWB": "", "Status": "MCF: Unfulfillable", "RTO": "",
                })
                time.sleep(0.4)
                continue

        if not effective_awb:
            print(f"{prefix} | AWB not found — sheet unchanged")
            results.append({
                "Row": row_num,
                "Order ID": order_id,
                "Action": "no_awb",
                "Reason": awb_origin or "",
                "AWB": "",
                "Status": "",
                "RTO": "",
            })
            time.sleep(0.4)
            continue

        awb_from_search = awb_origin == "order_id"
        if awb_from_search:
            found_awb_count += 1
            push_tracking_to_shopify(order_id, effective_awb, carrier, shopify_cfg)
            print(f"{prefix} | AWB via order ID: {effective_awb} ({source_q})")
        if not source_q:
            source_q = infer_sheet_source_q(carrier, effective_awb)

        # Phase 2 — live track
        track = track_awb_live(
            effective_awb,
            secrets=secrets,
            carrier_hint=carrier,
            source_hint=source_q,
            existing_rto=item["rto_ab"],
            existing_status=item["status_v"],
            fixed_order=True,
        )

        if not track.get("found"):
            track_failed_count += 1
            url = build_tracking_url(carrier, effective_awb)
            print(f"{prefix} | Track API miss — AWB sheet par likh rahe hain: {effective_awb}")
            _append_sheet_row(
                sheet_updates,
                row_num,
                source_q,
                carrier,
                effective_awb,
                url,
                item["status_v"] or "Awaiting scan",
                {},
            )
            results.append({
                "Row": row_num,
                "Order ID": order_id,
                "Action": "awb_only",
                "Reason": "track_failed_awb_saved",
                "AWB": effective_awb,
                "Status": item["status_v"] or "Awaiting scan",
                "RTO": "",
            })
            time.sleep(0.4)
            continue

        tracked_count += 1
        url = track.get("url") or build_tracking_url(carrier, effective_awb)
        status = track["status"]
        rto_val = track.get("rto", "")

        if str(status).lower() == "cancelled":
            print(f"{prefix} | Cancelled — Q/R/V updated")
            sheet_updates.append({
                "row": row_num,
                "source": "Cancelled",
                "fulfilled": "Cancelled",
                "carrier": _display_carrier(track.get("carrier") or carrier, source_q, effective_awb),
                "tracking_no": effective_awb,
                "url": url,
                "status": "Cancelled",
                "last_status": track.get("last_update", ""),
            })
            results.append({
                "Row": row_num, "Order ID": order_id, "Action": "cancelled",
                "Reason": "Cancelled", "AWB": effective_awb, "Status": "Cancelled", "RTO": "",
            })
            time.sleep(0.4)
            continue

        if str(status).lower() == "rto":
            status = "RTO"
            if not rto_val:
                rto_val = track.get("last_update", "") or "RTO Intransit"

        track_note = "AWB newly found" if awb_from_search else "T se track"
        print(
            f"{prefix} | {source_q} {effective_awb} | {status}"
            + (f" | AB: {rto_val}" if rto_val else "")
            + f" | {track_note}"
        )

        _append_sheet_row(
            sheet_updates,
            row_num,
            source_q,
            track.get("carrier") or carrier,
            effective_awb,
            url,
            status,
            {**(track or {}), "rto": rto_val or track.get("rto", "")},
        )

        results.append({
            "Row": row_num,
            "Order ID": order_id,
            "Action": "updated",
            "Reason": "awb_found" if awb_from_search else "tracked",
            "AWB": effective_awb,
            "Status": status,
            "RTO": rto_val,
            "ETA": track.get("eta", ""),
            "Pickup": track.get("pickup", ""),
            "Delivery": track.get("delivery", ""),
            "Last Status": track.get("last_update", ""),
            "Source": source_q,
        })

        time.sleep(0.4)

    if results:
        pd.DataFrame(results).to_excel(OUT_FILE, index=False)
        print(f"\n[INFO] Saved audit log to {OUT_FILE}")

    if sheet_updates:
        if dry_run:
            print(f"\n[DRY RUN] Would update {len(sheet_updates)} sheet row(s).")
        else:
            print(f"\n[INFO] Updating Google Sheet Q–AB for {len(sheet_updates)} row(s)...")
            chunk = 40
            for i in range(0, len(sheet_updates), chunk):
                batch = sheet_updates[i : i + chunk]
                try:
                    batch_update_tracking_rows(service, SHEET_ID, batch)
                except Exception as e:
                    print(f"[ERROR] Sheet batch update failed: {e}")
                    break
                time.sleep(0.5)
            print("[OK] Google Sheet updated.")

    print(
        f"\n[DONE] Eligible: {total} | AWB via order ID: {found_awb_count} | "
        f"Tracked & sheet updated: {tracked_count} | "
        f"Track failed (AWB saved): {track_failed_count}"
    )


def main():
    parser = argparse.ArgumentParser(description="Fulfilled sheet tracker — find AWB + live status")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and log only; do not write to Google Sheet",
    )
    args = parser.parse_args()
    print("Fulfilled Sheet Tracker")
    print("=" * 50)
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
