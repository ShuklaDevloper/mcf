"""Manual AWB tracking: Delhivery → MCF (Swiship) → iThink for each pasted tracking number."""
import time

import pandas as pd

from live_tracker import looks_like_tracking_number, track_awb_live
from utils import format_sheet_cell_value, init_sheets_service, read_secret, SHEET_ID


def main():
    secrets = read_secret()

    print("Initializing Sheets Service...")
    service = init_sheets_service()

    print("Fetching Sheet mapping (Tracking Numbers)...")
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range="Sheet1!A:W",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
        rows = result.get("values", [])
    except Exception as e:
        print(f"Error fetching sheet: {e}")
        return

    if not rows:
        print("No rows found in sheet.")
        return

    headers = [str(h).strip().lower() for h in rows[0]]
    tracking_col_idx = -1
    for col in ["tracking no", "tracking no.", "tracking number"]:
        if col in headers:
            tracking_col_idx = headers.index(col)
            break
    if tracking_col_idx == -1:
        tracking_col_idx = 19
        print("Tracking header not found, defaulting to Column T")

    row_map = {}
    for i, row in enumerate(rows):
        if len(row) <= tracking_col_idx:
            continue
        t_no = format_sheet_cell_value(row[tracking_col_idx]).strip()
        if t_no:
            row_map[t_no] = i + 1

    print(
        "Paste Tracking Numbers (space or newline). Type DONE when finished.\n"
        "Lookup order: Delhivery → MCF → iThink\n"
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

    if not target_ids:
        print("No Tracking Numbers provided.")
        return

    numbers = []
    for raw in dict.fromkeys(target_ids):
        if not looks_like_tracking_number(raw):
            print(f"[SKIP] Not a tracking number: {raw!r}")
            continue
        numbers.append(format_sheet_cell_value(raw).strip())

    if not numbers:
        print("No valid tracking numbers after filtering.")
        return

    results = []
    sheet_updates = []

    try:
        for idx, tracking_no in enumerate(numbers):
            track = track_awb_live(
                tracking_no,
                secrets=secrets,
                fixed_order=True,
            )

            if track.get("found"):
                status = track.get("status", "Intransit")
                carrier = track.get("carrier", "")
                eta_value = track.get("eta", "")
                pickup_value = track.get("pickup", "")
                delivery_value = track.get("delivery", "")
                last_update_value = track.get("last_update", "")
                tracking_url = track.get("url", "")
            else:
                status = "Not Found"
                carrier = ""
                eta_value = pickup_value = delivery_value = ""
                last_update_value = tracking_url = ""

            results.append({
                "Tracking Number": tracking_no,
                "Carrier": carrier,
                "Status": status,
                "Last Update": last_update_value,
                "ETA": eta_value,
                "Pickup Date": pickup_value,
                "Delivery Date": delivery_value,
                "Tracking URL": tracking_url,
            })

            row_num = row_map.get(tracking_no)
            if row_num:
                if str(status).lower() == "cancelled":
                    sheet_updates.append({
                        "range": f"Sheet1!Q{row_num}:R{row_num}",
                        "values": [["Cancelled", "Cancelled"]],
                    })
                    sheet_updates.append({
                        "range": f"Sheet1!V{row_num}",
                        "values": [["Cancelled"]],
                    })
                else:
                    sheet_updates.append({
                        "range": f"Sheet1!V{row_num}",
                        "values": [[status]],
                    })
                sheet_msg = f" | Updated Row {row_num}"
            else:
                sheet_msg = " | Not in Sheet"

            print(
                f"Tracking {idx + 1}/{len(numbers)}: {tracking_no} -> "
                f"{status} ({carrier}){sheet_msg}"
            )
            time.sleep(0.3)

    except KeyboardInterrupt:
        print("\nProcess interrupted by user! Saving current progress...")
    finally:
        if sheet_updates:
            print("\nUpdating Google Sheet (Q/R/V)...")
            try:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=SHEET_ID,
                    body={"valueInputOption": "RAW", "data": sheet_updates},
                ).execute()
                print("Google Sheet Updated!")
            except Exception as e:
                print(f"Error updating sheet: {e}")

        if results:
            pd.DataFrame(results).to_excel("Tracking_Results.xlsx", index=False)
            print(f"Saved {len(results)} results to Tracking_Results.xlsx")
        else:
            print("No results to save.")


if __name__ == "__main__":
    main()
