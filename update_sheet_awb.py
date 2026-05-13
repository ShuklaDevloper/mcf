import os
import re
import sys
from typing import List, Optional

try:
    import gspread
except ImportError:
    print("Please install gspread: pip install gspread google-auth")
    sys.exit(1)

SHEET_ID = "1OvtzHInl8viaLG6f2ZLG3u5h6YQpfID2UAbI64cYhF4"
SHEET_TAB = "Sheet1"
CREDS_FILE = "hide.json"

def normalize_order_id(v: str) -> str:
    s = (v or "").strip()
    if s.startswith("#"):
        s = s[1:]
    return s.strip()

def normalize_header(h: str) -> str:
    s = (h or "").strip().lower()
    s = re.sub(r"[\s\-_]+", "", s)
    s = re.sub(r"[^a-z0-9]", "", s)
    return s

def main():
    print("Connecting to Google Sheet...")
    try:
        import streamlit as st
        # First try to see if we are in Streamlit and have secrets
        try:
            creds_dict = st.secrets["gcp_service_account"]
            gc = gspread.service_account_from_dict(dict(creds_dict))
        except Exception:
            # Fallback to file
            creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), CREDS_FILE)
            if not os.path.exists(creds_path):
                print(f"Error: {CREDS_FILE} not found and st.secrets missing.")
                return
            gc = gspread.service_account(filename=creds_path)
            
        ws = gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB)
    except Exception as e:
        print(f"Failed to connect to Google Sheet: {e}")
        return
    
    print("Fetching data from sheet...")
    values = ws.get_all_values()
    if not values or len(values) < 2:
        print("Sheet is empty.")
        return
        
    headers = values[0]
    
    # 1. Find Order ID column (user says column A, but we can search or fallback to 0)
    order_col = -1
    for i, h in enumerate(headers):
        nh = normalize_header(h)
        if nh in {"orderid", "order", "ord", "orderno", "ordernumber"}:
            order_col = i
            break
            
    if order_col == -1:
        order_col = 0 # Default to Column A
        
    # 2. Find Tracking / AWB column
    tracking_col = -1
    for i, h in enumerate(headers):
        nh = normalize_header(h)
        if nh in {"tracking", "trackingn", "trackingno", "trackingnumber", "awb", "awbno", "waybill", "trackingm"}:
            tracking_col = i
            break
            
    if tracking_col == -1:
        print("❌ Could not find Tracking/AWB column in sheet headers!")
        return
        
    print(f"[OK] Order ID column: '{headers[order_col]}'")
    print(f"[OK] Tracking column: '{headers[tracking_col]}'")
    
    # Map Order ID to Row Number (1-indexed for gspread)
    order_to_row = {}
    for row_idx, row in enumerate(values):
        if row_idx == 0: continue
        if order_col < len(row):
            oid = normalize_order_id(row[order_col])
            if oid:
                order_to_row[oid] = row_idx + 1 # +1 because gspread rows are 1-indexed
                
    # Find customer details columns for display
    i_name = -1
    i_city = -1
    i_phone = -1
    for i, h in enumerate(headers):
        nh = normalize_header(h)
        if nh in {"customername", "customer", "name"} and i_name == -1:
            i_name = i
        if nh in {"city"} and i_city == -1:
            i_city = i
        if nh in {"phonenumber", "phone", "phonenum", "contact"} and i_phone == -1:
            i_phone = i
            
    print("\nPaste your data (Order ID, AWB, SKU). Press Enter on an empty line to finish:")
    lines = []
    while True:
        try:
            line = input()
            if not line.strip():
                break
            lines.append(line.strip())
        except EOFError:
            break
            
    if not lines:
        print("No data provided.")
        return
        
    updates = []
    awbs_and_skus = []
    
    print("\n--- Processing ---")
    for line in lines:
        # Support both tab-separated (from Excel/Sheets) or space-separated
        if '\t' in line:
            parts = [p.strip() for p in line.split('\t') if p.strip()]
        else:
            parts = [p.strip() for p in re.split(r'\s+', line) if p.strip()]
            
        if len(parts) < 2:
            print(f"⚠️ Skipping invalid line (needs at least Order ID and AWB): {line}")
            continue
            
        order_id = normalize_order_id(parts[0])
        awb = parts[1]
        sku = parts[2] if len(parts) > 2 else "N/A"
        
        row_num = order_to_row.get(order_id)
        if not row_num:
            print(f"❌ Order {order_id}: NOT FOUND in Google Sheet.")
            continue
            
        row_data = values[row_num - 1]
        name = row_data[i_name] if i_name != -1 and i_name < len(row_data) else "Unknown"
        city = row_data[i_city] if i_city != -1 and i_city < len(row_data) else "Unknown"
        phone = row_data[i_phone] if i_phone != -1 and i_phone < len(row_data) else ""
        
        print(f"✅ Order {order_id} | Customer: {name} ({city}) {phone} | AWB: {awb} | SKU: {sku}")
        
        # Batch update logic
        updates.append({
            'range': gspread.utils.rowcol_to_a1(row_num, tracking_col + 1), # +1 for 1-based col
            'values': [[awb]]
        })
        updates.append({
            'range': f"R{row_num}",
            'values': [["Fulfilled"]]
        })
        awbs_and_skus.append((awb, sku))
        
    if updates:
        print(f"\nUpdating {len(updates)} rows in Google Sheet... Please wait.")
        ws.batch_update(updates)
        print("✅ Google Sheet successfully updated!")
        
        # Now run del.label.py to generate labels for these AWBs
        import subprocess
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        del_label_path = os.path.join(script_dir, "del_label.py")
        
        if os.path.exists(del_label_path):
            print("\n--- Generating Labels ---")
            cmd = [sys.executable, del_label_path]
            for awb, sku in awbs_and_skus:
                cmd.extend(["--awb", awb])
                if sku and sku != "N/A":
                    cmd.extend(["--sku_map", f"{awb}={sku}"])
            subprocess.run(cmd)
        else:
            print("\n⚠️ 'del.label.py' not found in the same directory. Cannot generate labels automatically.")

    else:
        print("No valid updates to perform.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting.")
