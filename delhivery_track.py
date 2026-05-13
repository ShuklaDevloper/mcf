import os
import requests
from utils import read_secret
from live_tracker import format_dt

def main():
    print("=========================================================================")
    print("                 DELHIVERY BULK TRACKING CHECKER                         ")
    print("=========================================================================")
    
    secrets = read_secret()
    delhivery_api_key = secrets.get("DELHIVERY_API_KEY", "")
    
    if not delhivery_api_key:
        print("[!] Error: DELHIVERY_API_KEY not found in secret.txt")
        return

    url = 'https://track.delhivery.com/api/v1/packages/json/'
    headers = {'Authorization': f'Token {delhivery_api_key}'}

    print("\nTip: Paste multiple tracking numbers (comma, space, or newline separated).")
    print("Press Enter on an empty line to start processing. Type 'exit' to quit.\n")
    
    while True:
        try:
            print("Enter Delhivery AWBs:")
            lines = []
            while True:
                line = input()
                if not line:
                    break
                if line.lower() in ['exit', 'quit', 'q', 'close']:
                    print("Exiting...")
                    return
                lines.append(line)
            
            if not lines:
                continue
                
            # Split by commas and spaces, and filter empty
            import re
            raw_text = " ".join(lines)
            awbs = [x.strip() for x in re.split(r'[\s,]+', raw_text) if x.strip()]
            
            if not awbs:
                continue
                
            print(f"\n🚀 Processing {len(awbs)} Tracking Numbers...\n")
            print(f"{'AWB':<16} | {'STATUS':<15} | {'INSTRUCTIONS':<35} | {'PICKUP':<12} | {'ETA':<12} | {'LOCATION'}")
            print("-" * 120)
            
            for awb in awbs:
                try:
                    r = requests.get(url, headers=headers, params={'waybill': awb}, timeout=15)
                    if r.status_code == 200:
                        data = r.json().get('ShipmentData', [])
                        if data:
                            shipment = data[0].get('Shipment', {})
                            status_obj = shipment.get('Status', {})
                            
                            raw_state = status_obj.get('Status', '')
                            raw_inst = status_obj.get('Instructions', '')
                            loc = status_obj.get('StatusLocation', '')
                            
                            pickup_date = shipment.get('PickUpDate') or shipment.get('PickedupDate')
                            eta_date = shipment.get('ExpectedDeliveryDate') or shipment.get('EDD')
                            
                            p_date = format_dt(pickup_date) if pickup_date else 'N/A'
                            e_date = format_dt(eta_date) if eta_date else 'N/A'
                            
                            # Trim long strings
                            r_inst = (raw_inst[:32] + '...') if len(raw_inst) > 35 else raw_inst
                            r_loc = (loc[:25] + '...') if len(loc) > 28 else loc
                            
                            print(f"{awb:<16} | {raw_state:<15} | {r_inst:<35} | {p_date:<12} | {e_date:<12} | {r_loc}")
                        else:
                            print(f"{awb:<16} | {'No Data':<15} | {'Invalid or Expired':<35} | {'N/A':<12} | {'N/A':<12} | -")
                    else:
                        print(f"{awb:<16} | {'API Error':<15} | {f'HTTP {r.status_code}':<35} | {'N/A':<12} | {'N/A':<12} | -")
                except Exception as e:
                    print(f"{awb:<16} | {'Error':<15} | {str(e)[:35]:<35} | {'N/A':<12} | {'N/A':<12} | -")
            print("-" * 120 + "\n")
            
        except KeyboardInterrupt:
            print("\nExiting...")
            break

if __name__ == "__main__":
    main()
