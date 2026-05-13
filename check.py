import requests
from utils import read_secret

secrets = read_secret()
awbs = [
    '52799210002236',
    '52799210002155',
    '52799210001746',
    '52799210002240'
]
url = 'https://track.delhivery.com/api/v1/packages/json/'
headers = {'Authorization': f'Token {secrets.get("DELHIVERY_API_KEY", "")}'}

for awb in awbs:
    try:
        r = requests.get(url, headers=headers, params={'waybill': awb})
        data = r.json().get('ShipmentData', [])
        if data:
            shipment = data[0].get('Shipment', {})
            status = shipment.get('Status', {}).get('Status', '')
            instructions = shipment.get('Status', {}).get('Instructions', '')
            location = shipment.get('Status', {}).get('StatusLocation', '')
            status_dt = shipment.get('Status', {}).get('StatusDateTime', '')
            print(f'AWB: {awb}')
            print(f'  Status: {status}')
            print(f'  Instructions: {instructions}')
            print(f'  Location: {location}')
            print(f'  Last Updated: {status_dt}')
            print('-'*40)
        else:
            print(f'AWB: {awb} | No Data')
    except Exception as e:
        print(f'AWB: {awb} | Error: {e}')
