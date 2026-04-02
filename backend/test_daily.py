import requests
from datetime import datetime

RAPID_API_KEY = "71f1cb10a2msh46e08954191fd4dp1e55cajsne7b50af76124"
RAPID_API_HOST = "sportapi7.p.rapidapi.com"

def get_headers():
    return {
        "x-rapidapi-key": RAPID_API_KEY,
        "x-rapidapi-host": RAPID_API_HOST
    }

date_str = datetime.now().strftime("%Y-%m-%d")
url = f"https://sportapi7.p.rapidapi.com/api/v1/sport/football/scheduled-events/{date_str}"
print(f"Testing URL: {url}")
response = requests.get(url, headers=get_headers())
print("Status:", response.status_code)

if response.status_code == 200:
    data = response.json()
    print("Keys in response:", list(data.keys()))
    if 'events' in data:
        print(f"Found {len(data['events'])} events today.")
        if len(data['events']) > 0:
            print(data['events'][0]['tournament']['name'])
            # Print card info if available
            print("First event keys:", list(data['events'][0].keys()))
else:
    print(response.text)
