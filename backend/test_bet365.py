import requests
import json

RAPID_API_KEY = "71f1cb10a2msh46e08954191fd4dp1e55cajsne7b50af76124"
PROXY_SECRET = "71f1cb10a2msh46e08954191fd4dp1e55cajsne7b50af76124"

def test_bet365_api():
    # Let's try the live-events sports endpoint
    url = "https://bet365data.p.rapidapi.com/v2/bet365/live-events/sports"
    
    headers = {
        "x-rapidapi-host": "bet365data.p.rapidapi.com",
        "x-rapidapi-key": RAPID_API_KEY,
        "x-rapidapi-proxy-secret": PROXY_SECRET
    }
    
    print("Testing /v2/bet365/live-events/sports...")
    response = requests.get(url, headers=headers)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")
    print("-----")
    
    url = "https://bet365data.p.rapidapi.com/v2/bet365/live-events?sport=Soccer"
    print("Testing /v2/bet365/live-events?sport=Soccer...")
    response = requests.get(url, headers=headers)
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")

if __name__ == "__main__":
    test_bet365_api()
