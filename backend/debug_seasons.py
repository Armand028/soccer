import requests
import json
import sys

# Fix encoding for Windows command prompt formatting
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

RAPID_API_KEY = "71f1cb10a2msh46e08954191fd4dp1e55cajsne7b50af76124"
RAPID_API_HOST = "sportapi7.p.rapidapi.com"

def get_seasons(tournament_id):
    url = f"https://sportapi7.p.rapidapi.com/api/v1/unique-tournament/{tournament_id}/seasons"
    headers = {
        "x-rapidapi-key": RAPID_API_KEY,
        "x-rapidapi-host": RAPID_API_HOST
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        print(f"Tournament {tournament_id}:")
        for s in data.get('seasons', [])[:7]: # Print the top 7 newest seasons
            print(f"  ID: {s.get('id')} | Name: {s.get('name')} | Year: {s.get('year')}")
    else:
        print(f"Status Code {response.status_code}")

if __name__ == "__main__":
    get_seasons(52)
    get_seasons(238)
