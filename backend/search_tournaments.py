import requests
import json
import io
import sys

# Fix encoding for Windows command prompt formatting
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

RAPID_API_KEY = "71f1cb10a2msh46e08954191fd4dp1e55cajsne7b50af76124"
RAPID_API_HOST = "sportapi7.p.rapidapi.com"

def search_tournament(query):
    url = f"https://sportapi7.p.rapidapi.com/api/v1/search/all"
    querystring = {"q": query}
    headers = {
        "x-rapidapi-key": RAPID_API_KEY,
        "x-rapidapi-host": RAPID_API_HOST
    }
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()
        print(f"--- Search Results for '{query}' ---")
        if 'results' in data and len(data['results']) > 0:
             for res in data['results']:
                 if res.get('type') == 'uniqueTournament':
                     t = res.get('entity', {})
                     name = t.get('name', '')
                     category = t.get('category', {}).get('name', '')
                     print(f"ID: {t.get('id')} | Name: {name} | Category: {category}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    search_tournament("liga portugal")
