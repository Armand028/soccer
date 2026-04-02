import os
import requests
import sqlite3
import json

DB_PATH = "soccer.db"
RAPID_API_KEY = "71f1cb10a2msh46e08954191fd4dp1e55cajsne7b50af76124"
RAPID_API_HOST = "sportapi7.p.rapidapi.com"

def setup_database():
    conn = sqlite3.connect(DB_PATH)
    return conn

def test_api_connection():
    url = "https://sportapi7.p.rapidapi.com/api/v1/unique-tournament/17/seasons"
    
    headers = {
        "x-rapidapi-key": RAPID_API_KEY,
        "x-rapidapi-host": RAPID_API_HOST
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        print("Successfully connected to API!")
        
        # Print a sample of the data to understand the structure
        if 'seasons' in data:
            print(f"Found {len(data['seasons'])} seasons for this tournament.")
            print("First season sample:")
            print(json.dumps(data['seasons'][0], indent=2))
        else:
            print("Response:", json.dumps(data, indent=2))
            
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to API: {e}")

if __name__ == "__main__":
    test_api_connection()
