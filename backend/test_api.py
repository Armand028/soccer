import requests

url = "https://bet365data.p.rapidapi.com/tennis/events/%7Bfi%7D"

headers = {
	"Content-Type": "application/json",
	"x-rapidapi-host": "bet365data.p.rapidapi.com",
	"x-rapidapi-key": "71f1cb10a2msh46e08954191fd4dp1e55cajsne7b50af76124"
}

print("Testing provided tennis endpoint...")
response = requests.get(url, headers=headers)
print("Status:", response.status_code)
print("Response:", response.text)
