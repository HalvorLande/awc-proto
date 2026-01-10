import os, requests
from dotenv import load_dotenv

load_dotenv("backend/.env")

headers = {
    "Authorization": f"Token {os.getenv('PROFF_API_KEY')}",
    "Accept": "application/json",
}

url = "https://api.proff.no/api/companies/register/NO"
params = {"industryCode": "63.920", "pageSize": 1}

r = requests.get(url, headers=headers, params=params, timeout=30)
print("status:", r.status_code)
print("request:", r.request.url)
print("body:", r.text[:500])
