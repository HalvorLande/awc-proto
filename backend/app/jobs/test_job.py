import os, requests
from dotenv import load_dotenv
load_dotenv("backend/.env")

headers = {"Authorization": f"Token {os.getenv('PROFF_API_KEY')}", "Accept": "application/json", "api-version":"1.1"}

r = requests.get(
    "https://api.proff.no/api/companies/register/NO",
    headers=headers,
    params={"pageSize": 1, "industryCode": "63.920"},
    timeout=30,
)
print("search:", r.status_code, r.text[:200])
