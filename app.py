import requests, os

url = "https://dbc-1385039b-b177.cloud.databricks.com/api/2.0/token/list"
headers = {"Authorization": f"Bearer dapi81bfbcee432414d88ca60fa9f83efc02"}
resp = requests.get(url, headers=headers)
print(resp.json())
