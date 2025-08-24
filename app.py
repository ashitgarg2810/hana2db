import requests
import json
import base64

host = "https://dbc-1385039b-b177.cloud.databricks.com"
token = "dapiXXXXXXXXXXXXX"

headers = {"Authorization": f"Bearer {token}"}

file_path = "/dbfs/tmp/large_file.xml"
local_file = "large_file.xml"   # your XML file on local machine

# 1. Create the file handle
resp = requests.post(
    f"{host}/api/2.0/dbfs/create",
    headers=headers,
    json={"path": file_path, "overwrite": True}
)
handle = resp.json()["handle"]

# 2. Read file in chunks & add blocks
with open(local_file, "rb") as f:
    while True:
        block = f.read(1 << 20)  # 1 MB chunks
        if not block:
            break
        data = base64.b64encode(block).decode("utf-8")
        requests.post(
            f"{host}/api/2.0/dbfs/add-block",
            headers=headers,
            json={"handle": handle, "data": data}
        )

# 3. Close the handle
requests.post(
    f"{host}/api/2.0/dbfs/close",
    headers=headers,
    json={"handle": handle}
)

print(f"Uploaded {local_file} to {file_path}")
