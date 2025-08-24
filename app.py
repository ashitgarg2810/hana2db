import requests
import base64

# --------- CONFIG ---------
HOST = "https://dbc-1385039b-b177.cloud.databricks.com"   # 👈 your workspace URL
TOKEN = "dapi81bfbcee432414d88ca60fa9f83efc02"                                         # 👈 your token
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

TEST_PATH = "/dbfs/tmp/test.txt"   # use /dbfs/tmp/ for quick test (safe place)


def dbfs_write_test():
    # Step 1: Create handle
    create_url = f"{HOST}/api/2.0/dbfs/create"
    resp = requests.post(create_url, headers=HEADERS, json={"path": TEST_PATH, "overwrite": True})
    print("CREATE:", resp.status_code, resp.text)
    resp.raise_for_status()
    handle = resp.json()["handle"]

    # Step 2: Add block (hello world text)
    add_url = f"{HOST}/api/2.0/dbfs/add-block"
    data_b64 = base64.b64encode(b"Hello from API!").decode("utf-8")
    resp = requests.post(add_url, headers=HEADERS, json={"handle": handle, "data": data_b64})
    print("ADD BLOCK:", resp.status_code, resp.text)
    resp.raise_for_status()

    # Step 3: Close file
    close_url = f"{HOST}/api/2.0/dbfs/close"
    resp = requests.post(close_url, headers=HEADERS, json={"handle": handle})
    print("CLOSE:", resp.status_code, resp.text)
    resp.raise_for_status()

    # Step 4: Read file back
    read_url = f"{HOST}/api/2.0/dbfs/read"
    resp = requests.get(read_url, headers=HEADERS, params={"path": TEST_PATH})
    print("READ:", resp.status_code)
    resp.raise_for_status()
    data_b64 = resp.json()["data"]
    print("CONTENT:", base64.b64decode(data_b64).decode("utf-8"))


if __name__ == "__main__":
    dbfs_write_test()
