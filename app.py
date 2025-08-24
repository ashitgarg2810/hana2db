import streamlit as st
import requests

# Databricks config
DATABRICKS_HOST = "https://dbc-1385039b-b177.cloud.databricks.com"
TOKEN = "507145734504441"
VOLUME_PATH = "/Volumes/ashit_garg/project1/project1"

headers = {
    "Authorization": f"Bearer {TOKEN}"
}

st.title("Upload File to Databricks Volume")

uploaded_file = st.file_uploader("Choose a file", type=["xml", "txt", "csv", "json"])

if uploaded_file is not None:
    file_content = uploaded_file.read()
    file_name = uploaded_file.name
    target_path = f"{VOLUME_PATH}/{file_name}"

    # Upload via DBFS API
    # (Even though it’s a volume, we use dbfs API and point to the /Volumes path)
    put_url = f"{DATABRICKS_HOST}/api/2.0/dbfs/put"
    resp = requests.post(
        put_url,
        headers=headers,
        json={
            "path": target_path,
            "contents": file_content.decode("utf-8"),
            "overwrite": True
        }
    )

    if resp.status_code == 200:
        st.success(f"File uploaded successfully to {target_path}")
    else:
        st.error(f"Upload failed: {resp.text}")
