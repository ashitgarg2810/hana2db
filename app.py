import streamlit as st
import requests

# Databricks details
DATABRICKS_HOST = "https://dbc-1385039b-b177.cloud.databricks.com"
DATABRICKS_TOKEN = "507145734504441"
VOLUME_PATH = "/Volumes/ashit_garg/project1/project1/"  # replace with your volume

st.title("Upload XML to Databricks Volume")

uploaded_file = st.file_uploader("Choose an XML file", type=["txt"])

if uploaded_file is not None:
    file_name = uploaded_file.name
    file_bytes = uploaded_file.read()

    # Save into local /tmp first
    local_path = f"/tmp/{file_name}"
    with open(local_path, "wb") as f:
        f.write(file_bytes)

    # Upload to Databricks Volume via DBFS API (prefix /Volumes/ works)
    target_path = VOLUME_PATH + file_name
    url = f"{DATABRICKS_HOST}/api/2.0/dbfs/put"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    data = {
        "path": target_path,
        "overwrite": "true"
    }
    files = {"contents": file_bytes}

    response = requests.post(url, headers=headers, data=data, files=files)

    if response.status_code == 200:
        st.success(f"File uploaded successfully to {target_path}")
    else:
        st.error(f"Upload failed: {response.text}")
