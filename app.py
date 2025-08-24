import streamlit as st
import requests

host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]

st.title("📂 Upload File to Databricks Volume")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

if uploaded_file is not None:
    # Path in Volume
    volume_path = f"/Volumes/ashit_garg/project1/project1/{uploaded_file.name}"

    url = f"{host}/api/2.0/files/upload"

    headers = {"Authorization": f"Bearer {token}"}

    files = {
        "path": (None, volume_path),
        "contents": (uploaded_file.name, uploaded_file, "application/octet-stream")
    }

    response = requests.post(url, headers=headers, files=files)

    if response.status_code == 200:
        st.success(f"✅ Uploaded to {volume_path}")
    else:
        st.error(f"❌ Upload failed: {response.status_code} - {response.text}")
