import streamlit as st
import requests

host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]

st.title("📂 Upload File to Databricks Volume")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

if uploaded_file is not None:
    # File content
    file_bytes = uploaded_file.read()

    # Path in Volume (adjust to your catalog.schema.volume path)
    volume_path = "/Volumes/main.default.my_volume/" + uploaded_file.name

    url = f"{host}/api/2.0/fs/files{volume_path}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }

    response = requests.put(url, headers=headers, data=file_bytes)

    if response.status_code == 200:
        st.success(f"✅ Uploaded to {volume_path}")
    else:
        st.error(f"❌ Upload failed: {response.text}")
