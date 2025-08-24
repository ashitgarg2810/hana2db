import streamlit as st
import requests

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]

st.title("📂 Upload File to Databricks Volume")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

if uploaded_file is not None:
    # Read file as bytes
    file_bytes = uploaded_file.read()

    # Define the volume path (adjust catalog/schema/volume name)
    volume_path = f"/Volumes/ashit_garg/project1/project1/{uploaded_file.name}"

    # API endpoint
    url = f"{host}/api/2.0/fs/files{volume_path}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }

    # Upload file
    response = requests.put(url, headers=headers, data=file_bytes)

    # Show result
    if response.status_code == 200:
        st.success(f"✅ Uploaded to {volume_path}")
    else:
        st.error(f"❌ Upload failed: {response.status_code} - {response.text}")
