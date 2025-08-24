import streamlit as st
import requests

host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]

st.title("📂 Upload File to Databricks Volume")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

if uploaded_file is not None:
    file_bytes = uploaded_file.read()
    file_name = uploaded_file.name

    # Path in Unity Catalog Volume
    volume_path = f"/Volumes/ashit_garg/project1/project1/{file_name}"

    headers = {"Authorization": f"Bearer {token}"}

    # Step 1: Start upload session
    start_url = f"{host}/api/2.0/fs/files{volume_path}"
    start_resp = requests.post(start_url, headers=headers)

    if start_resp.status_code != 200:
        st.error(f"❌ Failed to start upload: {start_resp.text}")
    else:
        handle = start_resp.json().get("handle")

        # Step 2: Upload file content
        upload_url = f"{host}/api/2.0/fs/files/{handle}"
        put_resp = requests.put(upload_url, headers=headers, data=file_bytes)

        if put_resp.status_code != 200:
            st.error(f"❌ Failed to upload content: {put_resp.text}")
        else:
            # Step 3: Commit upload
            commit_url = f"{host}/api/2.0/fs/files/{handle}"
            commit_resp = requests.post(commit_url, headers=headers)

            if commit_resp.status_code == 200:
                st.success(f"✅ Uploaded to {volume_path}")
            else:
                st.error(f"❌ Commit failed: {commit_resp.text}")
