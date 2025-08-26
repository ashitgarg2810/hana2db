import streamlit as st
import requests

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]   # Store your job ID in secrets

st.title("📂 Upload File & Run Databricks Job")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

if uploaded_file is not None:
    # Read file as bytes
    file_bytes = uploaded_file.read()

    # Define the volume path (adjust catalog/schema/volume name)
    volume_path = f"/Volumes/ashit_garg/project1/project1/{uploaded_file.name}"

    # API endpoint to upload file
    url = f"{host}/api/2.0/fs/files{volume_path}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }

    # Upload file
    response = requests.put(url, headers=headers, data=file_bytes)

    if response.status_code in [200, 201, 204]:
        st.success(f"✅ Uploaded to {volume_path}")

        # Trigger the Databricks Job with file path as parameter
        run_url = f"{host}/api/2.1/jobs/run-now"
        payload = {
            "job_id": job_id,
            "notebook_params": {
                "file_path": volume_path
            }
        }

        run_response = requests.post(run_url, headers={"Authorization": f"Bearer {token}"}, json=payload)

        if run_response.status_code == 200:
            run_id = run_response.json().get("run_id")
            st.success(f"🚀 Job triggered successfully! Run ID: {run_id}")
        else:
            st.error(f"❌ Job trigger failed: {run_response.status_code} - {run_response.text}")

    else:
        st.error(f"❌ Upload failed: {response.status_code} - {response.text}")
