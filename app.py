import streamlit as st
import requests
import time
import os

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]  # make sure you set this in secrets

st.title("📂 Upload File & Run Databricks Job")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

if uploaded_file is not None:
    # Define the DBFS path
    dbfs_path = f"/dbfs/tmp/{uploaded_file.name}"

    # Upload to DBFS (overwrite if exists)
    st.write("🔄 Uploading file to DBFS...")
    url = f"{host}/api/2.0/dbfs/put"
    headers = {"Authorization": f"Bearer {token}"}
    data = {"path": dbfs_path, "overwrite": "true"}
    files = {"contents": uploaded_file.getvalue()}
    
    resp = requests.post(url, headers=headers, data=data, files=files)
    if resp.status_code == 200:
        st.success(f"✅ File uploaded to {dbfs_path}")
    else:
        st.error(f"❌ Upload failed: {resp.text}")
        st.stop()

    # Run Databricks job immediately with uploaded file path
    st.write("🚀 Triggering Databricks job...")
    run_url = f"{host}/api/2.1/jobs/run-now"
    payload = {
        "job_id": job_id,
        "notebook_params": {"input_path": dbfs_path}
    }
    run_resp = requests.post(run_url, headers=headers, json=payload)

    if run_resp.status_code == 200:
        run_id = run_resp.json().get("run_id")
        st.success(f"✅ Job started (Run ID: {run_id})")

        # Poll for completion
        while True:
            status_url = f"{host}/api/2.1/jobs/runs/get?run_id={run_id}"
            status_resp = requests.get(status_url, headers=headers).json()
            state = status_resp["state"]["life_cycle_state"]

            if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                result_state = status_resp["state"].get("result_state", "UNKNOWN")
                if result_state == "SUCCESS":
                    st.success("🎉 Job completed successfully!")
                else:
                    st.error(f"⚠️ Job failed with state: {result_state}")
                break
            else:
                st.write(f"⏳ Job running... status = {state}")
                time.sleep(5)
    else:
        st.error(f"❌ Job trigger failed: {run_resp.text}")
