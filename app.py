import streamlit as st
import requests
import time

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

st.title("📂 Upload & Run Databricks Job")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

if uploaded_file is not None:
    # Step 1: Upload file to DBFS
    file_bytes = uploaded_file.read()
    dbfs_path = f"/FileStore/{uploaded_file.name}"

    upload_url = f"{host}/api/2.0/dbfs/put"
    headers = {"Authorization": f"Bearer {token}"}
    data = {"path": dbfs_path, "overwrite": "true"}
    files = {"contents": file_bytes}

    resp = requests.post(upload_url, headers=headers, data=data, files=files)
    
    if resp.status_code == 200:
        st.success(f"✅ File uploaded to {dbfs_path}")

        # Step 2: Trigger the Databricks Job with uploaded file path
        run_url = f"{host}/api/2.1/jobs/run-now"
        payload = {
            "job_id": job_id,
            "notebook_params": {"file_path": dbfs_path}
        }
        run_resp = requests.post(run_url, headers=headers, json=payload)

        if run_resp.status_code == 200:
            run_id = run_resp.json()["run_id"]
            st.success(f"🚀 Job triggered! Run ID: {run_id}")

            # Step 3: Poll until completion
            status_url = f"{host}/api/2.1/jobs/runs/get"
            while True:
                status_resp = requests.get(status_url, headers=headers, params={"run_id": run_id})
                run_state = status_resp.json()["state"]["life_cycle_state"]

                if run_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                    result_state = status_resp.json()["state"].get("result_state", "UNKNOWN")
                    st.write(f"📊 Job finished with state: {result_state}")
                    break
                time.sleep(5)
        else:
            st.error(f"❌ Failed to trigger job: {run_resp.text}")
    else:
        st.error(f"❌ Failed to upload file: {resp.text}")
