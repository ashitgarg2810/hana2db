import streamlit as st
import requests
import time
import base64

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]

# Job details
job_id = st.secrets["DATABRICKS_JOB_ID"]   # <-- store your job id in secrets

st.title("📂 Upload + Run Job + Download Notebook")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

if uploaded_file is not None:
    # Upload to DBFS
    file_bytes = uploaded_file.read()
    file_path = f"/Volumes/ashit_garg/project1/project1/{uploaded_file.name}"

    upload_url = f"{host}/api/2.0/fs/files{file_path}"
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.put(upload_url, headers=headers, data=file_bytes)

    if res.status_code == 200:
        st.success(f"✅ File uploaded to {file_path}")

        # Trigger Databricks job
        run_url = f"{host}/api/2.1/jobs/run-now"
        payload = {
            "job_id": job_id,
            "notebook_params": {"input_path": file_path}
        }
        run_res = requests.post(run_url, headers=headers, json=payload)

        if run_res.status_code == 200:
            run_id = run_res.json()["run_id"]
            st.info(f"🚀 Job triggered (Run ID: {run_id})")

            # Poll until job completes
            status_url = f"{host}/api/2.1/jobs/runs/get"
            while True:
                status_res = requests.get(status_url, headers=headers, params={"run_id": run_id})
                state = status_res.json()["state"]["life_cycle_state"]

                if state in ["TERMINATED", "INTERNAL_ERROR", "SKIPPED"]:
                    result_state = status_res.json()["state"]["result_state"]
                    st.success(f"✅ Job finished with result: {result_state}")
                    break
                else:
                    st.write(f"⏳ Job running... ({state})")
                    time.sleep(5)

            # Get notebook output (export as ipynb)
            export_url = f"{host}/api/2.0/jobs/runs/export"
            export_res = requests.get(export_url, headers=headers, params={"run_id": run_id, "views_to_export": "CODE"})

            if export_res.status_code == 200:
                ipynb_content = export_res.json()["views"][0]["content"]
                ipynb_bytes = base64.b64decode(ipynb_content)

                st.download_button(
                    label="⬇️ Download Notebook (.ipynb)",
                    data=ipynb_bytes,
                    file_name="output_notebook.ipynb",
                    mime="application/x-ipynb+json"
                )
            else:
                st.error(f"❌ Export failed: {export_res.text}")
        else:
            st.error(f"❌ Could not trigger job: {run_res.text}")
    else:
        st.error(f"❌ Upload failed: {res.text}")
