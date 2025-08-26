import streamlit as st
import requests
import time

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

st.title("📂 Upload & Run Databricks Job")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

file_path = f"/Volumes/ashit_garg/project1/project1/"
if uploaded_file is not None:
    # Upload file to Databricks volume
    file_bytes = uploaded_file.read()
    upload_url = f"{host}/api/2.0/fs/files{file_path}{uploaded_file.name}"

    resp = requests.put(upload_url, headers={"Authorization": f"Bearer {token}"}, data=file_bytes)
    if resp.status_code == 200:
        st.success(f"✅ File uploaded: {uploaded_file.name}")
    else:
        st.error(f"❌ Upload failed: {resp.text}")

# --- Always trigger job, regardless of upload ---
if st.button("🚀 Run Databricks Job"):
    run_url = f"{host}/api/2.1/jobs/run-now"
    payload = {
        "job_id": job_id,
        "notebook_params": {
            "input_path": file_path + (uploaded_file.name if uploaded_file else "default.xml")
        }
    }

    run = requests.post(run_url, headers={"Authorization": f"Bearer {token}"}, json=payload)
    if run.status_code != 200:
        st.error(f"❌ Failed to start job: {run.text}")
    else:
        run_id = run.json()["run_id"]
        st.info(f"🔄 Job started: Run ID {run_id}")

        # Poll until job completes
        while True:
            time.sleep(5)
            status_url = f"{host}/api/2.1/jobs/runs/get?run_id={run_id}"
            status = requests.get(status_url, headers={"Authorization": f"Bearer {token}"}).json()
            state = status["state"]["life_cycle_state"]
            result = status["state"].get("result_state")

            if state == "TERMINATED":
                if result == "SUCCESS":
                    st.success("✅ Job completed successfully!")
                else:
                    st.error(f"❌ Job failed: {status}")
                break
            elif state in ["INTERNAL_ERROR", "SKIPPED"]:
                st.error(f"⚠️ Job ended unexpectedly: {status}")
                break

        # Example: fetch notebook output (optional)
        output_url = f"{host}/api/2.1/jobs/runs/export?run_id={run_id}&views_to_export=NOTEBOOK"
        output = requests.get(output_url, headers={"Authorization": f"Bearer {token}"})
        if output.status_code == 200:
            with open("job_output.ipynb", "wb") as f:
                f.write(output.content)
            st.download_button("📥 Download Notebook", data=output.content, file_name="job_output.ipynb")
