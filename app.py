import streamlit as st
import requests
import time

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

headers = {"Authorization": f"Bearer {token}"}

# --- UI Header ---
st.markdown("<h1 style='text-align: center; font-size: 48px;'>Lake Shift</h1>", unsafe_allow_html=True)

# --- Run Job Button ---
if st.button("▶️ Run Databricks Job"):
    # Trigger job run
    run_url = f"{host}/api/2.1/jobs/run-now"
    payload = {"job_id": job_id}
    run_response = requests.post(run_url, headers=headers, json=payload)

    if run_response.status_code != 200:
        st.error(f"Failed to trigger job: {run_response.text}")
    else:
        run_id = run_response.json()["run_id"]
        st.info(f"Job started with run_id: {run_id}")

        # Poll until job completes
        status_url = f"{host}/api/2.1/jobs/runs/get"
        while True:
            status_response = requests.get(status_url, headers=headers, params={"run_id": run_id})
            run_state = status_response.json()["state"]["life_cycle_state"]

            if run_state in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                break
            time.sleep(5)

        # --- Fetch notebook.exit() value ---
        result_url = f"{host}/api/2.1/jobs/runs/get-output"
        result_response = requests.get(result_url, headers=headers, params={"run_id": run_id})

        if result_response.status_code == 200:
            result_json = result_response.json()
            notebook_output = result_json.get("notebook_output", {}).get("result", "❌ No output returned")
            st.success("✅ Job Completed")
            st.code(notebook_output)
        else:
            st.error(f"❌ Could not fetch job output: {result_response.text}")
