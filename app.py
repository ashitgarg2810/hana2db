import streamlit as st
import requests
import time

# Databricks credentials from Streamlit secrets
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

headers = {"Authorization": f"Bearer {token}"}

# --- UI Header ---
st.markdown("<h1 style='text-align: center; font-size: 48px;'>Lake Shift</h1>", unsafe_allow_html=True)

# --- Trigger Job ---
if st.button("▶️ Run Databricks Job"):
    # Start job run
    run_url = f"{host}/api/2.1/jobs/run-now"
    response = requests.post(run_url, headers=headers, json={"job_id": job_id})
    response.raise_for_status()
    run_data = response.json()
    parent_run_id = run_data["run_id"]

    st.write(f"✅ Job triggered (parent run_id = {parent_run_id})")

    # Poll for completion
    status_url = f"{host}/api/2.1/jobs/runs/get"
    run_state = "PENDING"
    task_run_id = None

    while run_state not in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        resp = requests.get(status_url, headers=headers, params={"run_id": parent_run_id})
        resp.raise_for_status()
        run_info = resp.json()
        run_state = run_info["state"]["life_cycle_state"]

        # Extract first task run id
        if "tasks" in run_info and run_info["tasks"]:
            task_run_id = run_info["tasks"][0]["run_id"]

        time.sleep(5)

    if task_run_id is None:
        st.error("❌ Could not find task run id")
    else:
        st.success(f"Task finished (task_run_id = {task_run_id})")

        # Get output of that task
        output_url = f"{host}/api/2.1/jobs/runs/get-output"
        output_resp = requests.get(output_url, headers=headers, params={"run_id": task_run_id})
        output_resp.raise_for_status()
        output_data = output_resp.json()

        # This contains dbutils.notebook.exit() value
        if "notebook_output" in output_data:
            exit_value = output_data["notebook_output"].get("result", "")
            st.subheader("📤 Notebook Output")
            st.code(exit_value, language="text")
        else:
            st.error("❌ No notebook output found")
