import streamlit as st
import requests
import time

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

headers = {"Authorization": f"Bearer {token}"}

def run_job_and_get_output(xml_input: str):
    # Trigger the job
    run_url = f"{host}/api/2.1/jobs/run-now"
    payload = {"job_id": job_id, "notebook_params": {"xml_input": xml_input}}
    run_resp = requests.post(run_url, headers=headers, json=payload)
    run_resp.raise_for_status()
    run_id = run_resp.json()["run_id"]

    # Wait for completion
    run_status_url = f"{host}/api/2.1/jobs/runs/get"
    while True:
        resp = requests.get(run_status_url, headers=headers, params={"run_id": run_id})
        resp.raise_for_status()
        state = resp.json()["state"]["life_cycle_state"]
        if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break
        time.sleep(5)

    # ✅ Fetch task run_id (since jobs can have multiple tasks)
    run_data = resp.json()
    task_run_id = run_data["tasks"][0]["run_id"]   # First task only

    # Get notebook exit value
    output_url = f"{host}/api/2.1/jobs/runs/get-output"
    output_resp = requests.get(output_url, headers=headers, params={"run_id": task_run_id})
    output_resp.raise_for_status()
    output_data = output_resp.json()

    return output_data.get("notebook_output", {}).get("result", "No output found")

# --- UI Header (unchanged) ---
st.markdown("<h1 style='text-align: center; font-size: 48px;'>Lake Shift</h1>", unsafe_allow_html=True)

uploaded_file = st.file_uploader("Upload XML file", type=["xml"])
if uploaded_file is not None:
    xml_input = uploaded_file.read().decode("utf-8")
    with st.spinner("Running job..."):
        try:
            result = run_job_and_get_output(xml_input)
            st.success("✅ Job completed successfully!")
            st.text_area("Notebook Output", result, height=300)
        except Exception as e:
            st.error(f"❌ Could not fetch job output: {e}")
