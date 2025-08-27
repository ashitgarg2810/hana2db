import streamlit as st
import requests
import time

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

def run_databricks_job():
    # Trigger job
    run_url = f"{host}/api/2.1/jobs/run-now"
    response = requests.post(run_url, headers=headers, json={"job_id": job_id})
    response.raise_for_status()
    run_id = response.json()["run_id"]

    # Poll until job finishes
    status_url = f"{host}/api/2.1/jobs/runs/get"
    while True:
        r = requests.get(status_url, headers=headers, params={"run_id": run_id})
        r.raise_for_status()
        state = r.json()["state"]["life_cycle_state"]

        if state == "TERMINATED":
            break
        time.sleep(5)

    # ✅ Get the output from the notebook.exit()
    output_url = f"{host}/api/2.1/jobs/runs/get-output"
    r = requests.get(output_url, headers=headers, params={"run_id": run_id})
    r.raise_for_status()
    return r.json().get("notebook_output", {}).get("result", "❌ No output found")

# --- UI ---
st.title("🚀 Run Databricks Job and Fetch Output")

if st.button("Run Job"):
    with st.spinner("Running job on Databricks..."):
        try:
            result = run_databricks_job()
            st.success("✅ Job finished successfully!")
            st.text_area("Notebook Output", result, height=200)
        except Exception as e:
            st.error(f"Failed to fetch output: {e}")
