import streamlit as st
import requests
import time

host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

headers = {"Authorization": f"Bearer {token}"}

st.markdown("<h1 style='text-align: center; font-size: 48px;'>Lake Shift</h1>", unsafe_allow_html=True)

if st.button("🚀 Start"):
    # 1. Trigger job run
    run_url = f"{host}/api/2.1/jobs/run-now"
    run_resp = requests.post(run_url, headers=headers, json={"job_id": job_id})
    run_resp.raise_for_status()
    run_id = run_resp.json()["run_id"]
    st.write(f"✅ Job started with run_id: {run_id}")

    # 2. Poll for completion
    run_status_url = f"{host}/api/2.1/jobs/runs/get"
    while True:
        status_resp = requests.get(run_status_url, headers=headers, params={"run_id": run_id})
        status_resp.raise_for_status()
        state = status_resp.json()["state"]["life_cycle_state"]
        if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break
        time.sleep(5)

    # 3. Get all tasks
    run_data = status_resp.json()
    tasks = run_data.get("tasks", [])

    if not tasks:
        st.error("⚠️ No tasks found in job run.")
    else:
        for task in tasks:
            task_run_id = task["run_id"]

            # 4. Fetch output per task
            output_url = f"{host}/api/2.1/jobs/runs/get-output"
            output_resp = requests.get(output_url, headers=headers, params={"run_id": task_run_id})
            if output_resp.status_code == 200:
                output = output_resp.json()
                notebook_output = output.get("notebook_output", {}).get("result", "")
                st.subheader(f"Task: {task['task_key']}")
                st.code(notebook_output if notebook_output else "No output", language="python")
            else:
                st.error(f"❌ Could not fetch output for task {task['task_key']}: {output_resp.text}")
