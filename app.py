import streamlit as st
import requests
import time

# Databricks credentials from Streamlit secrets
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

# --- UI Header ---
st.markdown("<h1 style='text-align: center; font-size: 48px;'>Lake Shift</h1>", unsafe_allow_html=True)

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

# --- Start Button ---
if uploaded_file is not None and st.button("🚀 Start"):
    # Read file as bytes
    file_bytes = uploaded_file.read()

    # Define the volume path (adjust catalog/schema/volume name)
    volume_path = f"/Volumes/ashit_garg/project1/project1/{uploaded_file.name}"

    # Upload file to Volumes
    url = f"{host}/api/2.0/fs/files{volume_path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }
    response = requests.put(url, headers=headers, data=file_bytes)

    if response.status_code in [200, 201, 204]:
        st.success(f"✅ Uploaded to {volume_path}")

        # Trigger the Databricks Job
        run_url = f"{host}/api/2.1/jobs/run-now"
        payload = {
            "job_id": job_id,
            "notebook_params": {"file_path": volume_path}
        }
        run_response = requests.post(run_url, headers={"Authorization": f"Bearer {token}"}, json=payload)

        if run_response.status_code == 200:
            run_id = run_response.json().get("run_id")
            st.success(f"🚀 Job triggered successfully! Run ID: {run_id}")

            # Poll job status
            status_url = f"{host}/api/2.1/jobs/runs/get?run_id={run_id}"
            with st.spinner("⏳ Waiting for job to complete..."):
                while True:
                    status_resp = requests.get(status_url, headers={"Authorization": f"Bearer {token}"})
                    state = status_resp.json().get("state", {})
                    life_cycle = state.get("life_cycle_state")
                    result_state = state.get("result_state")

                    if life_cycle == "TERMINATED":
                        if result_state == "SUCCESS":
                            st.success("✅ Job completed successfully!")
                        else:
                            st.error(f"❌ Job failed with state: {result_state}")
                        break
                    time.sleep(5)

            # --- Fix: Get executed notebook path ---
            tasks = status_resp.json().get("tasks", [])
            notebook_path = None
            if tasks and "notebook_task" in tasks[0]:
                notebook_path = tasks[0]["notebook_task"].get("notebook_path")

            # Export notebook if available
            if notebook_path:
                export_url = f"{host}/api/2.0/workspace/export"
                params = {"path": notebook_path, "format": "JUPYTER"}
                export_resp = requests.get(export_url, headers={"Authorization": f"Bearer {token}"}, params=params)

                if export_resp.status_code == 200:
                    notebook_bytes = export_resp.content
                    st.download_button(
                        label="📥 Download Executed Notebook (.ipynb)",
                        data=notebook_bytes,
                        file_name="output.ipynb",
                        mime="application/x-ipynb+json"
                    )
                else:
                    st.error(f"❌ Notebook export failed: {export_resp.status_code} - {export_resp.text}")
            else:
                st.warning("⚠️ Could not find executed notebook path in job run response.")

        else:
            st.error(f"❌ Job trigger failed: {run_response.status_code} - {run_response.text}")

    else:
        st.error(f"❌ Upload failed: {response.status_code} - {response.text}")
