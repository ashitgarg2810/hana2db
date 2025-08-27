import streamlit as st
import requests
import time

# Databricks credentials from Streamlit secrets
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

# --- UI Header ---
st.markdown("<h1 style='text-align: center; font-size: 48px;'>Lake Shift</h1>", unsafe_allow_html=True)

uploaded_file = st.file_uploader("Upload an XML file", type=["xml"])

if uploaded_file is not None:
    xml_content = uploaded_file.read().decode("utf-8")

    if st.button("Run Migration Job"):
        headers = {"Authorization": f"Bearer {token}"}
        data = {
            "job_id": job_id,
            "notebook_params": {"xml_input": xml_content}
        }

        # --- Run Databricks Job ---
        response = requests.post(f"{host}/api/2.1/jobs/run-now", headers=headers, json=data)
        
        if response.status_code == 200:
            run_id = response.json().get("run_id")
            st.write("✅ Job started. Run ID:", run_id)

            # --- Poll until job finishes ---
            run_status = "PENDING"
            while run_status not in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                time.sleep(5)
                status_response = requests.get(
                    f"{host}/api/2.1/jobs/runs/get?run_id={run_id}",
                    headers=headers
                )
                run_status = status_response.json().get("state", {}).get("life_cycle_state", "")
                st.write("⏳ Job status:", run_status)

            # --- Retrieve job output (only final notebook.exit) ---
            output_response = requests.get(
                f"{host}/api/2.1/jobs/runs/get-output?run_id={run_id}",
                headers=headers
            )

            if output_response.status_code == 200:
                output_data = output_response.json()
                result_text = output_data.get("notebook_output", {}).get("result", "❌ No result found")
            else:
                result_text = f"❌ Could not fetch job output: {output_response.status_code} - {output_response.text}"

            st.subheader("📄 Job Output")
            st.code(result_text, language="python")

        else:
            st.error(f"❌ Failed to start job: {response.status_code} - {response.text}")
