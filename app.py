import streamlit as st
import requests
import time

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

volume_path = "/Volumes/ashit_garg/project1/project1/"

st.title("📂 Upload & Run Databricks Job")

# --- Step 1: Upload file (optional) ---
uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])
file_path = None

if uploaded_file:
    file_bytes = uploaded_file.read()
    file_path = f"{volume_path}{uploaded_file.name}"

    st.write(f"📤 Uploading file to: {file_path}")

    url = f"{host}/api/2.0/fs/files{file_path}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.put(url, headers=headers, data=file_bytes)

    if response.status_code == 200:
        st.success(f"✅ File uploaded successfully: {file_path}")
    else:
        st.error(f"❌ Upload failed: {response.text}")

# --- Step 2: Run job (always visible) ---
if st.button("🚀 Run Databricks Job"):
    st.write("Running job... please wait")

    # Use uploaded file path if exists, else a default one
    file_path = file_path or f"{volume_path}default.xml"

    run_url = f"{host}/api/2.1/jobs/run-now"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "job_id": job_id,
        "notebook_params": {"xml_input_path": file_path}
    }

    run_response = requests.post(run_url, headers=headers, json=payload)

    if run_response.status_code != 200:
        st.error(f"❌ Failed to start job: {run_response.text}")
    else:
        run_id = run_response.json()["run_id"]
        st.info(f"Job started with Run ID: {run_id}")

        # Poll until job completes
        result_url = f"{host}/api/2.1/jobs/runs/get?run_id={run_id}"
        while True:
            result_response = requests.get(result_url, headers=headers).json()
            state = result_response["state"]["life_cycle_state"]

            if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                break
            time.sleep(5)

        st.success("✅ Job finished!")

        # --- Generate .ipynb file from job output ---
        notebook_content = result_response.get("notebook_output", {}).get("result", "# No result")
        ipynb_file = "output.ipynb"
        with open(ipynb_file, "w") as f:
            f.write(notebook_content)

        with open(ipynb_file, "rb") as f:
            st.download_button("⬇️ Download Notebook", f, file_name="output.ipynb")
