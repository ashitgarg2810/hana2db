import streamlit as st
import requests
import time
import json

# Databricks credentials
host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]  # Your existing job id

headers = {"Authorization": f"Bearer {token}"}

st.title("📂 Upload & Run Databricks Job")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

if uploaded_file is not None:
    # Save to volume path
    file_bytes = uploaded_file.read()
    file_path = f"/Volumes/ashit_garg/project1/project1/{uploaded_file.name}"

    upload_url = f"{host}/api/2.0/fs/files{file_path}"
    resp = requests.put(upload_url, headers=headers, data=file_bytes)

    if resp.status_code == 200:
        st.success(f"✅ File uploaded: {file_path}")

        # --- Trigger Job ---
        run_url = f"{host}/api/2.1/jobs/run-now"
        payload = {
            "job_id": job_id,
            "notebook_params": {"input_path": file_path}
        }
        run = requests.post(run_url, headers=headers, json=payload)

        if run.status_code != 200:
            st.error(f"❌ Job trigger failed: {run.text}")
        else:
            run_id = run.json()["run_id"]
            st.info(f"🚀 Job started (run_id: {run_id})")

            # --- Poll until job finishes ---
            status_url = f"{host}/api/2.1/jobs/runs/get"
            state = "PENDING"
            while state not in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                time.sleep(5)
                check = requests.get(status_url, headers=headers, params={"run_id": run_id})
                state = check.json()["state"]["life_cycle_state"]
                st.write(f"⏳ Job status: {state}")

            final_state = check.json()["state"]["result_state"]
            if final_state == "SUCCESS":
                st.success("✅ Job finished successfully")

                # --- Collect notebook output ---
                output_url = f"{host}/api/2.1/jobs/runs/export"
                out = requests.get(output_url, headers=headers, params={"run_id": run_id})
                if out.status_code == 200:
                    content = out.json()["views"][0]["content"]

                    # Write as .ipynb
                    file_name = uploaded_file.name.replace(".xml", ".ipynb")
                    with open(file_name, "w", encoding="utf-8") as f:
                        f.write(content)

                    with open(file_name, "rb") as f:
                        st.download_button("⬇️ Download Notebook", f, file_name)
                else:
                    st.error(f"❌ Failed to fetch job output: {out.text}")
            else:
                st.error(f"❌ Job failed: {final_state}")

    else:
        st.error(f"❌ Upload failed: {resp.text}")
