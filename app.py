import streamlit as st
import requests
import time

# Databricks config
HOST = st.secrets["DATABRICKS_HOST"]
TOKEN = st.secrets["DATABRICKS_TOKEN"]
JOB_ID = st.secrets["DATABRICKS_JOB_ID"]

st.title("📂 Upload & Run Databricks Job")

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

if uploaded_file is not None:
    # Upload file
    file_bytes = uploaded_file.read()
    dbfs_path = f"/Volumes/ashit_garg/project1/project1/{uploaded_file.name}"

    upload_url = f"{HOST}/api/2.0/fs/files{dbfs_path}"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.put(upload_url, headers=headers, data=file_bytes)
    if r.status_code == 200:
        st.success(f"✅ File uploaded: {dbfs_path}")

        # Add a button to trigger the job
        if st.button("🚀 Run Databricks Job"):
            payload = {"job_id": JOB_ID, "notebook_params": {"input_path": dbfs_path}}
            run_url = f"{HOST}/api/2.1/jobs/run-now"
            run_resp = requests.post(run_url, headers=headers, json=payload)

            if run_resp.status_code == 200:
                run_id = run_resp.json()["run_id"]
                st.info(f"▶️ Job started (Run ID: {run_id})")

                # Poll job until completion
                status_url = f"{HOST}/api/2.1/jobs/runs/get?run_id={run_id}"
                state = "RUNNING"
                while state in ("PENDING", "RUNNING"):
                    time.sleep(5)
                    status_resp = requests.get(status_url, headers=headers)
                    state = status_resp.json()["state"]["life_cycle_state"]
                    st.write(f"Job status: {state}")

                st.success("✅ Job completed!")

                # Get output notebook
                output_url = f"{HOST}/api/2.0/jobs/runs/{run_id}/export?views=CODE"
                out_resp = requests.get(output_url, headers=headers)

                if out_resp.status_code == 200:
                    with open("output.ipynb", "wb") as f:
                        f.write(out_resp.content)
                    st.download_button("⬇️ Download Notebook", out_resp.content, "output.ipynb")
                else:
                    st.error(f"⚠️ Could not fetch notebook. Status: {out_resp.status_code}")
            else:
                st.error(f"⚠️ Job trigger failed: {run_resp.text}")
    else:
        st.error(f"⚠️ Upload failed: {r.text}")
