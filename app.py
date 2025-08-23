import requests
import streamlit as st
import time
import tempfile
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.sdk.service import DbfsService
import json
from datetime import datetime

# Databricks credentials
DB_HOST = "https://<your-databricks-workspace-url>"
DB_TOKEN = "<your-token>"
DB_JOB_ID = "<your-job-id>"


# --- Upload file to DBFS ---
def upload_to_dbfs(file, dbfs_path):
    api_client = ApiClient(host=DB_HOST, token=DB_TOKEN)
    dbfs = DbfsService(api_client)
    content = file.read()
    dbfs.put(dbfs_path, content, overwrite=True)
    return dbfs_path


# --- Run Databricks Job ---
def run_databricks_notebook(dbfs_path: str):
    url = f"{DB_HOST}/api/2.1/jobs/run-now"
    headers = {"Authorization": f"Bearer {DB_TOKEN}"}
    payload = {
        "job_id": int(DB_JOB_ID),
        "notebook_params": {
            "xml_path": dbfs_path   # pass only path, not full XML
        }
    }

    resp = requests.post(url, headers=headers, json=payload)
    print("STATUS:", resp.status_code)
    print("RESPONSE:", resp.text)
    resp.raise_for_status()
    run_id = resp.json()["run_id"]

    # Poll until job finishes
    while True:
        status_url = f"{DB_HOST}/api/2.1/jobs/runs/get?run_id={run_id}"
        r = requests.get(status_url, headers=headers)
        r.raise_for_status()
        state = r.json()["state"]["life_cycle_state"]
        if state == "TERMINATED":
            break
        time.sleep(5)

    # Fetch output
    output_url = f"{DB_HOST}/api/2.1/jobs/runs/get-output?run_id={run_id}"
    out_resp = requests.get(output_url, headers=headers)
    out_resp.raise_for_status()
    return out_resp.json()


# --- Build IPYNB ---
def build_ipynb_from_output(output: str) -> str:
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    f"# Auto-generated at {datetime.utcnow().isoformat()}Z\n",
                    output + "\n"
                ],
            }
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.9"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return json.dumps(notebook, indent=2)


# --- Streamlit UI ---
st.title("XML → Databricks Notebook Runner")

uploaded_file = st.file_uploader("Upload a .txt or .xml file containing XML", type=["txt", "xml"])
out_name = st.text_input("Output filename (.ipynb)", value="converted.ipynb")

if st.button("Run Notebook & Generate File", type="primary"):
    if uploaded_file is None:
        st.warning("Please upload a file first.")
        st.stop()

    # Save uploaded file to DBFS
    dbfs_path = f"/FileStore/xml_uploads/{uploaded_file.name}"
    upload_to_dbfs(uploaded_file, dbfs_path)

    # Run Databricks job with DBFS path
    with st.spinner("Running on Databricks..."):
        result = run_databricks_notebook(dbfs_path)

    db_output = result.get("notebook_output", {}).get("result", "No output found")
    ipynb_content = build_ipynb_from_output(db_output)

    st.success("Notebook executed and .ipynb file generated! Download below 👇")
    st.download_button(
        "Download file",
        data=ipynb_content.encode("utf-8"),
        file_name=out_name.strip(),
        mime="application/x-ipynb+json",
    )
