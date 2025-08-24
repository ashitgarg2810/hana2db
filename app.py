import requests
import streamlit as st
import time
import json
from datetime import datetime
import base64

# ---------- CONFIG ----------
HOST = "https://dbc-1385039b-b177.cloud.databricks.com"
JOB_ID = "507145734504441"
TOKEN = "dapi81bfbcee432414d88ca60fa9f83efc02"
DBFS_BASE_PATH = "/Volumes/ashit_garg/project1/project1"

HEADERS = {"Authorization": f"Bearer {TOKEN}"}


# ---------- FUNCTION TO UPLOAD TO DBFS ----------
def upload_to_dbfs(file_content: bytes, file_name: str) -> str:
    dbfs_path = f"{DBFS_BASE_PATH}/{file_name}"

    # 1. Create file handle (overwrite = True ensures overwrite if exists)
    create_url = f"{HOST}/api/2.0/dbfs/create"
    payload = {"path": dbfs_path, "overwrite": True}
    resp = requests.post(create_url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    handle = resp.json()["handle"]

    # 2. Add block (Databricks requires base64 encoding)
    add_url = f"{HOST}/api/2.0/dbfs/add-block"
    data_b64 = base64.b64encode(file_content).decode("utf-8")
    requests.post(add_url, headers=HEADERS, json={"handle": handle, "data": data_b64}).raise_for_status()

    # 3. Close file
    close_url = f"{HOST}/api/2.0/dbfs/close"
    requests.post(close_url, headers=HEADERS, json={"handle": handle}).raise_for_status()

    return dbfs_path


# ---------- FUNCTION TO RUN NOTEBOOK ----------
def run_databricks_notebook(xml_input_path: str):
    url = f"{HOST}/api/2.1/jobs/run-now"
    payload = {
        "job_id": int(JOB_ID),
        "notebook_params": {
            "xml_input": xml_input_path
        }
    }

    resp = requests.post(url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    run_id = resp.json()["run_id"]

    # Poll until job finishes
    while True:
        status_url = f"{HOST}/api/2.1/jobs/runs/get?run_id={run_id}"
        r = requests.get(status_url, headers=HEADERS)
        r.raise_for_status()
        state = r.json()["state"]["life_cycle_state"]
        if state == "TERMINATED":
            break
        time.sleep(5)

    # Fetch output
    output_url = f"{HOST}/api/2.1/jobs/runs/get-output?run_id={run_id}"
    out_resp = requests.get(output_url, headers=HEADERS)
    out_resp.raise_for_status()
    return out_resp.json()


# ---------- FUNCTION TO BUILD NOTEBOOK ----------
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


# ---------- STREAMLIT UI ----------
uploaded_file = st.file_uploader("Upload a .txt or .xml file", type=["txt", "xml"])
out_name = st.text_input("Output filename (.ipynb)", value="converted.ipynb")

if st.button("Run Notebook & Generate File", type="primary"):
    if uploaded_file is None:
        st.warning("Please upload a file first.")
        st.stop()

    with st.spinner("Uploading file to DBFS..."):
        file_bytes = uploaded_file.read()
        dbfs_path = upload_to_dbfs(file_bytes, uploaded_file.name)

    with st.spinner(f"Running Notebook with input file {dbfs_path}..."):
        result = run_databricks_notebook(dbfs_path)

    # Extract notebook stdout (print output)
    db_output = result.get("notebook_output", {}).get("result", "No output found")

    ipynb_content = build_ipynb_from_output(db_output)

    st.success("Notebook executed and .ipynb file generated! Download below 👇")
    st.download_button(
        "Download file",
        data=ipynb_content.encode("utf-8"),
        file_name=out_name.strip(),
        mime="application/x-ipynb+json",
    )
