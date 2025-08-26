import requests
import streamlit as st
import time
import json
from datetime import datetime

# ---------- CONFIG ----------
HOST = "https://dbc-1385039b-b177.cloud.databricks.com"
JOB_ID = "507145734504441"
TOKEN = "dapi81bfbcee432414d88ca60fa9f83efc02"
DBFS_BASE_PATH = "/Volumes/ashit_garg/project1/project1"

HEADERS = {"Authorization": f"Bearer {TOKEN}"}


# ---------- FUNCTION TO UPLOAD TO DATABRICKS VOLUME ----------
def upload_to_databricks(uploaded_file):
    file_bytes = uploaded_file.read()
    volume_path = f"{DBFS_BASE_PATH}/{uploaded_file.name}"
    url = f"{HOST}/api/2.0/fs/files{volume_path}"

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/octet-stream"
    }

    resp = requests.put(url, headers=headers, data=file_bytes)

    if resp.status_code in [200, 201, 204]:
        st.info(f"✅ Uploaded to {volume_path}")
        return volume_path
    else:
        st.error(f"❌ Upload failed: {resp.status_code} - {resp.text}")
        return None


# ---------- FUNCTION TO RUN NOTEBOOK ----------
def run_databricks_notebook(xml_input_path: str):
    url = f"{HOST}/api/2.1/jobs/run-now"
    payload = {
        "job_id": int(JOB_ID),
        "notebook_params": {"xml_input": xml_input_path}
    }

    resp = requests.post(url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    run_id = resp.json()["run_id"]

    st.write(f"🚀 Job started with run_id: {run_id}")

    # Poll until job finishes
    while True:
        status_url = f"{HOST}/api/2.1/jobs/runs/get?run_id={run_id}"
        r = requests.get(status_url, headers=HEADERS)
        r.raise_for_status()
        state = r.json()["state"]["life_cycle_state"]
        st.write(f"📡 Job status: {state}")
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
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {"name": "python", "version": "3.9"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return json.dumps(notebook, indent=2)


# ---------- STREAMLIT UI ----------
st.title("🚀 Databricks XML → Notebook Converter")

uploaded_file = st.file_uploader("Upload a .txt or .xml file", type=["txt", "xml"])
out_name = st.text_input("Output filename (.ipynb)", value="converted.ipynb")

if st.button("Run Notebook & Generate File", type="primary"):
    if uploaded_file is None:
        st.warning("Please upload a file first.")
        st.stop()

    with st.spinner("📂 Uploading file to Databricks Volume..."):
        dbfs_path = upload_to_databricks(uploaded_file)

    if dbfs_path:
        with st.spinner(f"⚡ Running Databricks job with input file: {dbfs_path}"):
            result = run_databricks_notebook(dbfs_path)

        # Debug: show raw job result
        st.write("🔍 Raw job result:", result)

        # Extract notebook stdout (print output)
        db_output = result.get("notebook_output", {}).get("result")

        if not db_output:
            db_output = "⚠️ No notebook output found. Check if your Databricks notebook is printing results."

        ipynb_content = build_ipynb_from_output(db_output)

        st.success("🎉 Notebook executed and .ipynb file generated! Download below 👇")
        st.download_button(
            "Download file",
            data=ipynb_content.encode("utf-8"),
            file_name=out_name.strip(),
            mime="application/x-ipynb+json",
        )
