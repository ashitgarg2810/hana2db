import requests
import streamlit as st
import time

def run_databricks_notebook(xml_input: str):
    host = "https://dbc-1385039b-b177.cloud.databricks.com"
    token = "dapi81bfbcee432414d88ca60fa9f83efc02"
    job_id = "507145734504441"


    #host = st.secrets["DATABRICKS_HOST"]
    #token = st.secrets["DATABRICKS_TOKEN"]
    #job_id = st.secrets["DATABRICKS_JOB_ID"]

    url = f"{host}/api/2.1/jobs/run-now"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "job_id": int(job_id),
        "notebook_params": {
            "xml_input": str(xml_input)
        }
    }

    resp = requests.post(url, headers=headers, json=payload)
    print("STATUS:", resp.status_code)#added
    print("RESPONSE:", resp.text)#added
    resp.raise_for_status()
    run_id = resp.json()["run_id"]

    # Poll until job finishes
    while True:
        status_url = f"{host}/api/2.1/jobs/runs/get?run_id={run_id}"
        r = requests.get(status_url, headers=headers)
        r.raise_for_status()
        state = r.json()["state"]["life_cycle_state"]
        if state == "TERMINATED":
            break
        time.sleep(5)

    # Fetch output
    output_url = f"{host}/api/2.1/jobs/runs/get-output?run_id={run_id}"
    out_resp = requests.get(output_url, headers=headers)
    out_resp.raise_for_status()
    return out_resp.json()


import json
from datetime import datetime

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



st.title("XML → Databricks → Download .ipynb")

xml_text = st.text_area("Paste your XML here:", height=200)
out_name = st.text_input("Output filename (.ipynb)", value="converted.ipynb")

if st.button("Run Notebook & Generate File", type="primary"):
    if not xml_text.strip():
        st.warning("Please paste XML first.")
        st.stop()

    with st.spinner("Running on Databricks..."):
        result = run_databricks_notebook(xml_text)

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
