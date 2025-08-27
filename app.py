import streamlit as st
import requests
import time
import json
import os

Databricks credentials from Streamlit secrets

host = st.secrets["DATABRICKS_HOST"]
token = st.secrets["DATABRICKS_TOKEN"]
job_id = st.secrets["DATABRICKS_JOB_ID"]

--- UI Header ---

st.markdown("<h1 style='text-align: center; font-size: 48px;'>Lake Shift</h1>", unsafe_allow_html=True)

uploaded_file = st.file_uploader("Choose a file", type=["txt", "xml"])

--- Start Button ---

if uploaded_file is not None and st.button("🚀 Start"):
# Read file as bytes
file_bytes = uploaded_file.read()

# Define the volume path  
volume_path = f"/Volumes/ashit_garg/project1/project1/{uploaded_file.name}"  

# Upload file to Volumes  
url = f"{host}/api/2.0/fs/files{volume_path}"  
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"}  
response = requests.put(url, headers=headers, data=file_bytes)  

if response.status_code in [200, 201, 204]:  
    st.success(f"✅ Uploaded to {volume_path}")  

    # Trigger the Databricks Job  
    run_url = f"{host}/api/2.1/jobs/run-now"  
    payload = {"job_id": job_id, "notebook_params": {"file_path": volume_path}}  
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

        # --- Fetch job output ---  
        output_url = f"{host}/api/2.1/jobs/runs/get-output?run_id={run_id}"  
        output_resp = requests.get(output_url, headers={"Authorization": f"Bearer {token}"})  

        if output_resp.status_code == 200:  
            output_json = output_resp.json()  
            result_text = output_json.get("notebook_output", {}).get("result", "")  

            if result_text:  
                # Build minimal Jupyter notebook structure  
                notebook_json = {  
                    "cells": [  
                        {  
                            "cell_type": "code",  
                            "execution_count": 1,  
                            "metadata": {},  
                            "outputs": [],  
                            "source": result_text.splitlines(True)  
                        }  
                    ],  
                    "metadata": {  
                        "kernelspec": {  
                            "display_name": "Python 3",  
                            "language": "python",  
                            "name": "python3"  
                        },  
                        "language_info": {  
                            "name": "python",  
                            "pygments_lexer": "ipython3"  
                        }  
                    },  
                    "nbformat": 4,  
                    "nbformat_minor": 5  
                }  

                # Build output file name  
                base_name, _ = os.path.splitext(uploaded_file.name)  
                out_file = f"{base_name}_cnv.ipynb"  

                # Offer download  
                st.download_button(  
                    label=f"📥 Download {out_file}",  
                    data=json.dumps(notebook_json, indent=2),  
                    file_name=out_file,  
                    mime="application/x-ipynb+json"  
                )  
            else:  
                st.warning("⚠️ No result text found in notebook output.")  
        else:  
            st.error(f"❌ Could not fetch job output: {output_resp.status_code} - {output_resp.text}")  

    else:  
        st.error(f"❌ Job trigger failed: {run_response.status_code} - {run_response.text}")  

else:  
    st.error(f"❌ Upload failed: {response.status_code} - {response.text}")
