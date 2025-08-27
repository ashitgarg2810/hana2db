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
