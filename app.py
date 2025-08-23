import streamlit as st
from datetime import datetime
import json

st.set_page_config(page_title="Upload/Paste → .ipynb", page_icon="🧩")
st.title("Upload or Paste Text → Download a .ipynb File")

mode = st.radio("Choose input method:", ["Upload file", "Paste text"], horizontal=True)

final_text = ""
if mode == "Upload file":
    up = st.file_uploader("Upload a .txt or .xml file", type=["txt", "xml"])
    if up is not None:
        final_text = up.read().decode("utf-8", errors="ignore")
        st.caption("Preview of uploaded content:")
        st.text_area("Preview", final_text, height=200)
else:
    final_text = st.text_area("Paste your text here:", height=200)

out_name = st.text_input("Output filename (.ipynb)", value="converted.ipynb")

def build_ipynb_file(text: str) -> str:
    """Builds a minimal .ipynb structure with one code cell containing the text."""
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    f"# Auto-generated at {datetime.utcnow().isoformat()}Z\n",
                    "DATA = '''" + text.replace("'''", "'''\"\"\"'''") + "'''\n",
                    "print('Received input of', len(DATA), 'characters')\n",
                ],
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "version": "3.9"
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return json.dumps(notebook, indent=2)

if st.button("Create .ipynb file", type="primary"):
    if not final_text.strip():
        st.warning("Please upload a file or paste some text first.")
        st.stop()
    if not out_name.strip().endswith(".ipynb"):
        st.warning("Output file name must end with .ipynb")
        st.stop()

    ipynb_json = build_ipynb_file(final_text)

    st.success("Your .ipynb file is ready! Download below 👇")
    st.download_button(
        "Download file",
        data=ipynb_json.encode("utf-8"),
        file_name=out_name.strip(),
        mime="application/x-ipynb+json",
    )
