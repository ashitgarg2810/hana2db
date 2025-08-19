import streamlit as st
from datetime import datetime

st.set_page_config(page_title="Upload/Paste → .py", page_icon="🧩")
st.title("Upload or Paste Text → Download a .py File")

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

out_name = st.text_input("Output filename (.py)", value="converted.py")

def build_python_file(text: str) -> str:
    # 🔁 This is where you’d transform the text if you want.
    # For now we just embed it into a Python script.
    body = (
        f"# Auto-generated at {datetime.utcnow().isoformat()}Z\n"
        f"# Replace this function with your real processing later.\n\n"
        f"DATA = {text!r}\n\n"
        f"def main():\n"
        f"    print('Received input of', len(DATA), 'characters')\n"
        f"    # TODO: do something with DATA\n\n"
        f"if __name__ == '__main__':\n"
        f"    main()\n"
    )
    return body

if st.button("Create .py file", type="primary"):
    if not final_text.strip():
        st.warning("Please upload a file or paste some text first.")
        st.stop()
    if not out_name.strip().endswith(".py"):
        st.warning("Output file name must end with .py")
        st.stop()

    py_code = build_python_file(final_text)

    st.success("Your .py file is ready! Download below 👇")
    st.download_button(
        "Download file",
        data=py_code,
        file_name=out_name.strip(),
        mime="text/x-python",
    )
