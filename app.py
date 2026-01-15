import streamlit as st
from saara_data import run_pipeline

st.set_page_config(page_title="Saara Data Pipeline", layout="centered")

st.title("📊 Saara Data Automation")
st.caption("Postgres → SQL → Google Sheets")

# ---------------- Inputs ----------------
report_date = st.date_input("Report Date")
delay = st.number_input("Delay (days)", min_value=0, value=1)

user = st.text_input(
    "Your Name",
    placeholder="e.g. Anikait Kanwar"
)

spreadsheet_url = st.text_input(
    "Google Sheet URL",
    placeholder="https://docs.google.com/spreadsheets/..."
)

# ---------------- Run Button ----------------
if st.button("🚀 Run Pipeline"):
    if not user or not spreadsheet_url:
        st.error("Please fill all required fields")
    else:
        with st.spinner("Running data pipeline..."):
            try:
                run_pipeline(
                    report_date_input=str(report_date),
                    delay=delay,
                    user=user,
                    spreadsheet_url=spreadsheet_url
                )
                st.success("✅ Data pushed to Google Sheets successfully!")
            except Exception as e:
                st.error("❌ Pipeline failed")
                st.exception(e)
