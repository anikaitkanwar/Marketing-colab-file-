import streamlit as st
import json
from datetime import date

from saara_data import run_pipeline

st.set_page_config(page_title="Marketing Spend Dashboard", layout="wide")

st.title("📊 Marketing Spend → Revenue Dashboard")
st.caption("Dynamic Inputs → Run pipeline → Preview output → Push to Google Sheet")

# ---------------- Sidebar inputs ----------------
st.sidebar.header("⚙️ Inputs")

report_date = st.sidebar.date_input("Report date", value=date.today())
delay = st.sidebar.number_input("Delay (days)", min_value=1, max_value=365, value=90)

user_name = st.sidebar.text_input("User Name", value="Anikait")

spreadsheet_url = st.sidebar.text_input(
    "Google Sheet URL",
    value="https://docs.google.com/spreadsheets/d/13mH0fgE6tdWmR6PwoVvtqRyudvW2nAJl-Af8XQa3GG8/edit?usp=sharing"
)

worksheet_name = st.sidebar.text_input("Worksheet name", value="Last 3 Months")

category = st.sidebar.text_input("Category filter", value="Finance")

st.sidebar.divider()
st.sidebar.subheader("Database Config")

host = st.sidebar.text_input("Host", value="data-analysis-db-ro-postgresql-blr1-73858-do-user-13062511-0.m.db.ondigitalocean.com")
database = st.sidebar.text_input("Database", value="test")
db_user = st.sidebar.text_input("DB User", value="doadmin")
password = st.sidebar.text_input("DB Password", value="AVNS_zuOg83f71JBINpat9pi", type="password")
port = st.sidebar.text_input("Port", value="25060")

st.sidebar.divider()
st.sidebar.subheader("Google Service Account Key")

json_key_str = st.sidebar.text_area(
    "Paste service account json (dict format)",
    height=250,
    value=""
)

run_btn = st.sidebar.button("🚀 Run Pipeline")


# ---------------- Main execution ----------------
if run_btn:
    if not spreadsheet_url:
        st.error("Google Sheet URL required.")
        st.stop()

    if not json_key_str.strip():
        st.error("Google service account json_key is required.")
        st.stop()

    try:
        json_key = json.loads(json_key_str)
    except Exception:
        st.error("Invalid JSON. Paste service account key in valid JSON format.")
        st.stop()

    with st.spinner("Running pipeline… querying DB, mapping spends, uploading to Google Sheets…"):
        try:
            df = run_pipeline(
                report_date_input=str(report_date),
                delay=int(delay),
                user_name=user_name,
                spreadsheet_url=spreadsheet_url,
                category=category,
                worksheet_name=worksheet_name,
                host=host,
                database=database,
                db_user=db_user,
                password=password,
                port=port,
                json_key=json_key
            )

            st.success("✅ Pipeline completed. Data pushed to Google Sheet.")
            st.subheader("🔍 Preview Output Data")
            st.dataframe(df, use_container_width=True)

            st.download_button(
                "⬇️ Download CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="marketing_pipeline_output.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error("❌ Pipeline failed")
            st.exception(e)
