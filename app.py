import streamlit as st
from datetime import date
from saara_data import run_pipeline

st.set_page_config(page_title="Marketing Dashboard", layout="wide")
st.title("📊 Marketing Spend Dashboard")

# Sidebar Inputs
st.sidebar.header("Inputs")

report_date = st.sidebar.date_input("Report Date", value=date.today())
delay = st.sidebar.number_input("Delay (days)", min_value=1, max_value=365, value=90)

user_name = st.sidebar.text_input("User Name", value="Anikait")
spreadsheet_url = st.sidebar.text_input("Google Sheet URL", value="url")
category = st.sidebar.text_input("Category", value="Finance")
worksheet_name = st.sidebar.text_input("Worksheet Name", value="Last 3 Months")

st.sidebar.header("DB Config")
host = st.sidebar.text_input("Host", value="h")
database = st.sidebar.text_input("Database", value="d")
db_user = st.sidebar.text_input("DB User", value="u")
password = st.sidebar.text_input("DB Password", value="p", type="password")
port = st.sidebar.text_input("Port", value="25060")

# For now keep json_key empty in UI since your test pipeline returns dummy output
json_key = {}

# Run Button
if st.sidebar.button("🚀 Run Pipeline"):
    with st.spinner("Running pipeline..."):
        result = run_pipeline(
            report_date_input=str(report_date),
            delay=int(delay),
            user_name=user,
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

    st.success("✅ Pipeline executed successfully!")
    st.write("Output:")
    st.json(result)

else:
    st.info("👈 Fill inputs on the left and click **Run Pipeline**")
