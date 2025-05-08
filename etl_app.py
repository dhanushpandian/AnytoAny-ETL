# etl_app.py
import streamlit as st
from modules.ui import db_credential_input, display_schema_preview, transformation_input, code_editor_section
from modules.validator import validate_connection
from modules.generator import generate_etl_code
from modules.executor import run_etl_code

st.set_page_config(page_title="🔁 Smart ETL Workflow", layout="wide")
st.title("🔁 AI ETL Tool")

# Step 1: DB Selection and Credential Input
st.header("1️⃣ Select Source and Target Databases")
db_types = ["PostgreSQL", "MySQL", "MSSQL", "MongoDB", "SQLite"]

col1, col2 = st.columns(2)
with col1:
    source_type = st.selectbox("Source DB Type", db_types, key="source_db")
    source_creds = db_credential_input("Source", source_type)

with col2:
    target_type = st.selectbox("Target DB Type", db_types, key="target_db")
    target_creds = db_credential_input("Target", target_type)

# Step 2: Validate and Preview
if st.button("🔍 Validate and Preview DBs"):
    src_status, src_schema, src_rows = validate_connection(source_type, source_creds)
    tgt_status, tgt_schema, _ = validate_connection(target_type, target_creds)

    if src_status and tgt_status:
        st.success("✅ Both connections successful!")
        display_schema_preview("Source", src_schema, src_rows)
        display_schema_preview("Target", tgt_schema, [])

        # Step 3: Enter Transformations
        transformations = transformation_input()

        # Step 4: Generate Code
        if st.button("⚙️ Generate ETL Code"):
            generated_code = generate_etl_code(source_type, source_creds, target_type, target_creds, transformations)
            code = code_editor_section(generated_code)

            # Step 5: Run the Code
            if st.button("🚀 Run ETL Now"):
                run_output = run_etl_code(code)
                st.success("✅ ETL Script Executed")
                #st.text(run_output)

            st.download_button("📥 Download Script", code, file_name="etl_script.py", mime="text/x-python")
    else:
        st.error("❌ Connection failed. Check credentials.")
