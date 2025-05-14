# etl_app.py
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="numpy")

import streamlit as st
# from etl_utils.ui import render_db_ui, display_schema_preview, editable_code_section
from modules.ui import render_db_ui, display_schema_preview, editable_code_section
from modules.generator import generate_etl_code
from modules.executor import run_etl_script_remote_password
# app.py
from modules.validator import validate_and_fetch_schema
from modules.validator import validate_db_connection
from modules.executor import run_etl_script 

# app.py
st.set_page_config(page_title=" Smart ETL", layout="wide")
st.title("🔁Smart ETL Workflow")

# --- Step 1: Select Source and Target DB Types ---
db_types = ["PostgreSQL", "MySQL", "MSSQL", "MongoDB", "SQLite"]

col1, col2 = st.columns(2)
with col1:
    st.header("Source Database")
    source_type = st.selectbox("Source DB Type", db_types, key="src_type")
    source_creds = render_db_ui("Source", source_type)

with col2:
    st.header("Target Database")
    target_type = st.selectbox("Target DB Type", db_types, key="tgt_type")
    target_creds = render_db_ui("Target", target_type)

# --- Step 2: Validate connections and preview schema ---
if st.button("🔍 Validate and Preview Schema"):
    src_status, src_preview ,src_schema = validate_and_fetch_schema(source_type, source_creds)
    tgt_status, tgt_preview , tgt_schema = validate_and_fetch_schema(target_type, target_creds)
    #print(tgt_preview,tgt_status)
    print(source_type,source_creds,target_type,target_creds)
    if tgt_preview ==[]:
        print("-------------------Table dosent exists----------------")
    if src_status and tgt_status:
        st.success("Both connections successful! Previewing schemas:")
        col3, col4 = st.columns(2)
        with col3:
            display_schema_preview("Source", src_preview, src_schema,"green")
        with col4:
            display_schema_preview("Target", tgt_preview, tgt_schema,"orange")
    else:
        st.error("❌ Connection failed. Please check credentials.")


# --- Step 3: Input transformation rules and generate code ---
st.subheader("🛠️ Describe Transformations")
transformations = st.text_area("What changes should be made to the data?", placeholder="e.g., Change column 'created_at' format, drop rows where salary is null...")
    
if st.button("🧠 Generate ETL Code"):
    src_status, src_preview ,src_schema = validate_and_fetch_schema(source_type, source_creds)
    tgt_status, tgt_preview , tgt_schema = validate_and_fetch_schema(target_type, target_creds)
    etl_code = generate_etl_code(source_type, source_creds, target_type, target_creds, transformations, src_preview, tgt_preview,src_schema,tgt_schema)
    st.session_state["etl_code"] = etl_code

# --- Step 4: Show editable ETL code and allow execution ---

if "etl_code" in st.session_state:
    st.subheader("📝 Review and Edit Generated Code")
    edited_code = editable_code_section(st.session_state["etl_code"])

    if st.button("▶️ Run ETL Script LOCAL"):
        stdout, stderr = run_etl_script(edited_code)
        st.success("✅ ETL process completed successfully!")
    # c1,c2=st.columns([1,3])
    ssh_host = st.text_input("Host (e.g., 192.168.1.100)", placeholder="Enter IP or domain")
    ssh_user = st.text_input("Username", placeholder="e.g., ubuntu")
    ssh_password = st.text_input("Password", type="password")
    if st.button("▶️ Run ETL Script SSH"):
        stdout, stderr = run_etl_script_remote_password(edited_code,ssh_user,ssh_host,ssh_password)
        st.success("✅ ETL process completed successfully!")
        if stdout:
            st.text(stdout)



    st.download_button("📥 Download ETL Script", edited_code, "etl_script.py", mime="text/x-python")

