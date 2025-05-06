import streamlit as st
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Gemini
llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)

# Helper function to render credential fields based on DB type
def render_db_fields(prefix, db_type):
    creds = {}

    if db_type in ["PostgreSQL", "MySQL", "MSSQL"]:
        creds["host"] = st.text_input(f"{prefix} Host")
        creds["port"] = st.text_input(f"{prefix} Port")
        creds["username"] = st.text_input(f"{prefix} Username")
        creds["password"] = st.text_input(f"{prefix} Password", type="password")
        creds["database"] = st.text_input(f"{prefix} Database Name")
        creds["table"] = st.text_input(f"{prefix} Table Name")
    elif db_type == "MongoDB":
        creds["uri"] = st.text_input(f"{prefix} Mongo URI (e.g., mongodb+srv://...)")
        creds["database"] = st.text_input(f"{prefix} Database Name")
        creds["collection"] = st.text_input(f"{prefix} Collection Name")
    elif db_type == "SQLite":
        creds["file_path"] = st.text_input(f"{prefix} SQLite File Path")
        creds["table"] = st.text_input(f"{prefix} Table Name")

    return creds

# Function to generate ETL code prompt
def generate_etl_code_prompt(source_type, source_creds, target_type, target_creds, transformations):
    return f"""
You are a Python data engineer. Write a complete ETL script to:

1. Connect to the **source** database:
   - Type: {source_type}
   - Credentials: {source_creds}

2. Extract data from it.

3. Apply the following transformations:
   - {transformations}

4. Load the transformed data into the **target** database:
   - Type: {target_type}
   - Credentials: {target_creds}

Use the appropriate Python libraries (like `pymongo`, `psycopg2`, `pyodbc`, `sqlalchemy`, `sqlite3`, `pymysql`, etc.) depending on the DB types.
Include all necessary imports and connection handling.
"""

# UI
st.title("🔁 Smart ETL Code Generator (Mongo, MySQL, MSSQL, PostgreSQL, SQLite)")

db_types = ["PostgreSQL", "MySQL", "MSSQL", "MongoDB", "SQLite"]

col1, col2 = st.columns(2)
with col1:
    st.header("Source Database")
    source_type = st.selectbox("Source DB Type", db_types, key="source_db")
    source_creds = render_db_fields("Source", source_type)

with col2:
    st.header("Target Database")
    target_type = st.selectbox("Target DB Type", db_types, key="target_db")
    target_creds = render_db_fields("Target", target_type)

st.subheader("🛠️ Transformation Rules")
transformations = st.text_area("Describe your transformations (natural language)", 
    placeholder="e.g., Convert 'date' to DD-MM-YYYY, remove rows with null salary...")

if st.button("🚀 Generate ETL Code"):
    with st.spinner("Calling Gemini to generate code..."):
        prompt = generate_etl_code_prompt(source_type, source_creds, target_type, target_creds, transformations)
        response = llm.invoke([HumanMessage(content=prompt)])
        st.success("✅ ETL Code Generated")
        st.code(response.content, language="python")
        st.download_button("📥 Download Python Script", response.content, file_name="etl_script.py", mime="text/x-python")
