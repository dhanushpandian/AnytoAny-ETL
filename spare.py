

import streamlit as st
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
import os
from dotenv import load_dotenv

# Load Gemini API key
load_dotenv()

llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)

# Function to generate ETL code using Gemini
def generate_etl_code(source, target, transformations):
    prompt = f"""
You are a Python data engineer. Write a complete, executable ETL script that does the following:

1. Connect to the **source** database:
   - Type: {source['type']}
   - Host: {source['host']}
   - Port: {source['port']}
   - Username: {source['user']}
   - Password: {source['password']}
   - Database Name: {source['database']}
   - Table/Collection: {source['object']}

2. Extract the data.

3. Apply the following transformations:
   - {transformations}

4. Load the transformed data into the **target** database:
   - Type: {target['type']}
   - Host: {target['host']}
   - Port: {target['port']}
   - Username: {target['user']}
   - Password: {target['password']}
   - Database Name: {target['database']}
   - Table/Collection: {target['object']}

Use the appropriate Python libraries (e.g., psycopg2 for PostgreSQL, pymongo for MongoDB, sqlalchemy, pandas). Ensure:
- Connection setup
- Error handling
- Transformation logic
- Inserting into the target

Output only the complete Python code.
"""
    response = llm.invoke([HumanMessage(content=prompt)])
    return response.content

# UI Title
st.title("🔁 Any-to-Any AI ETL Code Generator (with Full Credentials)")

st.subheader("🔹 Source Database")
source_type = st.selectbox("Source DB Type", ["PostgreSQL", "MySQL", "MongoDB", "SQLite", "Snowflake", "Other"])
source_host = st.text_input("Source Host (e.g., localhost or cluster.mongodb.net)")
source_port = st.text_input("Source Port")
source_user = st.text_input("Source Username")
source_password = st.text_input("Source Password", type="password")
source_db = st.text_input("Source Database Name")
source_object = st.text_input("Source Table / Collection")

st.subheader("🔸 Target Database")
target_type = st.selectbox("Target DB Type", ["PostgreSQL", "MySQL", "MongoDB", "SQLite", "Snowflake", "Other"])
target_host = st.text_input("Target Host")
target_port = st.text_input("Target Port")
target_user = st.text_input("Target Username")
target_password = st.text_input("Target Password", type="password")
target_db = st.text_input("Target Database Name")
target_object = st.text_input("Target Table / Collection")

st.subheader("🛠️ Data Transformation Rules")
transformations = st.text_area("Describe the transformations", placeholder="e.g., Drop nulls, convert 'created_at' from UTC to IST...")

# Button to generate ETL code
if st.button("🚀 Generate ETL Code"):
    if not all([
        source_type, source_host, source_port, source_user, source_password, source_db, source_object,
        target_type, target_host, target_port, target_user, target_password, target_db, target_object,
        transformations
    ]):
        st.warning("Please fill out all fields.")
    else:
        source = {
            "type": source_type,
            "host": source_host,
            "port": source_port,
            "user": source_user,
            "password": source_password,
            "database": source_db,
            "object": source_object
        }
        target = {
            "type": target_type,
            "host": target_host,
            "port": target_port,
            "user": target_user,
            "password": target_password,
            "database": target_db,
            "object": target_object
        }
        with st.spinner("Generating complete ETL code using Gemini..."):
            etl_code = generate_etl_code(source, target, transformations)
        st.success("✅ ETL Code generated!")
        st.code(etl_code, language="python")
        st.download_button("📥 Download ETL Script", etl_code, "etl_script.py", mime="text/x-python")

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
