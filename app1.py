

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
