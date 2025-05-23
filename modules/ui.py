import streamlit as st
import pandas as pd

def db_credential_input(prefix, db_type):
    creds = {}
    if db_type in ["PostgreSQL", "MySQL", "MSSQL"]:
        creds["host"] = st.text_input(f"{prefix} Host")
        creds["port"] = st.text_input(f"{prefix} Port")
        creds["user"] = st.text_input(f"{prefix} Username")
        creds["password"] = st.text_input(f"{prefix} Password", type="password")
        creds["database"] = st.text_input(f"{prefix} Database Name")
        creds["table"] = st.text_input(f"{prefix} Table Name")
    elif db_type == "MongoDB":
        creds["uri"] = st.text_input(f"{prefix} Mongo URI")
        creds["database"] = st.text_input(f"{prefix} Database Name")
        creds["collection"] = st.text_input(f"{prefix} Collection Name")
    elif db_type == "SQLite":
        creds["file_path"] = st.text_input(f"{prefix} SQLite File Path")
        creds["table"] = st.text_input(f"{prefix} Table Name")
    return creds

def display_schema_preview(role, preview, schema, role_color="gray"):
    if preview and schema:
        st.subheader(f"🔍 {role} Preview", divider=role_color)
        st.text("📘 Schema:")
        st.json(schema)

        if preview:
            st.text("🧪 Sample Rows:")
            if isinstance(preview, list) and isinstance(preview[0], dict):
                df = pd.DataFrame(preview)
            else:
                df = pd.DataFrame(preview, columns=[col["name"] for col in schema])
            st.dataframe(df)

def transformation_input():
    st.subheader("✏️ Define Transformation Rules")
    return st.text_area("Describe the transformations:", placeholder="e.g., Drop nulls, convert timestamps, etc.")

def code_editor_section(code):
    st.subheader("📝 Generated ETL Script")
    return st.text_area("Edit the code below (optional):", value=code, height=400)

def editable_code_section(code):
    return st.text_area("Edit the ETL Python code below", value=code, height=400)


def render_db_ui(prefix, db_type):
    creds = {}

    if db_type in ["PostgreSQL", "MySQL", "MSSQL"]:
        creds["host"] = st.text_input(f"{prefix} Host")
        creds["port"] = st.text_input(f"{prefix} Port")
        creds["user"] = st.text_input(f"{prefix} User")
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