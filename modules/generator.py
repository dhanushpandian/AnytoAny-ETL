from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
import os
from dotenv import load_dotenv

load_dotenv()
llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)

def generate_etl_code(source_type, source_creds, target_type, target_creds, transformations):
    prompt = f'''
You are a Python data engineer. Write a full ETL script to:

1. Connect to source DB ({source_type}) using:
   {source_creds}

2. Extract data.

3. Apply transformations:
   - {transformations}

4. Load into target DB ({target_type}) using:
   {target_creds}

Use appropriate libraries (psycopg2, pymysql, pymongo, pyodbc, sqlite3, pandas). Include imports and connection handling.
'''
    response = llm.invoke([HumanMessage(content=prompt)])
    return response.content
