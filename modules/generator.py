from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
import os
from dotenv import load_dotenv

load_dotenv()
llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)

def strip_code_block(text):
    if text.startswith("```python"):
        text = text[len("```python"):].lstrip()
    if text.endswith("```"):
        text = text[:-3].rstrip()
    return text


def generate_etl_code(source_type, source_creds, target_type, target_creds, transformations, src_preview, tgt_preview):
    prompt = f'''
You are a Python data engineer. Write a full ETL script only without explaination to:

1. Connect to source DB ({source_type}) using:
   {source_creds}

2. Extract data.

3. Apply transformations:
   - {transformations}

4. Load into target DB ({target_type}) using:
   {target_creds}

Use appropriate libraries (psycopg2, pymysql, pymongo, pyodbc, sqlite3, pandas) and approriate parameters for each type , there might be many unwanted credentials given to u, 
u need to use only the ones u need and understand the use case. Include imports and connection handling. use the below data as sample for the table
sourcedb: {src_preview}
and 
destination u need to create a new table for the above requiremnts and then insert
'''
    response = llm.invoke([HumanMessage(content=prompt)])
    return strip_code_block(response.content)
