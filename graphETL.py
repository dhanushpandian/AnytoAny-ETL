# langgraph_etl_graph.py

from langgraph.graph import StateGraph, END
from langchain_core.runnables import Runnable
from typing import TypedDict, Literal
from modules.validator import validate_and_fetch_schema
from modules.generator import generate_etl_code
from modules.executor import run_etl_script

# -------- STATE --------
class ETLState(TypedDict):
    source_type: str
    source_creds: dict
    target_type: str
    target_creds: dict
    transformations: str

    src_status: bool
    tgt_status: bool

    src_schema: list
    src_preview: list
    tgt_schema: list
    tgt_preview: list

    create_new: bool
    etl_code: str
    ask_for_edit: Literal["Yes", "No"]
    stdout: str
    stderr: str

# --------- NODES ---------

# Validate Source DB
def src_connection_node(state: ETLState) -> ETLState:
    status, preview, schema = validate_and_fetch_schema(
        state["source_type"], state["source_creds"]
    )
    return {**state, "src_status": status, "src_preview": preview, "src_schema": schema}

# Validate Target DB
def tgt_connection_node(state: ETLState) -> ETLState:
    status, preview, schema = validate_and_fetch_schema(
        state["target_type"], state["target_creds"]
    )
    create_new = preview == []
    return {
        **state,
        "tgt_status": status,
        "tgt_preview": preview,
        "tgt_schema": schema,
        "create_new": create_new,
    }

# Generate ETL Code Node
def generate_code_node(state: ETLState) -> ETLState:
    code = generate_etl_code(
        state["source_type"],
        state["source_creds"],
        state["target_type"],
        state["target_creds"],
        state["transformations"],
        state["src_preview"],
        state["tgt_preview"],
        state["src_schema"],
        state["tgt_schema"],
    )
    return {**state, "etl_code": code, "ask_for_edit": "Yes"}

# Show code and ask if user wants to edit
def show_code_node(state: ETLState) -> ETLState:
    print("\n🔧 Generated ETL Code:\n", state["etl_code"])
    ask = input("Do you want to improve the ETL code? (Yes/No): ").strip()
    return {**state, "ask_for_edit": ask}

# Execute ETL
def execute_etl_node(state: ETLState) -> ETLState:
    stdout, stderr = run_etl_script(state["etl_code"])
    return {**state, "stdout": stdout, "stderr": stderr}

# -------- DECISION FUNCTIONS --------

# Decide whether to re-generate code
def should_regenerate(state: ETLState) -> str:
    return "generate" if state["ask_for_edit"] == "Yes" else "execute"

# -------- GRAPH BUILDING --------

workflow = StateGraph(ETLState)

workflow.add_node("src_connection", src_connection_node)
workflow.add_node("tgt_connection", tgt_connection_node)
workflow.add_node("generate", generate_code_node)
workflow.add_node("show_code", show_code_node)
workflow.add_node("execute", execute_etl_node)

workflow.set_entry_point("src_connection")

workflow.add_edge("src_connection", "tgt_connection")
workflow.add_edge("tgt_connection", "generate")
workflow.add_edge("generate", "show_code")
workflow.add_conditional_edges("show_code", should_regenerate, {
    "generate": "generate",
    "execute": "execute"
})
workflow.add_edge("execute", END)

graph = workflow.compile()

print("=========================Graph Compiled=======================================")

# graph.invoke({
#     "source_type": "PostgreSQL",
#     "source_creds": {...},
#     "target_type": "MySQL",
#     "target_creds": {...},
#     "transformations": "Convert 'created_at' to ISO format and rename 'name' to 'username'"
# })

# graph.invoke({
#     "source_type": "MongoDB",
#     "source_creds": {
#         "uri": "mongodb://localhost:27017",
#         "database": "mydatabase",
#         "collection": "customers"
#     },
#     "target_type": "MySQL",
#     "target_creds": {
#         "host": "localhost",
#         "port": 3306,
#         "username": "root",
#         "password": "admin@123",
#         "database": "dash",
#         "table": "cust_graph"
#     },
#     "transformations": "Convert 'created_at' to ISO format and rename 'name' to 'username'"
# })
