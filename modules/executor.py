import subprocess
import tempfile

def run_etl_code(code):
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.py', delete=False) as f:
        f.write(code)
        f.flush()
        result = subprocess.run(["python", f.name], capture_output=True, text=True)
        return result.stdout + '\\n' + result.stderr
        
# def run_etl_script(code: str) -> str:
#     import subprocess
#     import tempfile

#     try:
#         with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
#             f.write(code)
#             temp_path = f.name

#         result = subprocess.run(["python", temp_path], capture_output=True, text=True)
#         return result.stdout + "\n" + result.stderr
#     except Exception as e:
#         return f"❌ Error running script: {e}"

def run_etl_script(code: str) -> tuple[str, str]:
    import subprocess
    import tempfile

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            temp_path = f.name

        result = subprocess.run(["python", temp_path], capture_output=True, text=True)

        # Filter out known numpy warning
        filtered_stderr = "\n".join(
            line for line in result.stderr.splitlines()
            if "numpy/_core/getlimits.py" not in line
        )

        return result.stdout.strip(), filtered_stderr.strip()
    except Exception as e:
        return "", f"❌ Error running script: {e}"
