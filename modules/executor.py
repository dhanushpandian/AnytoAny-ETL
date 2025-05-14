import subprocess
import tempfile

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

import paramiko

def run_etl_script_remote_password(code: str, ssh_user: str, ssh_host: str, ssh_password: str) -> tuple[str, str]:
    print("==========================enetred ssh block")
    try:
        # Save the code temporarily
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(code)
            local_path = f.name
            remote_path = f"/tmp/{f.name.split('/')[-1]}"

        # Setup SSH connection
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=ssh_host, username=ssh_user, password=ssh_password)

        # Use SFTP to upload the file
        sftp = ssh.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()

        # Run the script
        stdin, stdout, stderr = ssh.exec_command(f"python3 {remote_path}")

        out = stdout.read().decode()
        err = stderr.read().decode()

        # Filter warnings
        filtered_err = "\n".join(
            line for line in err.splitlines()
            if "numpy/_core/getlimits.py" not in line
        )

        ssh.close()
        return out.strip(), filtered_err.strip()
    except Exception as e:
        return "", f"❌ SSH Error: {e}"
