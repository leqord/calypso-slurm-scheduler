import subprocess
import tempfile
import os
from pathlib import Path
from string import Template
from slurm_exceptions import *


# TODO: организовать объектную обёртку и нормальное логирование
# в т.ч. содержимого скрипта slurm

def prepare_job_script(template_path, command):
    with open(template_path, 'r') as f:
        template_content = f.read()
    template = Template(template_content)
    return template.safe_substitute(COMMAND=command)


def submit_job(template_path: Path, cwd: Path, job_name: str) -> int:
    with open(template_path, 'r') as f:
        template_content = f.read()

    script_path = Path(os.path.dirname(os.path.abspath(__file__))) / "task.py"
    script_path.resolve()

    template = Template(template_content)
    script_content = template.safe_substitute(TASK_SCRIPT=script_path) # TODO: <- доработать

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.slurm') as tmp_file:
        tmp_file.write(script_content)
        tmp_file_path = tmp_file.name

    try:
        params = f"sbatch --job-name={job_name} {tmp_file_path}"
        result = subprocess.run(
            params,
            capture_output=True, 
            text=True, 
            check=True,
            cwd=cwd,
            executable='/bin/bash'
        )
        output = result.stdout.strip()
        parts = output.split()
        if len(parts) >= 4 and parts[0] == "Submitted" and parts[1] == "batch" and parts[2] == "job":
            job_id = int(parts[3])
            return job_id
        else:
            raise SlurmSubmissionError(f"Не удалось разобрать вывод sbatch: {output}")
    except subprocess.CalledProcessError as e:
        raise SlurmSubmissionError(f"Ошибка при отправке задания ({params}):\n {e.stderr}") from e
    finally:
        os.remove(tmp_file_path)


def parse_scontrol_output(output):
    info = {}
    for token in output.split():
        if '=' in token:
            key, value = token.split("=", 1)
            info[key] = value
    return info


def get_job_status(job_id):
    """
    :param job_id: Идентификатор задания Slurm (например, 48)
    :return: Строка, описывающая состояние задания (например, "PENDING", "RUNNING", "COMPLETED", "CANCELLED", "UNKNOWN")
    :raises SlurmScontrolError: При возникновении других ошибок выполнения команды scontrol.
    """
    try:
        result = subprocess.run(
            "scontrol show job {job_id}",
            capture_output=True, text=True, check=True,
            executable='/bin/bash'
        )
        output = result.stdout.strip()

        if "slurm_load_jobs error:" in output or "Invalid job id specified" in output:
            return "UNKNOWN"

        job_info = parse_scontrol_output(output)
        return job_info.get("JobState", "UNKNOWN")
    except subprocess.CalledProcessError as e:
        err_msg = e.stderr.strip()
        if "Invalid job id specified" in err_msg:
            return "UNKNOWN"
        raise SlurmScontrolError(f"Ошибка при выполнении scontrol: {err_msg}")


