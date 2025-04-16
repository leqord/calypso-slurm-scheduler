#!/usr/bin/env python3
import os
import sys
import json
import logging
import argparse
import subprocess
import re
import shutil
import time
from pathlib import Path
from datetime import datetime



def run_calypso(config, logger):
    calypso_exe = Path(config["calypso_exe"]).resolve()
    calypso_workdir = Path(config["calypso_workdir"]).resolve()
    calypso_workdir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Запуск calypso.x в {calypso_workdir}")

    # Удаляем старые файлы, если они есть
    for pattern in ["OUTCAR_*", "CONTCAR_*", "POSCAR_*"]:
        for f in calypso_workdir.glob(pattern):
            try:
                f.unlink()
                logger.info(f"Удалён старый файл: {f}")
            except Exception as e:
                logger.warning(f"Не удалось удалить {f}: {e}")

    calypso_args = config.get("calypso_args", "")
    cmd = f"{calypso_exe} {calypso_args}"
    result = subprocess.run(cmd, shell=True, cwd=calypso_workdir)
    if result.returncode != 0:
        logger.error(f"calypso.x завершился с ошибкой, код: {result.returncode}")
        raise RuntimeError("Ошибка выполнения calypso.x")
    logger.info("calypso.x выполнен успешно")


def get_generated_poscars(calypso_workdir, logger):
    poscars = sorted(
        calypso_workdir.glob("POSCAR_*"),
        key=lambda f: int(re.search(r'POSCAR_(\d+)', f.name).group(1)) if re.search(r'POSCAR_(\d+)', f.name) else 0
    )
    logger.info(f"Найдено {len(poscars)} файлов POSCAR_* в {calypso_workdir}")
    return poscars


def generate_task_config(config, poscar_files, logger):
    task_config = {}
    poscar_dir = Path(config.get("task_poscar_dir", "task_poscar")).resolve()
    poscar_dir.mkdir(parents=True, exist_ok=True)
    # Очистка директории от предыдущих файлов
    for f in poscar_dir.glob("POSCAR_*"):
        f.unlink()
    for poscar in poscar_files:
        shutil.copy(poscar, poscar_dir / poscar.name)
    logger.info(f"Скопированы POSCAR_* файлы в {poscar_dir}")

    task_config["input_dir"] = config["input_dir"]  # каталог с INCAR_* и POTCAR
    task_config["poscar_dir"] = str(poscar_dir)
    task_config["global_work_dir"] = config["global_work_dir"]
    task_config["vasp_cmd"] = config.get("vasp_cmd", "mpirun vasp_std")
    # Файл статуса, куда task.py запишет информацию о выполнении задач
    default_status = str(Path(config["global_work_dir"]) / "status.json")
    task_config["status_file"] = str(Path(config.get("status_file", default_status)).resolve())
    task_config["job_prefix"] = config.get("job_prefix", "job_")

    task_config_path = Path(config.get("task_config_path", "task_config.json")).resolve()
    with open(task_config_path, "w") as f:
        json.dump(task_config, f, indent=4)
    logger.info(f"Сгенерирован task_config.json: {task_config_path}")
    return task_config_path


def create_sbatch_script(config, task_config_path, logger):
    slurm_template = config.get("slurm_template", 
r"""#!/bin/bash
#SBATCH --mail-user={mail_user}
#SBATCH --mail-type=ALL
#SBATCH --cpus-per-gpu=2
#SBATCH --gres=gpu:1
#SBATCH --time={slurm_time}
#SBATCH --job-name={job_name}
#SBATCH --partition={partition}

python3 {task_py_path} --config {task_config_path}
""")
    mail_user = config.get("mail_user", "example@example.com")
    slurm_time = config.get("slurm_time", "0-48:00:00")
    partition = config.get("partition", "intel-a100-pci4")
    task_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    job_name = config.get("job_name_prefix", "TASK") + "_" + task_id
    task_py_path = Path(config.get("task_py_path", "./task.py")).resolve()

    sbatch_content = slurm_template.format(
        mail_user=mail_user,
        slurm_time=slurm_time,
        job_name=job_name,
        partition=partition,
        task_py_path=task_py_path,
        task_config_path=task_config_path
    )

    slurm_submission_dir = Path(config.get("slurm_submission_dir", ".")).resolve()
    slurm_submission_dir.mkdir(parents=True, exist_ok=True)
    sbatch_filename = f"task_{task_id}.sbatch"
    sbatch_path = slurm_submission_dir / sbatch_filename
    with open(sbatch_path, "w") as f:
        f.write(sbatch_content)
    logger.info(f"Сгенерирован sbatch скрипт: {sbatch_path}")
    return sbatch_path, job_name


def submit_job(sbatch_path, logger):
    cmd = f"sbatch {sbatch_path}"
    logger.info(f"Отправка задания: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Ошибка отправки задания: {result.stderr}")
        raise RuntimeError("Ошибка отправки задания через sbatch")
    stdout = result.stdout.strip()
    logger.info(f"Ответ sbatch: {stdout}")
    match = re.search(r"Submitted batch job (\d+)", stdout)
    if not match:
        raise RuntimeError("Не удалось получить идентификатор задания из вывода sbatch")
    job_id = match.group(1)
    logger.info(f"Задание отправлено, job_id: {job_id}")
    return job_id


def monitor_job(job_id, logger, poll_interval=30):
    logger.info(f"Начало мониторинга задания {job_id}")
    while True:
        result = subprocess.run("squeue --me", shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Ошибка при вызове squeue: {result.stderr}")
            raise RuntimeError("Ошибка вызова squeue")
        if job_id not in result.stdout:
            logger.info(f"Задание {job_id} завершилось")
            break
        else:
            logger.info(f"Задание {job_id} всё ещё выполняется")
        time.sleep(poll_interval)


def check_task_status(task_status_file, logger):
    status_path = Path(task_status_file).resolve()
    if not status_path.is_file():
        raise FileNotFoundError(f"Файл статуса не найден: {status_path}")
    with open(status_path, "r") as f:
        status_data = json.load(f)
    jobs = status_data.get("jobs", {})
    all_success = True
    failed_jobs = []
    for job_key, info in jobs.items():
        if info.get("status") != "success":
            all_success = False
            failed_jobs.append(job_key)
    if all_success:
        logger.info("Все VaspJob задачи завершились успешно")
    else:
        logger.error(f"Следующие задачи завершились с ошибкой: {failed_jobs}")
    return all_success, failed_jobs


def copy_final_files(task_config, logger):
    global_work_dir = Path(task_config["global_work_dir"]).resolve()
    job_prefix = task_config.get("job_prefix", "job_")
    job_dirs = sorted([d for d in global_work_dir.iterdir() if d.is_dir() and d.name.startswith(job_prefix)])
    if not job_dirs:
        raise FileNotFoundError("Не найдены директории задач VaspJob в глобальном рабочем каталоге")
    
    for job_dir in job_dirs:
        step_dirs = sorted(
            [d for d in job_dir.iterdir() if d.is_dir() and d.name.startswith("step_")],
            key=lambda d: int(re.search(r'step_(\d+)', d.name).group(1))
        )
        if not step_dirs:
            raise FileNotFoundError(f"В задаче {job_dir} не найдены поддиректории этапов")
        final_step = step_dirs[-1]
        contcar = final_step / "CONTCAR"
        outcar = final_step / "OUTCAR"
        if not contcar.is_file():
            raise FileNotFoundError(f"Файл CONTCAR не найден в {final_step}")
        if not outcar.is_file():
            raise FileNotFoundError(f"Файл OUTCAR не найден в {final_step}")
        m_match = re.search(rf'{job_prefix}(\d+)', job_dir.name)
        if not m_match:
            raise RuntimeError(f"Не удалось извлечь идентификатор из {job_dir.name}")
        m = m_match.group(1)
        dest_contcar = global_work_dir / f"CONTCAR_{m}"
        dest_outcar = global_work_dir / f"OUTCAR_{m}"
        shutil.copy(contcar, dest_contcar)
        shutil.copy(outcar, dest_outcar)
        logger.info(f"Скопированы файлы из {final_step}: {contcar} -> {dest_contcar}, {outcar} -> {dest_outcar}")


def main():
    parser = argparse.ArgumentParser(
        description="Планировщик: генерация POSCAR_* через calypso.x, формирование конфигурации и управление Slurm заданиями."
    )
    parser.add_argument("--config", required=True, help="Путь к конфигурационному файлу для scheduler.py (JSON)")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    if not config_path.is_file():
        print(f"Конфигурационный файл не найден: {config_path}", file=sys.stderr)
        sys.exit(1)
    with open(config_path, "r") as f:
        config = json.load(f)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger("Scheduler")

    calypso_workdir = Path(config["calypso_workdir"]).resolve()

    while True:
        logger.info("===== Запуск цикла планировщика =====")
        run_calypso(config, logger)
        
        poscar_files = get_generated_poscars(calypso_workdir, logger)
        if not poscar_files:
            logger.info("Новые POSCAR_* файлы не сгенерированы. Вычисления завершены.")
            break
        
        task_config_path = generate_task_config(config, poscar_files, logger)
        
        sbatch_path, job_name = create_sbatch_script(config, task_config_path, logger)
        
        job_id = submit_job(sbatch_path, logger)
        
        monitor_job(job_id, logger)
        
        task_status_file = config.get("status_file", str(Path(config["global_work_dir"]) / "status.json"))
        all_success, failed_jobs = check_task_status(task_status_file, logger)
        
        if not all_success:
            logger.error("Некоторые VaspJob задачи завершились с ошибкой. Перезапуск задания.")
            continue
        
        with open(task_config_path, "r") as f:
            task_config = json.load(f)
        copy_final_files(task_config, logger)
        
        logger.info("Цикл завершён успешно, переходим к следующему запуску calypso.x")
        time.sleep(10) 


if __name__ == "__main__":
    main()
