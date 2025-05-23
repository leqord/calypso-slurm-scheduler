#!/usr/bin/env python3

import sys
import math
import json
import logging
import argparse
import subprocess
import re
import shutil
from pathlib import Path
from datetime import datetime
from incar import IncarFile
import threading
import time
import os
import sys


class VaspExecutionError(Exception):
    pass


class VaspJob:
    def __init__(self,
                 workdir: Path,
                 inputdir: Path,
                 initial_structure_filepath: Path,
                 logger: logging.Logger,
                 task_cmd: str = "mpirun vasp_std",
                 ml_train: bool = False,
                 ml_refit: bool = False,
                 ml_predict: bool = False,
                 ml_input: Path = None,
                 ml_output: Path = None,
                 ):
        self.logger = logger
        self.task_cmd = task_cmd
        self.workdir = workdir.resolve()
        self.inputdir = inputdir.resolve()
        self.initial_structure_filepath = initial_structure_filepath.resolve()


        if not self.initial_structure_filepath.is_file():
            raise FileNotFoundError(f"Начальный файл структуры не найден: {self.initial_structure_filepath}")
        potcar_path = self.inputdir / "POTCAR"
        if not potcar_path.is_file():
            raise FileNotFoundError(f"Файл POTCAR не найден в {self.inputdir}")

        self.incar_files = sorted(
            self.inputdir.glob("INCAR_*"),
            key=lambda f: int(re.search(r'INCAR_(\d+)', f.name).group(1))
        )
        if not self.incar_files:
            raise FileNotFoundError(f"Не найдено ни одного файла INCAR_* в {self.inputdir}")
        
        if ml_predict and (ml_train or ml_refit):
            self.logger.warning(f"Конфликт: одновременно активны флаг ml_predict и ml_train/ml_refit; все функции МО отключены")

            self.ml_train = False
            self.ml_refit = False
            self.ml_predict = False
        else:
            self.ml_train = ml_train
            self.ml_refit = ml_refit
            self.ml_predict = ml_predict


        self.ml_input = ml_input.resolve() if ml_input is not None else None
        self.ml_output = ml_output.resolve() if ml_output is not None else None

        if self.ml_output is not None:
            self.ml_output.mkdir(parents=True, exist_ok=True)
        
        self.ml_ab_files = []
        self.ml_ff_files = []

        if self.ml_input is not None and self.ml_input.exists():
            self.ml_ab_files = sorted(
                self.ml_input.glob("ML_AB_*"),
                key=lambda f: int(re.search(r'ML_AB_(\d+)', f.name).group(1))
            )
            self.ml_ff_files = sorted(
                self.ml_input.glob("ML_FF_*"),
                key=lambda f: int(re.search(r'ML_FF_(\d+)', f.name).group(1))
            )
        else:
            self.logger.warning(f"Директория входных файлов для MLFF {str(self.ml_input)} не задана или не существует")


        self.workdir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Рабочая директория задачи: {self.workdir}")

        self.poscar_original = self.workdir / "POSCAR_ORIGINAL"
        shutil.copy(self.initial_structure_filepath, self.poscar_original)
        self.logger.info(f"Скопирован исходный файл структуры в {self.poscar_original}")


    def configure_incar_for_ml(self) -> None:
        self.logger.debug(f"Конфигурирую INCAR для включения MLFF")
        return None
    

    def monitor_and_restart(self, 
                            cmd, 
                            log_file_path, 
                            cwd,
                            incar_file: IncarFile,
                            timeout=300, 
                            check_string="starting setup",
                            ):
        ml_enabled = incar_file.get("ML_LMLFF", False)

        if ml_enabled:
            self.logger.info(f"Машинное обучение подключено, наблюдаю за ходом задачи...")

        for i in range(3):
            self.logger.info(f"Запуск '{cmd}' в cwd={cwd}, попытка {i}")
            with open(log_file_path, "w") as logfile:
                process = subprocess.Popen(cmd,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT,
                                        text=True,
                                        cwd=cwd,
                                        bufsize=1,
                                        universal_newlines=True)

                start_time = time.time()
                found_check_string = False

                def reader():
                    nonlocal found_check_string
                    for line in process.stdout:
                        logfile.write(line)
                        if check_string in line:
                            found_check_string = True
                        logfile.flush()

                thread = threading.Thread(target=reader)
                thread.start()

                if not ml_enabled:
                    self.logger.info(f"Ожидаю задачу...")
                    returncode = process.wait()
                    thread.join()
                    return returncode


                while True:
                    time.sleep(5)
                    elapsed = time.time() - start_time

                    if process.poll() is not None:
                        thread.join()
                        return process.returncode

                    if elapsed > timeout and not found_check_string:
                        self.logger.warning(f"Таймаут {timeout}с достигнут, отключение МО и переход задачи в нестандартный режим...")
                        incar_file.delete("ML_LMLFF")
                        incar_file.delete("ML_MODE")
                        open(cwd / "CUSTOM", 'a').close()
                        self.logger.warning(f"Завершаю работу задания до дальнеёшего перезапуска планировщиком!")

                        current_job_slurm_id = os.environ['SLURM_JOB_ID']

                        if current_job_slurm_id is None:
                            raise Exception(f"Не удалось определить slurm id: не задана переменная окружения SLURM_JOB_ID")

                        self.logger.warning(f"Пытаюсь завершить задание {current_job_slurm_id}...")
                        params = ["scancel", current_job_slurm_id]
                        subprocess.run(
                            params,
                            capture_output=True, 
                            text=True, 
                            check=True,
                            cwd=cwd,
                        )

                        sys.exit(1)

                        #process.terminate()
                        #process.kill()
                        #thread.join(10)
                        #process.wait() 
                        # NOTE: какой безобразный ужас...
                        break 
                
                    


    def run(self) -> None:
        for i, incar_file in enumerate(self.incar_files, start=1):
            step_dir = self.workdir / f"step_{i}"
            step_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Создана директория для этапа {i}: {step_dir}")

            custom_dest = step_dir / "CUSTOM"
            incar_name = incar_file.name
            incar_dest = step_dir / "INCAR"

            if "SCF" in incar_file.name:
                self.logger.info(f"Этап {incar_file.name} выполняется БЕЗ машинного обучения.")

            if not custom_dest.is_file():
                shutil.copy(incar_file, incar_dest)
                self.logger.info(f"Этап {i}: {incar_file.name} скопирован в {incar_dest}")

                incar_file = IncarFile(incar_dest)
                ml_ab  = self.ml_input / f"ML_AB_{i}"
                ml_abn = self.ml_input / f"ML_ABN_{i}"
                ml_ab_target = step_dir / "ML_AB"
                ml_ff_target = step_dir / "ML_FF"

                if self.ml_train and not "SCF" in incar_name:
                    if ml_abn.is_file():
                        self.logger.debug(f"{ml_abn} -> {ml_ab_target}")
                        shutil.copy(ml_abn, ml_ab_target)
                    elif ml_ab.is_file():
                        self.logger.debug(f"{ml_ab} -> {ml_ab_target}")
                        shutil.copy(ml_ab, ml_ab_target)
                    else:
                        self.logger.warning(f"Нет входного файла ML_ABN_{i}/ML_AB_{i}, начинаем с нуля")
                    
                    incar_file.set("ML_LMLFF", True)
                    incar_file.set("ML_MODE", "train")
                    # NOTE: не забыть добавить параметр, увеличивающий колво референсных структур

                if self.ml_refit and not "SCF" in incar_name:
                    if ml_abn.is_file():
                        self.logger.debug(f"{ml_abn} -> {ml_ab_target}")
                        shutil.copy(ml_abn, ml_ab_target)
                    elif ml_ab.is_file():
                        self.logger.debug(f"{ml_ab} -> {ml_ab_target}")
                        shutil.copy(ml_ab, ml_ab_target)
                    
                    if ml_abn.is_file() or ml_ab.is_file():
                        incar_file.set("ML_LMLFF", True)
                        incar_file.set("ML_MODE", "refit")
                    else:
                        self.logger.warning(f"Нет входного файла ML_ABN_{i}/ML_AB_{i}, этап refit для INCAR_{i} пропущен")
                        incar_file.set("ML_LMLFF", False)

                if self.ml_predict and not "SCF" in incar_name:
                    ml_ff  = self.ml_input / f"ML_FF_{i}"
                    ml_ffn  = self.ml_input / f"ML_FFN_{i}"

                    if ml_ff.is_file():
                        self.logger.debug(f"{ml_ff} -> {ml_ff_target}")
                        shutil.copy(ml_ff, ml_ff_target)
                    elif ml_ffn.is_file():
                        self.logger.debug(f"{ml_ffn} -> {ml_ff_target}")
                        shutil.copy(ml_ffn, ml_ff_target)
                    
                    if ml_ff.is_file() or ml_ffn.is_file():
                        incar_file.set("ML_LMLFF", True)
                        incar_file.set("ML_MODE", "run")
                    else:
                        self.logger.warning(f"Нет входного файла ML_FFN_{i}/ML_FF_{i}, этап predict для INCAR_{i} пропущен")

                potcar_src = self.inputdir / "POTCAR"
                potcar_dest = step_dir / "POTCAR"
                shutil.copy(potcar_src, potcar_dest)
                self.logger.info(f"Этап {i}: POTCAR скопирован в {potcar_dest}")

                poscar_dest = step_dir / "POSCAR"
                if i == 1:
                    shutil.copy(self.poscar_original, poscar_dest)
                    self.logger.info(f"Этап {i}: POSCAR_ORIGINAL скопирован в {poscar_dest}")
                else:
                    prev_step_dir = self.workdir / f"step_{i-1}"
                    prev_contcar = prev_step_dir / "CONTCAR"
                    if not prev_contcar.is_file():
                        error_message = f"Этап {i}: Не найден CONTCAR в предыдущем этапе ({prev_contcar})"
                        self.logger.error(error_message)
                        raise FileNotFoundError(error_message)
                    shutil.copy(prev_contcar, poscar_dest)
                    self.logger.info(f"Этап {i}: CONTCAR из {prev_contcar} скопирован в {poscar_dest}")

            log_file_path = step_dir / f"vasp_step_{i}.log"

            self.logger.info(f"Этап {i}: запуск команды '{self.task_cmd}' с cwd={step_dir}")
            returncode = self.monitor_and_restart(
                self.task_cmd,
                log_file_path,
                step_dir,
                IncarFile(incar_dest),
                300,
            )
            
            if returncode != 0:
                error_message = f"Этап {i} завершился с ошибкой. Код: {returncode}. Проверьте лог: {log_file_path} и OUTCAR"
                self.logger.error(error_message)

                raise VaspExecutionError(error_message)
            else:
                self.logger.info(f"Этап {i} завершён успешно")
            
            outcar_file = step_dir / "OUTCAR"

            if not outcar_file.is_file():
                error_message = f"Этап {i}: Файл OUTCAR не найден в {step_dir}"
                self.logger.error(error_message)
                raise FileNotFoundError(error_message)
            
            ml_abn_out = step_dir / "ML_ABN"
            ml_ffn_out = step_dir / "ML_FFN"

            if ml_abn_out.is_file() and self.ml_output is not None:
                shutil.copy(ml_abn_out, self.ml_output / f"ML_ABN_{i}")
            
            if ml_ffn_out.is_file() and self.ml_output is not None:
                shutil.copy(ml_ffn_out, self.ml_output / f"ML_FFN_{i}")


def save_status(status_file: Path, status_data: dict) -> None:
    with open(status_file, 'w') as f:
        json.dump(status_data, f, indent=4)


def load_status(status_file: Path) -> dict:
    if status_file.is_file():
        with open(status_file, 'r') as f:
            return json.load(f)
    return {}


def main():
    parser = argparse.ArgumentParser(description="Верхнеуровневый скрипт для последовательного запуска VaspJob задач с изоляцией этапов")
    parser.add_argument("--config", 
                        required=False,
                        default="./config.json",
                        help="Путь к конфигурационному файлу (JSON)")
    args = parser.parse_args()


    config_path = Path(args.config).resolve()
    if not config_path.is_file():
        print(f"Конфигурационный файл не найден: {config_path}", file=sys.stderr)
        sys.exit(1)
    with open(config_path, 'r') as f:
        config: dict = json.load(f)

    try:
        input_dir = Path(config["input_dir"]).resolve()
        poscar_dir = Path(config["poscar_dir"]).resolve()
        global_work_dir = Path(config["global_work_dir"]).resolve()
        vasp_cmd = config["vasp_cmd"]
        status_file = Path(config.get("status_file", global_work_dir / "status.json")).resolve()
        job_prefix = config.get("job_prefix", "job_")

        ml_train = str(config.get("ml_train", "")).lower() == "enable"
        ml_refit = str(config.get("ml_refit", "")).lower() == "enable"
        ml_predict = str(config.get("ml_predict", "")).lower() == "enable"
        #ml_input = config.get("ml_input", None)

    except KeyError as e:
        print(f"Отсутствует ключ в конфигурации: {e}", file=sys.stderr)
        sys.exit(1)

    if ml_train is not None or ml_refit is not None or ml_predict is not None:
        print(f"АКТИВИРОВАНЫ ФУНКЦИИ МАШИННОГО ОБУЧЕНИЯ:\nml_train: {ml_train}\nml_refit: {ml_refit}\nml_predict: {ml_predict}")

    
    # NOTE: не нужно, всё тасуется в input dir
    # if ml_refit or ml_predict:
    #    if ml_input is None or not Path(ml_input).resolve().exists():
    #        print(f"Опции МО ml_refit/ml_predict включены, но директория ml_input {ml_input} не задана или не существует\
    #              хотя должна содержать входные файлы ML_AB(N)/ML_FF(N)", file=sys.stderr)
    #        sys.exit(1)


    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger("TaskLogger")

    logger.info("Запуск верхнеуровневого скрипта для VaspJob задач с изоляцией этапов")
    logger.info(f"Параметры: input_dir = {input_dir}, poscar_dir = {poscar_dir}, global_work_dir = {global_work_dir}")
    logger.info(f"Команда VASP: {vasp_cmd}")

    global_work_dir.mkdir(parents=True, exist_ok=True)

    status_data = load_status(status_file)
    status_data.setdefault("jobs", {})

    poscar_files = sorted(
        poscar_dir.glob("POSCAR_*"),
        key=lambda f: int(re.search(r'POSCAR_(\d+)', f.name).group(1))
    )
    if not poscar_files:
        logger.error(f"Не найдено ни одного файла POSCAR_* в {poscar_dir}")
        sys.exit(1)

    current_ml_input: Path = input_dir

    for poscar_file in poscar_files:
        if status_data["jobs"].get(poscar_file.name) is None:
            status_data["jobs"][poscar_file.name] = {
            "status": "not-finished",
            "timestamp": "",
            "workdir": "",
            "error": "",
            "warning": "",
            }

    for poscar_file in poscar_files:
        logger.debug(f"Входные файлы MLFF берём из {current_ml_input}")

        identifier = re.search(r'POSCAR_(\d+)', poscar_file.name).group(1)
        job_key = poscar_file.name 

        job_status = status_data["jobs"].get(job_key, {}).get("status", "not-finished")
        if  job_status == "success":
            logger.info(f"Задача {job_key} уже успешно завершена, пропускаем")
            continue

        if job_status == "failed":
            logger.info(f"Задача {job_key} во время предыдущего запуска завершилась с ошибкой и будет перезапущена")

        job_workdir = global_work_dir / f"{job_prefix}{identifier}"
        logger.info(f"Запуск задачи {job_key} в каталоге {job_workdir}")

        status_data["jobs"][job_key]["workdir"] =  str(job_workdir)

        try:
            job = VaspJob(workdir=job_workdir,
                          inputdir=input_dir,
                          initial_structure_filepath=poscar_file,
                          logger=logger,
                          task_cmd=vasp_cmd,
                          ml_input=current_ml_input,
                          ml_output=current_ml_input,
                          ml_train=ml_train,
                          ml_refit=ml_refit,
                          ml_predict=ml_predict)
            # NOTE: можно ли ситуативно делать refit?
            job.run()
        except VaspExecutionError as e:
            logger.warning(f"Задача {job_key} столкнулась с проблемой на стороне VASP: {e}, дальнейшие шаги релаксации пропущены")
            status_data["jobs"][job_key]["warning"] = str(e)
        except FileNotFoundError as e:
            logger.error(f"Задача {job_key} столкнулась с ошибкой: {e}")
            status_data["jobs"][job_key]["status"] = "error"
            status_data["jobs"][job_key]["error"] = str(e)
        
        if ml_refit:
            ml_refit = False
            ml_predict = True

        status_data["jobs"][job_key]["timestamp"] = datetime.now().isoformat()
        status_data["jobs"][job_key]["status"] = "success"

        save_status(status_file, status_data)
        logger.info(f"Задача {job_key} завершена, статус сохранен")

    logger.info("Все задачи обработаны.")

if __name__ == "__main__":
    main()
