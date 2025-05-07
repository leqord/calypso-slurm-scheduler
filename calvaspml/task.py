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


        self.ml_input: Path = ml_input.resolve() if ml_input is not None else None
        self.ml_output: Path = ml_output.resolve() if ml_output is not None else None

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

        
        if self.ml_train or self.ml_refit:
            if not self.ml_ab_files:
                self.logger.warning(f"Активировано обучение/переобучение MLFF, но нет ни одного ML_AB_* файла в")
        
        if self.ml_predict and not self.ml_predict:
            self.logger.warning(f"Активировано использование обученных MLFF, но нет ни одного ML_FF_* файла в")


        self.workdir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Рабочая директория задачи: {self.workdir}")

        self.poscar_original = self.workdir / "POSCAR_ORIGINAL"
        shutil.copy(self.initial_structure_filepath, self.poscar_original)
        self.logger.info(f"Скопирован исходный файл структуры в {self.poscar_original}")


    def configure_incar_for_ml(self) -> None:
        self.logger.debug(f"Конфигурирую INCAR для включения MLFF")
        return None


    def run(self) -> None:
        for i, incar_file in enumerate(self.incar_files, start=1):
            # TODO: брать исходные файлы для ML из input dir (режим predict)
            # TODO: на первой итерации калипсо должен быть полный рандом
            # так что делаем популяцию больше и обучаем на первой итерации

            # TODO: если для данного этапа N имеется ML_AB_N, То скопировать его сюжа
            # после выполнения шага, если в INCAR_N был train, то скопировать ML_ABN в ML_AB
            # КУДА-ТО, где его сможет взять следующая структура или уже взяла бы эта
            # организовать в main()?
            # входные брать из ml_inputdir, а выходные ML_AB_N копировать с каждого этапа в workdir этого job

            step_dir = self.workdir / f"step_{i}"
            step_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Создана директория для этапа {i}: {step_dir}")

            incar_dest = step_dir / "INCAR"
            shutil.copy(incar_file, incar_dest)
            self.logger.info(f"Этап {i}: {incar_file.name} скопирован в {incar_dest}")

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
            with open(log_file_path, "w") as logfile:
                self.logger.info(f"Этап {i}: запуск команды '{self.task_cmd}' с cwd={step_dir}")
                result = subprocess.run(self.task_cmd, 
                                        stdout=logfile, 
                                        stderr=subprocess.STDOUT, 
                                        text=True, 
                                        cwd=step_dir,
                                        )
            
            if result.returncode != 0:
                error_message = f"Этап {i} завершился с ошибкой. Код: {result.returncode}. Проверьте лог: {log_file_path} и OUTCAR"
                self.logger.error(error_message)

                raise VaspExecutionError(error_message)
            else:
                self.logger.info(f"Этап {i} завершён успешно")
            
            outcar_file = step_dir / "OUTCAR"

            if not outcar_file.is_file():
                error_message = f"Этап {i}: Файл OUTCAR не найден в {step_dir}"
                self.logger.error(error_message)
                raise FileNotFoundError(error_message)


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

    except KeyError as e:
        print(f"Отсутствует ключ в конфигурации: {e}", file=sys.stderr)
        sys.exit(1)


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
        logger.debug(f"Входные файлы MLFF берём из {current_ml_input}")

        identifier = re.search(r'POSCAR_(\d+)', poscar_file.name).group(1)
        job_key = poscar_file.name 

        job_status = status_data["jobs"].get(job_key, {}).get("status")
        if  job_status == "success":
            logger.info(f"Задача {job_key} уже успешно завершена, пропускаем")
            continue

        if job_status == "failed":
            logger.info(f"Задача {job_key} во время предыдущего запуска завершилась с ошибкой и будет перезапущена")

        job_workdir = global_work_dir / f"{job_prefix}{identifier}"
        logger.info(f"Запуск задачи {job_key} в каталоге {job_workdir}")

        status_data["jobs"][job_key] = {
            "status": "success",
            "timestamp": "",
            "workdir": str(job_workdir),
            "error": "",
            "warning": "",
        }

        try:
            job = VaspJob(workdir=job_workdir,
                          inputdir=input_dir,
                          initial_structure_filepath=poscar_file,
                          logger=logger,
                          task_cmd=vasp_cmd,
                          ml_input=current_ml_input,
                          ml_output=job_workdir,
                          ml_train=ml_train,
                          ml_refit=ml_refit,
                          ml_predict=ml_predict)
            job.run()
            current_ml_input = job_workdir
        except VaspExecutionError as e:
            logger.warning(f"Задача {job_key} столкнулась с проблемой на стороне VASP: {e}, дальнейшие шаги релаксации пропущены")
            status_data["jobs"][job_key]["warning"] = str(e)
        except Exception as e:
            logger.error(f"Задача {job_key} завершилась с ошибкой: {e}")
            status_data["jobs"][job_key]["status"] = "failed"
            status_data["jobs"][job_key]["error"] = str(e)

        status_data["jobs"][job_key]["timestamp"] = datetime.now().isoformat()
        save_status(status_file, status_data)
        logger.info(f"Задача {job_key} завершена, статус сохранен")

    logger.info("Все задачи обработаны.")

if __name__ == "__main__":
    main()
