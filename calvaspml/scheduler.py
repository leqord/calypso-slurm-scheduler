#!/usr/bin/env python3

import os
import json
import argparse
import subprocess
import shutil
import time
import json
import logging
import tempfile
from logging import Logger
from typing import List
from pathlib import Path
from string import Template



class SlurmSubmissionError(Exception):
    pass


class SlurmScontrolError(Exception):
    pass


class CalypsoError(Exception):
    pass


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
        params = ["sbatch", f"--job-name={job_name}", f"{tmp_file_path}"]
        result = subprocess.run(
            params,
            capture_output=True, 
            text=True, 
            check=True,
            #shell=True,
            cwd=cwd,
            #executable='/bin/bash'
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
    try:
        result = subprocess.run(
            ["scontrol", "show", "job", f"{job_id}"],
            capture_output=True, 
            text=True, 
            check=True,
            #shell=True,
            #executable='/bin/bash'
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



class DuplicateFilter(logging.Filter):

    def filter(self, record):
        current_log = (record.module, record.levelno, record.msg)
        if current_log != getattr(self, "last_log", None):
            self.last_log = current_log
            return True
        return False


class SlurmConfig():
    def __init__(self,
                 sbatch_template: Path,
                 job_prefix: str = "cal"):
        self.sbatch_template = sbatch_template.resolve()
        if not self.sbatch_template.is_file():
            raise FileNotFoundError(f"Файл шаблона sbatch не найден по пути:\n{self.sbatch_template}\n{sbatch_template}")
        
        self.job_prefix = job_prefix


class CalypsoScheduler():
    def __init__(self,
                 vasp_cmd: str,
                 calypso_exe: Path,
                 calypso_workdir: Path,
                 tasks_dir: Path,
                 input_dir: Path,
                 slurm_config: SlurmConfig,
                 logger: Logger,
                 ml_train_until: int = 0,
                 ):
        self.calypso_exe = calypso_exe.resolve()
        if not self.calypso_exe.is_file():
            raise FileNotFoundError(f"Исполняемый файл Calypso не обнаружен:\n{self.calypso_exe}\n{calypso_exe}")

        self.calypso_workdir = calypso_workdir.resolve()
        if not self.calypso_workdir.is_dir():
            raise FileNotFoundError(f"Рабочая директория Calypso не найдена:\n{self.calypso_workdir}\n{calypso_workdir}")
        
        calypso_infile = self.calypso_workdir / "input.dat"
        if not calypso_infile.is_file():
            raise FileNotFoundError(f"Входной файл Calypso input.dat не найден в рабочей директории по пути:\n{calypso_infile}")

        self.input_dir = input_dir.resolve()
        if not self.input_dir.is_dir():
            raise FileNotFoundError(f"Директория входных файлов не найдена:\n{self.input_dir}\n{input_dir}")
        
        potcar_file = self.input_dir / "POTCAR"
        if not potcar_file.is_file():
            raise FileNotFoundError(f"Файл POTCAR не найден в директории входных файлов по пути:\n{potcar_file}")
        
        incar_files = list(self.input_dir.glob("INCAR_*"))
        if not incar_files:
            raise FileNotFoundError(f"Не найдено ни одного файла INCAR_* в директории входных файлов:\n{self.input_dir}")
        
        self.tasks_dir = tasks_dir.resolve()
        self.tasks_dir.mkdir(parents=True, exist_ok=True)

        self.slurm_config = slurm_config
        self.vasp_cmd = vasp_cmd
        self.task_status_filename = "status.json"
        self.task_config_filename = "config.json"
        self.task_poscars_subfolder_name = "poscars"
        self.task_slurm_status_filename = "slurm.json"
        self.task_job_prefix = "job_"
        self.ml_train_until = ml_train_until
        self.loop_sleep_seconds = 300
        self.logger = logger


    def check_calypso_generation(self):
        step_file_path = self.calypso_workdir / "step"

        self.logger.debug(f"Проверка поколения Calypso в файле {step_file_path}")

        generation = None
        if step_file_path.is_file():
            with open(step_file_path) as step_file:
                generation = int(step_file.read()) - 1
        
        return generation


    def execute_calypso(self):
        self.logger.debug(f"Запуск calypso {self.calypso_exe} в {self.calypso_workdir}")

        result = subprocess.run(str(self.calypso_exe), shell=True, cwd=self.calypso_workdir, executable='/bin/bash')
        if result.returncode != 0:
            raise CalypsoError(f"Ошибка выполнения calypso.x\nКод: {result.returncode}\nОшибка:\n{result.stderr}\nВывод:\n{result.stdout}")
        return None


    def get_calypso_poscars(self):
        generated_poscars = sorted(self.calypso_workdir.glob('POSCAR_*'), key=lambda x: int(x.name.split('_')[1]))

        if not generated_poscars:
            return None

        return generated_poscars


    def get_task_path_from_id(self, task_id: str):
        return self.tasks_dir / f"task_{task_id}"


    def get_task_slurm_statusfile_from_id(self, task_id: str):
        return self.get_task_path_from_id(task_id) / self.task_slurm_status_filename


    def get_current_slurm_id_from_id(self, task_id: str):
        slurm_status_file = self.get_task_slurm_statusfile_from_id(task_id)

        if not slurm_status_file.is_file():
            return None
        
        with open(slurm_status_file, 'r') as f:
            slurm_status: dict = json.load(f)
        
        if "current_id" in slurm_status.keys():
            return slurm_status["current_id"]
        
        return None


    def check_if_all_task_job_completed_from_id(self, task_id: str) -> bool:
        task_folder = self.get_task_path_from_id(task_id)
        status_file_path = task_folder / self.task_status_filename

        if not status_file_path.is_file():
            return False
        
        with open(status_file_path, 'r') as f:
            status: dict = json.load(f)
        
        if not "jobs" in status.keys():
            raise RuntimeError(f"Файл status.json {status_file_path} не имеет ключа 'jobs'")

        # NOTE: можно выполнять проверку и надёжнее
        for poscar in status["jobs"].keys():
            poscar_state: dict = status["jobs"][poscar]

            if not "status" in poscar_state.keys():
                raise RuntimeError(f"В файле status.json {status_file_path} для {poscar} нет ключа 'status'")
            
            poscar_status = poscar_state["status"]

            if poscar_status != 'success':
                return False

        return True


    def copy_output_from_task_from_id(self, task_id: str):
        task_folder = self.get_task_path_from_id(task_id)

        for job_folder in task_folder.glob("job_*"):
            if not job_folder.is_dir():
                continue

            try:
                job_number = job_folder.name.split("_")[1]
            except IndexError:
                self.logger.warning(f"Не удалось извлечь номер из папки {job_folder}")
                continue
            
            step_folders = [folder for folder in job_folder.glob("step_*") if folder.is_dir()]
            if not step_folders:
                self.logger.warning(f"Нет подпапок 'step_*' в {job_folder}")
                continue
            
            def extract_step_number(step_path: Path) -> int:
                return int(step_path.name.split("_")[1])
            
            max_step_folder = max(step_folders, key=extract_step_number)

            target_outcar = self.calypso_workdir / f"OUTCAR_{job_number}"
            target_contcar = self.calypso_workdir / f"CONTCAR_{job_number}"
            target_poscar = self.calypso_workdir / f"POSCAR_{job_number}"

            poscar_source = job_folder / "POSCAR_ORIGINAL"
            if not poscar_source.exists():
                raise FileNotFoundError(f"Файл {poscar_source} не найден")

            self.logger.debug(f"{poscar_source} -> {target_poscar}")
            shutil.copy(poscar_source, target_poscar)

            contcar_source = max_step_folder / "CONTCAR"
            if not contcar_source.exists():
                self.logger.error(f"Отсутствует файл CONTCAR: {contcar_source}, файл НЕ копируется")
                #continue
                #raise FileNotFoundError(f"Файл {contcar_source} не найден")
            else:
                with open(contcar_source, 'r') as file_obj:
                    file_content = file_obj.read().strip()
                    
                    #if not file_content:
                    #    self.logger.warning(f"Файл {contcar_source} пуст, пропускаю")
                    #    continue
                    #else:
                    self.logger.debug(f"{contcar_source} -> {target_contcar}")
                    shutil.copy(contcar_source, target_contcar)
            
            outcar_source = max_step_folder / "OUTCAR"
            if not outcar_source.exists():
                raise FileNotFoundError(f"Файл {outcar_source} не найден")
            
            self.logger.debug(f" {outcar_source} -> {target_outcar}")
            shutil.copy(outcar_source, target_outcar)

        return None


    def submit_slurm_task_from_id(self, task_id: str, job_name: str) -> int:
        self.logger.info(f"Запуск задания slurm для {task_id}")
        task_path = self.get_task_path_from_id(task_id)

        slurm_id = submit_job(self.slurm_config.sbatch_template, task_path, f"{self.slurm_config.job_prefix}_{job_name}")

        self.update_slurm_id_from_id(task_id, str(slurm_id))

        self.logger.info(f"Задание {task_id} с id {slurm_id} запущено, ожидаю завершения")
        return slurm_id
    

    def update_slurm_id_from_id(self, task_id: str, slurm_id: str) -> None:
        task_path = self.get_task_path_from_id(task_id)
        slurm_status_file = task_path / "slurm.json"

        if slurm_status_file.is_file():
            with open(slurm_status_file, 'r') as f:
                slurm_config = json.load(f)
            
            slurm_config["current_id"] = slurm_id
        else:
            slurm_config = {"current_id": slurm_id}

        with open(slurm_status_file, 'w') as f:
            json.dump({ "current_id": slurm_id}, f)


    def prepare_task_from_poscars(self, 
                                  poscars: List[Path], 
                                  task_id: str, 
                                  ml_train: bool = False,
                                  ml_refit: bool = False,
                                  ml_predict: bool = False):
        task_path = self.get_task_path_from_id(task_id)
        task_poscars_path = task_path / self.task_poscars_subfolder_name

        self.logger.debug(f"Подготавливается задача {task_id} в {task_path}")

        task_poscars_path.mkdir(parents=True, exist_ok=True)

        for poscar_file in poscars:
            self.logger.debug(f"{poscar_file} -> {task_poscars_path}")

            shutil.copy(poscar_file, task_poscars_path)
        
        task_config = {
            "input_dir": str(self.input_dir),
            "poscar_dir": str(task_poscars_path),
            "global_work_dir": str(task_path),
            "vasp_cmd": self.vasp_cmd,
            "status_file": str(task_path / self.task_status_filename),
            "job_prefix": self.task_job_prefix
        }

        if ml_train:
            self.logger.debug(f"Планировщик активирует обучение для задачи {task_id}")
            task_config["ml_train"] = "enable"
        
        if ml_refit:
            self.logger.debug(f"Планировщик активирует переобучение для задачи {task_id}")
            task_config["ml_refit"] = "enable"
        
        if ml_predict:
            self.logger.debug(f"Планировщик активирует предсказание для задачи {task_id}")
            task_config["ml_predict"] = "enable"


        task_config_file_path = task_path / self.task_config_filename

        self.logger.debug(f"Подготовка конфигурационного файла задачи {task_id} по пути {task_config_file_path}")
        with open(task_config_file_path, mode="w", encoding="utf-8") as task_config_file:
            json.dump(task_config, task_config_file)

        self.logger.debug(f"Подготовлена задача {task_id} в {task_path}")
        return task_path


    def run(self):
        self.logger.info(f"Запуск основного цикла")

        while True:
            self.logger.debug(f"Проверка, не завершена ли уже работа с текущим поколением")

            current_generation_number = self.check_calypso_generation()
            if current_generation_number is not None:
                possible_current_task_path = self.get_task_path_from_id(str(current_generation_number))

                possible_current_task_config = possible_current_task_path / self.task_config_filename
                if possible_current_task_config.is_file():
                    self.logger.debug(f"Задание slurm для поколения {str(current_generation_number)} уже существует по пути {possible_current_task_path}")
                    
                    slurm_id = self.get_current_slurm_id_from_id(current_generation_number)

                    if slurm_id is None:
                        self.submit_slurm_task_from_id(current_generation_number, str(current_generation_number))
                        time.sleep(self.loop_sleep_seconds)
                        continue

                    job_status = get_job_status(slurm_id)

                    if job_status == "PENDING" or job_status == "RUNNING":
                        self.logger.info(f"Задание slurm {slurm_id} для поколения {current_generation_number} в состоянии {job_status}, ожидаю")
                        time.sleep(self.loop_sleep_seconds)
                        continue
                    elif not self.check_if_all_task_job_completed_from_id(str(current_generation_number)):
                        # NOTE: лучше бы проверить детально наличие всех нужных файлов и соответствие количеств POSCAR_*
                        self.logger.warning(f"Задание slurm для поколения {current_generation_number} завершились не полностью или с ошибками")
                        self.submit_slurm_task_from_id(current_generation_number, f"{current_generation_number}R")
                        time.sleep(self.loop_sleep_seconds)
                        continue

                    self.logger.debug(f"Копирую выходые файлы расчётов в папку Calypso")
                    self.copy_output_from_task_from_id(str(current_generation_number))
                else:
                    self.logger.info(f"Подготовка задния для поколения {current_generation_number}")
                    poscars = self.get_calypso_poscars()

                    task_path = self.prepare_task_from_poscars(poscars=poscars, 
                                                               task_id=str(current_generation_number),
                                                               ml_train=current_generation_number < self.ml_train_until,
                                                               ml_refit=current_generation_number == self.ml_train_until,
                                                               ml_predict=current_generation_number > self.ml_train_until,
                                                               # TODO: включить refit/predict по триггеру
                                                               )
                    self.logger.info(f"Подготовлено задание в {str(task_path)}")
                    continue

            self.logger.info("Запуск Calypso")
            self.execute_calypso()
            poscars = self.get_calypso_poscars()
            updated_generation_number = self.check_calypso_generation()

            if not poscars:
                self.logger.info(f"Calypso корректно завершила работу, но не сгенерировала POSCAR_*. Работа завершена.")
                return None
            
            if updated_generation_number is None:
                raise RuntimeError(f"После запуска Calypso generation_number = None")
            
            if updated_generation_number == current_generation_number:
                raise RuntimeError(f"После запуска Calypso updated_generation_number ({updated_generation_number}) = \
                                   current_generation_number {current_generation_number}")
        
        return None


def main():
    parser = argparse.ArgumentParser(description="Скрипт-планировщик для работы с Calypso и Slurm")
    parser.add_argument("--command", required=True, 
                        help="Команда запуска вычислений для структуры") 
    parser.add_argument("--debug", action='store_true',
                        help="Режим отладки")
    parser.add_argument("--calypso_exe", required=True, 
                        help="Путь к исполняемому файлу Calypso")
    parser.add_argument("--calypso_workdir", required=False,
                        default="./calypso",
                        help="Путь к рабочей директории Calypso")
    parser.add_argument("--tasks_dir", required=False,
                        default="./tasks", 
                        help="Путь к директории, в которой будут располагаться задания Slurm")
    parser.add_argument("--input_dir", required=False,
                        default="./input",
                        help="Путь к директории, в которой находятся общие входные файлы для VASP:\
                         POTCAR, INCAR_1, INCAR_2, ..., INCAR_N")
    parser.add_argument("--sbatch_template", required=True, 
                        help="Путь к шаблону sbatch-файла для Slurm.\
                        Шаблон уже должен содержать корректные описания запрашиваемых ресурсов для Slurm,\
                        а также правильно вызывать task.py: '$TASK_SCRIPT'")
    parser.add_argument("--log_file", required=False,
                        default="./scheduler.log",
                        help="Путь к файлу лога")
    parser.add_argument("--ml_train_until", required=False,
                        help="Поколения с 1 (включительно) до указанного будут использованы для обучения VASP ML FF,\
                            затем на указанном поколении будет произведён refit и\
                            после указанного обученные ML FF будут использоваться в режиме Fast")
    
    
    args = parser.parse_args()

    logger = logging.getLogger("ПЛАНИРОВЩИК")
    logger.setLevel(logging.DEBUG)

    logger.addFilter(DuplicateFilter())

    if not args.log_file is None:
        file_formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s', 
                                        datefmt='%Y-%m-%d %H:%M:%S')
        file_handler = logging.FileHandler(str(Path(args.log_file).resolve()))
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    ml_train_until = 0
    if not args.ml_train_until is None:
        ml_train_until = int(args.ml_train_until)
        logger.info(f"Подключено машинное обучение VASP до итерации {args.ml_train_until}")

    console_formatter = logging.Formatter('[%(asctime)s] %(message)s', 
                                        datefmt='%Y-%m-%d %H:%M:%S')
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO) 
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(console_handler)

    slurm_config = SlurmConfig(
        sbatch_template=Path(args.sbatch_template).resolve()
    )

    scheduler = CalypsoScheduler(
        vasp_cmd=args.command,
        calypso_exe=Path(args.calypso_exe).resolve(),
        calypso_workdir=Path(args.calypso_workdir).resolve(),
        tasks_dir=Path(args.tasks_dir).resolve(),
        input_dir=Path(args.input_dir).resolve(),
        slurm_config=slurm_config,
        logger=logger,
        ml_train_until=ml_train_until,
    )

    scheduler.run()
    return None


if __name__ == "__main__":
    main()
