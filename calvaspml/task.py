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


class VaspExecutionError(Exception):
    pass


class VaspJob:
    def __init__(self,
                 workdir: Path,
                 inputdir: Path,
                 initial_structure_filepath: Path,
                 logger: logging.Logger,
                 kspacing: float = 0.05,
                 task_cmd: str = "mpirun vasp_std",
                 ml_inputdir: Path = None,
                 ):
        self.logger = logger
        self.task_cmd = task_cmd
        self.workdir = workdir.resolve()
        self.inputdir = inputdir.resolve()
        self.initial_structure_filepath = initial_structure_filepath.resolve()
        self.kspacing = kspacing

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
        
        if ml_inputdir is not None:
            self.ml_inputdir = ml_inputdir.resolve()
            self.logger.info(f"Подключены входные файлы для МО: {self.ml_inputdir}")
        else:
            self.logger.info(f"МО не подключено")
            self.ml_inputdir = None

        self.workdir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Рабочая директория задачи: {self.workdir}")

        self.poscar_original = self.workdir / "POSCAR_ORIGINAL"
        shutil.copy(self.initial_structure_filepath, self.poscar_original)
        self.logger.info(f"Скопирован исходный файл структуры в {self.poscar_original}")


    def write_kpoints(self, workdir: Path) -> None:
        # credits to Li Zhu < zhulipresent@gmail.com >
        def dot(x, y):
            return x[0]*y[0] + x[1]*y[1] + x[2]*y[2]
        
        def cross(x, y):
            z1 = x[1]*y[2] - x[2]*y[1]
            z2 = x[2]*y[0] - x[0]*y[2]
            z3 = x[0]*y[1] - x[1]*y[0]
            return [z1, z2, z3]
        
        def kmf(kgrid, gi):
            kd = int(gi/kgrid/2.0/math.pi)
            if kd == 0: kd = 1
            dd = gi/kd/2.0/math.pi
            if dd >= kgrid:
                for i in range(0, 10):
                    kd += i
                    dd = gi/kd/2.0/math.pi
                    if dd <= kgrid: break
            return kd 

        poscar_content = []     
        with open(workdir + 'POSCAR') as poscar:
            for line in poscar:
                poscar_content.append(line.split())
        
        lattice = []
        for item in poscar_content[2:5]:
            lattice.append(list(map(float, item)))

        c = cross(lattice[1], lattice[2])
        volume = dot(lattice[0], c)
        g = []
        g1 = [ 2.0 * math.pi * item / volume for item in c]
        c = cross(lattice[2], lattice[0])
        g2 = [ 2.0 * math.pi * item / volume for item in c]
        c = cross(lattice[0], lattice[1])
        g3 = [ 2.0 * math.pi * item / volume for item in c]
        g = [g1, g2, g3]
        
        rl = []
        for i in range(0, 3):
            rl.append(math.sqrt(dot(g[i],g[i])))

        kmesh = []
        for i in range(0, 3):
            kmesh.append(kmf(self.kspacing, rl[i]))
        
        with open(workdir + 'KPOINTS', 'w') as kpoints:
            kpoints.write('A\n0\nG\n')
            kpoints.write('%2d %2d %2d\n' % tuple(kmesh))
            kpoints.write('%2d %2d %2d\n' % (0,0,0))
            kpoints.close()
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

            self.logger.debug(f"Создаём KPOINTS")
            self.write_kpoints(step_dir)

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
        kspacing = config.get("kspacing", 0.04)

        ml_train = config.get("ml_train", False)
        ml_refit = config.get("ml_refit", False)
        ml_predict = config.get("ml_predict", False)

        # TODO: проверить логику
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


    for poscar_file in poscar_files:
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
                          kspacing=kspacing,
                          logger=logger,
                          task_cmd=vasp_cmd)
            job.run()
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
