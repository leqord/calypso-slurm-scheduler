import logging
import sys
from pathlib import Path
from job import VaspJob


def test_job() -> None:
    print("Запуск теста")
    print("Создание задачи")
    logger = logging.getLogger("TRAINING")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s %(name)s %(levelname)s] %(message)s', datefmt='%H:%M:%S')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(formatter)

    job = VaspJob(Path("test/task"), Path("test/input"), Path("test/POSCAR_initial"), logger, task_cmd="echo 'step' && touch ./CONTCAR")
    print("Запуск")
    job.run()
    return None


if __name__ == "__main__":
    test_job()
