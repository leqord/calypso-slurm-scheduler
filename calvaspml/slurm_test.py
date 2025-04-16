#!/usr/bin/env python3

from slurm import *

def main_submit():
    import sys
    #if len(sys.argv) < 4:
    #    print("Использование: {} s<template_path> <command>".format(sys.argv[0]))
    #    sys.exit(1)
    template_path = sys.argv[1]
    command = sys.argv[2]
    try:
        job_id = submit_job(template_path, command)
        print(f"Задание отправлено успешно. ID: {job_id}")
    except SlurmSubmissionError as e:
        print(f"Ошибка: {e}")


def main_status():
    import sys
    
    if len(sys.argv) < 2:
        print("Использование: {} <job_id>".format(sys.argv[0]))
        sys.exit(1)
    job_id = sys.argv[1]
    status = get_job_status(job_id)
    print(f"Статус задания {job_id}: {status}")


if __name__ == "__main__":
    main_status()