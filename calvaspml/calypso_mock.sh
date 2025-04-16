#!/bin/bash

threshold=42


if [[ ! -f "step" ]]; then
    echo 0 > step
fi

value=$(<step)

rm ./OUTCAR_*
rm ./CONTCAR_*
rm ./POSCAR_*

if (( value > threshold )); then
    echo "работа завершена"
    exit 0
else
    value=$((value + 1))
    echo "$value" > step
    echo "############## Calypso $value"

    touch ./POSCAR_1
    touch ./POSCAR_2
    touch ./POSCAR_3
    touch ./POSCAR_4
    touch ./POSCAR_5
    touch ./POSCAR_6
    touch ./POSCAR_7
    touch ./POSCAR_8
    touch ./POSCAR_9
    touch ./POSCAR_10
fi
