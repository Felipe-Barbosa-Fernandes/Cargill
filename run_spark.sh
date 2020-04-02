#!/usr/bin/env bash

. venv/bin/activate

input_path = $1
output_path = $2

spark-submit \
    --executor-cores 2 \
    --num-executors 2\
    --executor-memory 1G \
    --master local[*] \
    code.py ${input_path} ${output_path}


