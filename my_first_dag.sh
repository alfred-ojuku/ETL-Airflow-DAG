#!/bin/bash

echo "extract_transform_and_load"

cut -d":" -f1,3,6 /etc/passwd > /home/fender/airflow/dags/extracted-data.txt


tr ":" "," < /home/fender/airflow/dags/extracted-data.txt > /home/fender/airflow/dags/transformed-data.csv
