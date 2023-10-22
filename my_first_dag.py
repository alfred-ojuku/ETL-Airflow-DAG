# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Alfred Ojuku',
    'start_date': days_ago(0),
    'email': ['alfredokinyi68.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG

# define the DAG
dag = DAG(
    'Dag_Using_Shell',
    default_args=default_args,
    description='Dag Using shell',
    schedule_interval=timedelta(days=1),
)

# define the task **extract_transform_and_load** to call shell script

#calling the shell script
extract_transform_load = BashOperator(
    task_id="extract_transform_load",
    bash_command="/home/fender/airflow/dags/my_first_dag.sh ",
    dag=dag,
)

# task pipeline
extract_transform_load
