# An Apache Airflow DAG is a python program. It consists of these logical blocks.
#Imports
#DAG Arguments
#Task Definitions
#Task Pipeline

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
    'email': ['Alfredokinyi68@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#DAG arguments are like settings for the DAG.
#The above settings mention
#the owner name,
#when this DAG should run from: days_age(0) means today,
#the email address where the alerts are sent to,
#whether alert must be sent on failure,
#whether alert must be sent on retry,
#the number of retries in case of failure, and
#the time delay between retries.

# define the DAG

dag = DAG(
    dag_id='sample-etl-dag',
    default_args=default_args,
    description='Sample ETL DAG using Bash',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the first task named extract
extract = BashOperator(
    task_id='extract',
    bash_command='ech "extract"',
    dag=dag,
)

# define the second task named transform
transform = BashOperator(
    task_id='transform',
    bash_command='echo "transform"',
    dag=dag,
)

# define the third task named load

load = BashOperator(
    task_id='load',
    bash_command='echo "load"',
    dag=dag,
)

# task pipeline
extract >> transform >> load