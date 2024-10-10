# Import the libraries

from datetime import timedelta
# The DAG object we will need to instanciate DAG
from airflow.models import DAG
# Operators, you need these to write tasks
from airflow.operators.bash import BashOperator
# This makes the scheduling easy
from airflow.utils.dates import days_ago

# defining DAG arguments
default_args = {
    'ower': 'your_name_here',
    'email': ['your_email_here'],
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'my-first-dag',
    default_args=default_args,
    description= 'My first DAG',
    schedule_interval=timedelta(days=1),
)

# defining the first task

extract = BashOperator(
    task_id='extract',
    bash_command='cut -d":" -f1,3,6 /etc/passwd > /home/project/airflow/dags/extracted-data.txt',
    dag=dag,
)

# define the second task

transform_and_load = BashOperator(
    task_id='transform',
    bash_command='tr ":" "," < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv',
    dag=dag
)

# task pipeline
extract >> transform_and_load