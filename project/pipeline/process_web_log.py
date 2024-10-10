# Import the libraries
import tarfile
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator

# This makes scheduling easy
from airflow.utils.dates import days_ago

# Define the path for the input and output files
input_file = './accesslog.txt'
extracted_file = 'extracted_data.txt'
transformed_file = 'transformed.txt'
output_file = 'transformed_data.csv'

# Function to extract IP addresses
def extract():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as logs, \
            open(extracted_file, 'w') as outfile:
        for log in logs:
            fields = log.split(' ')
            if len(fields) > 0:
                ip_address = fields[0]  # The IP address is the first field
                outfile.write(ip_address + "\n")
    
            
# Function to transform the data by filtering out a specific IP address
def transform():
    global extracted_file, transformed_file
    print("Inside Transform")
    # Open the extracted file and the output file
    with open(extracted_file, 'r') as infile, \
            open(transformed_file, 'w') as outfile:
        for line in infile:
            # Strip any whitespace and check if the line contains the IP address to filter out
            ip_address = line.strip()
            if ip_address != "198.46.149.143":  # Exclude this IP
                # Write all IP addresses except the one being filtered out
                outfile.write(ip_address + "\n")
            
# Function to load data by archiving the transformed file into a tar file
def load():
    global transformed_file
    output_tar_file = 'weblog.tar'
    print("Inside Load")
    # Create a tar archive file
    with tarfile.open(output_tar_file, "w") as tar:
        # Add the transformed file to the archive
        tar.add(transformed_file, arcname='transformed_data.txt')


def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)


# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'John Doe',
    'start_date': days_ago(0),
    'email': ['example@email.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='Process Web Log DAG',
    schedule_interval=timedelta(days=1),
)

# Define the task named execute_extract to call the `extract` function
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    dag=dag,
)

# Define the task named execute_transform to call the `transform` function
transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag,
)

# Define the task named execute_load to call the `load` function
load_data = PythonOperator(
    task_id='load_data',
    python_callable=load,
    dag=dag,
)

# Define the task named execute_load to call the `load` function
execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)

# Task pipeline
extract_data >> transform_data >> load_data 


# Start Airflow Services
# airflow webserver --port 8080

# Start the Airflow scheduler
# airflow scheduler

# Create a user
# airflow users create \
#   --username admin \
#   --password YOUR_PASSWORD \
#   --firstname Admin \
#   --lastname User \
#   --role Admin \
#   --email admin@example.com

# TO SUMIT A DAG
# * Create
# export AIRFLOW_HOME=/home/project/airflow
# echo $AIRFLOW_HOME

# * Submit
#  cp my_first_dag.py $AIRFLOW_HOME/dags

# * list
# airflow dags list

# * Verify
# airflow dags list|grep "my-first-python-etl-dag"

# Run the command below to list out all the tasks
# airflow tasks list my-first-dag

# Unpause Dags
# airflow dags unpause process_web_log

# Monitor
# airflow dags list-runs -d process_web_log