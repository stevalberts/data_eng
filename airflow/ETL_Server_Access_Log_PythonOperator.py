# Import the Libraries
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

# For scheduling
from airflow.utils.dates import days_ago
import requests

# Define the path for the input and output files
input_file = 'web-server-access-log.txt'
extracted_file = 'extracted-data.txt'
transformed_file = 'transformed.txt'
output_file = 'capitalized.txt'

def download_file():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    
    # Send GET request to the URL
    with requests.get(url, stream=True) as response:
        # Raise an exception for HTTP errors
        response.raise_for_status()
        # Open a local file in binary mode
        with open(input_file, 'wb') as file:
            # Write the content to the local file
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"File downloaded successfully: {input_file}")
    
    
def extract():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile,\
            open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split(':')
            if len(fields) >= 6:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")
    
    
def transform():
    global extracted_file, transformed_file
    print("Inside Transform")
    with open(extracted_file, 'r') as infile, \
            open(transformed_file, 'w') as outfile:
        for line in infile:
            processed_line = line.upper()
            outfile.write(processed_line + '\n')
      
def load():
    global transformed_file, output_file
    print("Inside Load")
    # Save the array to a CSV file
    with open(transformed_file, 'r') as infile, \
            open(output_file, 'w') as outfile:
        for line in infile:
            outfile.write(line + '\n')


def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)


# You can overide them on a per-task basis during operator initialization
default_args = {
    'owner' : 'John Doe',
    'start_date' : days_ago(0),
    'email' : ['example@email.com'],
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}


# Define the DAG
dag = DAG(
    'my-first-python-etl-dag',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1),
)

# Define the task named download to call the download_file function
download = PythonOperator(
    task_id = 'download',
    python_callable=download_file,
    dag=dag,
)

# Define the task named execute_extract to call the extract function
execute_extract = PythonOperator(
    task_id = 'extract',
    python_callable=extract,
    dag=dag,
)

# Definle the task named execute_transform to call the transform function
execute_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

# Define the task named execute_load to call the load function
execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

# Define the task named execute_load to call the load function
execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)

# Task pipeline
download >> execute_extract >> execute_transform >> execute_load >> execute_check


# Open terminal
# Copy the DAG file into the dags directory.
'''
cp ETL_Server_Access_Log_Processing.py $AIRFLOW_HOME/dags
'''

# Verify if the DAG is submitted by running the following command.
'''
airflow dags list | grep etl-server-logs-dag
'''

# If the DAG didn't get imported properly, you can check the error using the following command.
'''
airflow dags list-import-errors
'''







"""
Write a DAG named ETL_Server_Access_Log_Processing that will extract a file from a remote server and then transform the content and load it into a file.

The file URL is given below:
https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt

The server access log file contains these fields.

a. timestamp - TIMESTAMP
b. latitude - float
c. longitude - float
d. visitorid - char(37)
e. accessed_from_mobile - boolean
f. browser_code - int

Tasks
Add tasks in the DAG file to download the file, read the file, and extract the fields timestamp and visitorid from the web-server-access-log.txt.

Capitalize the visitorid for all the records and store it in a local variable.

Load the data into a new file capitalized.txt.

Create the imports block.

Create the DAG Arguments block. You can use the default settings.

Create the DAG definition block. The DAG should run daily.

Create the tasks extract, transform, and load to call the Python script.

Create the task pipeline block.

Submit the DAG.

Verify if the DAG is submitted.
"""