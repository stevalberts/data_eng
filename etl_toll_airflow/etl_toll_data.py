# Import the libraries
from datetime import timedelta
# The DAG object we will need to instanciate DAG
from airflow.models import DAG
# Operators, you need these to write tasks
from airflow.operators.bash import BashOperator
# This makes the scheduling easy
from airflow.utils.dates import days_ago


# defining the DAG arguments
default_args = {
    'owner':'john_doe',
    'email':['example.email.com'],
    'start_date':days_ago(0),
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description= 'Apache Airflow ETL toll data',
    schedule_interval=timedelta(days=1),
)

# defining unzip_data task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf tolldata.tgz',
    dag=dag,
)

# Define the staging directory and file paths
staging_dir = '/home/project/airflow/dags/finalassignment/staging'  # Update this with your staging directory path
extracted_file = f"{staging_dir}/extracted_data.csv"
transformed_file = f"{staging_dir}/transformed_data.csv"


# defining extract_data_from_csv task (Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type) 
# from the vehicle-data.csv file and save them into a file named csv_data.csv

# cut -d',' -f1,2,3

# cut -d',' -f1,2,3 vehicle-data.csv > csv_data.csv



extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d',' -f1,2,3 vehicle-data.csv > csv_data.csv",
    dag=dag,
)

# defining extract_data_from_tsv task (Number of axles, Tollplaza id, and Tollplaza code)
# from the tollplaza-data.tsv file and save it into a file named tsv_data.csv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -d$'\t' -f1,2,3 tollplaza-data.tsv > tsv_data.csv",
    dag=dag,
)

# defining extract_data_from_fixed_width task (Type of Payment code, Vehicle Code)
# from the fixed width file payment-data.txt and save it into a file named fixed_width_data.csv.
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="cut -c12-13,14-19 payment-data.txt > fixed_width_data.csv",
    dag=dag,
)

# defining consolidate_data task to consolidate data extracted from (csv_data.csv,tsv_data.csv,fixed_width_data.csv)
# to a single file extracted_data.csv
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=(
        f"paste {staging_dir}/csv_data.csv {staging_dir}/tsv_data.csv {staging_dir}/fixed_width_data.csv > {extracted_file}"
    ),  
    dag=dag,
)

# defining transform_data task to transform the vehicle_type field in extracted_data.csv into capital letters 
# and save it into a file named transformed_data.csv in the staging directory.
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=(
        f"head -n 1 {extracted_file} > {transformed_file} && "
        f"tail -n +2 {extracted_file} | "
        f"awk -F',' '{{OFS=\",\"; $4=toupper($4); print}}' >> {transformed_file}"
        ),
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

# TO SUMIT A DAG
# * Create
# export AIRFLOW_HOME=/home/project/airflow
# echo $AIRFLOW_HOME

# * Submit
#  cp ETL_toll_data.py $AIRFLOW_HOME/dags

# * list
# airflow dags list

# * Verify
# airflow dags list|grep "my-first-python-etl-dag"

# Run the command below to list out all the tasks
# airflow tasks list my-first-dag

# Unpause (ETL_toll_data) as id
# airflow dags unpause <dag_id>

# Trigger the DAG:
# airflow dags trigger <dag_id>

# EG:
# airflow dags unpause ETL_toll_data
# airflow dags trigger ETL_toll_data

# Show tasks
# airflow tasks list <dag_id>

# Show dag runs
# airflow dags list-runs -d <dag_id>



