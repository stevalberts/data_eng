# Build ETL Data Pipelines with BashOperator using Apache Airflow

# Project Scenario:
# Decongesting the national highways by analyzing the road traffic data from different toll plazas. 
# Difficulty: Each highway is operated by a different toll operator with a different IT setup that uses different file formats. 
# The task: To collect data available in different formats and consolidate it into a single file.

# Set up the environment

# Create a directory structure for the staging area
sudo mkdir -p /home/project/airflow/dags/finalassignment/staging

# Give appropriate permission to the directories.
sudo chmod -R 777 /home/project/airflow/dags/finalassignment

# Download the data set from the source to the following destination using the curl command.
sudo curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o /home/project/airflow/dags/finalassignment/tolldata.tgz

