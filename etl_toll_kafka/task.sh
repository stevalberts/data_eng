# Build a Streaming ETL Pipeline using Kafka

# Project: To de-congest the national highways by analyzing the road traffic data from different toll plazas. 
# As a vehicle passes a toll plaza, the vehicle's data like vehicle_id,vehicle_type,toll_plaza_id, and timestamp are streamed to Kafka. 
# Your task is to create a data pipe line that collects the streaming data and loads it into a database.

# STEPS

# Start a MySQL database server
# Create a table to hold the toll data
# Start the Kafka server
# Install the Kafka Python driver
# Install the MySQL Python driver
# Create a topic named toll in Kafka
# Download streaming data generator program
# Customize the generator program to steam to toll topic
# Download and customize streaming data consumer
# Customize the consumer program to write into a MySQL database table
# Verify that streamed data is being collected in the database table


# Download and extract Kafka
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.12-3.7.0.tgz

# Extract Kafka from the zip file by running the command below.
tar -xzf kafka_2.12-3.7.0.tgz

# Configure KRaft and start server
cd kafka_2.12-3.7.0
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)" # Generate a cluster UUID
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties # Pass the cluster id

# Start Kafka server
bin/kafka-server-start.sh config/kraft/server.properties

# Start MySQL server and setup the database
mysql --host=mysql --port=3306 --user=root --password=your_password_here

# Create a database named tolldata
create database tolldata;

# Create a table named livetolldata with the schema to store the data generated by the traffic simulator.
use tolldata;
create table livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);

# Disconnect from the MySQL server.
exit

# Install the Python packages
pip3 install kafka-python

# Install the Python module mysql-connector-python
pip3 install mysql-connector-python==8.0.31

# Create data pipeline for toll data
bin/kafka-topics.sh --create --topic toll --bootstrap-server localhost:9092 # Create a Kafka topic named toll

# Download the toll_traffic_generator.py from the url given below using wget.
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/toll_traffic_generator.py

# Open the code using the editor using the “Menu –> File –>Open” and set topic to toll.
# Run the code
python3 toll_traffic_generator.py

# Download the streaming-data-reader.py from the URL below using wget.
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/vVxmU5uatDowvAIKRZrFjg/streaming-data-reader.py

# Open the streaming-data-reader.py and modify the following details so that the program can connect to your MySQL server.
# Run the code
python3 streaming-data-reader.py


