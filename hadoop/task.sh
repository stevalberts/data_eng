# Setup Hive and Bee
# You will use the hive from the docker hub for this lab. 
# Pull the hive image into your system by running the following command.

docker pull apache/hive:4.0.0-alpha-1

# Now, you will run the hive server on port 10002. You will name the server instance myhiveserver. 
# We will mount the local data folder in the hive server as hive_custom_data. 
# This would mean that the whole data folder that you created locally, along with anything you add in the data folder, 
# is copied into the container under the directory hive_custom_data.

docker run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 -v /data:/hive_custom_data --name myhiveserver apache/hive:4.0.0-alpha-1

# You can open and take a look at the Hive server with the GUI. Click the button to open the HiveServer2 GUI.
# Now run the following command, which allows you to access beeline. This is a SQL cli where you can create, 
# modify, delete table, and access data in the table.

docker exec -it myhiveserver beeline -u 'jdbc:hive2://localhost:10000/'

# Create table, add and view data
create table Employee(emp_id string, emp_name string, salary  int)  row format delimited fields terminated by ',' ;

show tables;

LOAD DATA INPATH '/hive_custom_data/emp.csv' INTO TABLE Employee;

select * from Employee;