import sqlite3
import pandas as pd

# Connect to the SQLite3 service
conn = sqlite3.connect('STAFF.db')

# Define table parameters
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Read the CSV data
file_path = '~/development/ml_eng/home/project/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

# Load the CSV to the database
df.to_sql(table_name, conn, if_exists = 'replace', index = False)
print('Table is ready')

# Query 1: Display all rows of the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 2: Display only the FNAME column for the full table.
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Query 3: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Define data to be appended
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

# Append data to the table
data_append.to_sql(table_name, conn, if_exists = 'append', index = False)
print('Data appended successfully')

# Query 4: Display the count of the total number of rows.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Close the connection
conn.close()

# Practice Problems
# Try the following practice problems to test your understanding of the lab. Please note that the solutions for the following are not shared, and the learners are encouraged to use the discussion forums in case they need help.

# In the same database STAFF, create another table called Departments. The attributes of the table are as shown below.

# Header	Description
# DEPT_ID	Department ID
# DEP_NAME	Department Name
# MANAGER_ID	Manager ID
# LOC_ID	Location ID
# Populate the Departments table with the data available in the CSV file which can be downloaded from the link below using wget.

# 1
# https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/Departments.csv
# Copied!Executed!
# Append the Departments table with the following information.

# Attribute	Value
# DEPT_ID	9
# DEP_NAME	Quality Assurance
# MANAGER_ID	30010
# LOC_ID	L0010
# Run the following queries on the Departments Table:
# a. View all entries
# b. View only the department names
# c. Count the total entries