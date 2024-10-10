# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2

# Connect to MySQL
conSql = mysql.connector.connect(user='root', password='',host='localhost',database='sales')

# Connect to DB2 or PostgreSql
conPg = psycopg2.connect(
   database="postgres", 
   user='postgres',
   password='postgres',
   host='localhost', 
   port= "6432"
)

# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():
    cursor = conPg.cursor() # Define the cursor from the connection
    # Query the sales_data table to get the last rowid
    cursor.execute('select rowid from sales_data order by rowid desc limit 1;')
    rows = cursor.fetchall()
    conPg.commit()
    for row in rows:
        print(row)  # This will print the entire row tuple
        return row[0]  # Return only the rowid

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    cursor = conSql.cursor()
    cursor.execute(f'SELECT * FROM sales_data where "rowid" > {rowid};')
    rows = cursor.fetchall()
    print(rows)
    return rows
		

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    # Reopen the connection if it's closed
	cursor = conPg.cursor()
	cursor.executemany('INSERT INTO sales_data (rowid, sales_id, sales_amount, sales_date, product_id, customer_id, store_id) VALUES (%s, %s, %s, %s, %s, %s, %s)', records)
	conPg.commit()
 

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
conSql.close()

# disconnect from DB2 or PostgreSql data warehouse 
conPg.close()

# End of program