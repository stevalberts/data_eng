# PROJECT SCENARIO
# A company sells various electronic products through its online and offline channels across major cities in the United States. 
# They operate multiple stores and warehouses to manage their inventory and sales operations. 
# The company wants to create a data warehouse to analyze its sales performance and inventory management and aim to generate reports, such as:
# Total sales revenue per year per city
# Total sales revenue per month per city
# Total sales revenue per quarter per city
# Total sales revenue per year per product category
# Total sales revenue per product category per city
# Total sales revenue per product category per store

# INSTRUCTIONS
# In this practice project, you are going to perform the following tasks:
# Task 1: Design the dimension table DimDate
# Task 2: Design the dimension table DimProduct
# Task 3: Design the dimension table DimCustomerSegment
# Task 4: Design the fact table FactSales
# Task 5: Create the dimension table DimDate
# Task 6: Create the dimension table DimProduct
# Task 7: Create the dimension table DimCustomerSegment
# Task 8: Create the fact table FactSales
# Task 9: Load data into the dimension table DimDate
# Task 10: Load data into the dimension table DimProduct
# Task 11: Load data into the dimension table DimCustomerSegment
# Task 12: Load data into the fact table FactSales
# Task 13: Create a grouping sets query 
# Task 14: Create a rollup query 
# Task 15: Create a cube query using the columns year, city, productid, average sales revenue 
# Task 16: Create a materialized view named max_sales using the columns city, productid, product type, and max sales.

CREATE TABLE DimDate (
    dateid INT PRIMARY KEY,
    month INT,
    monthname VARCHAR(20),
    year INT,
    day INT,
    weekday INT,
    weekdayname VARCHAR(20)
);

CREATE TABLE MyDimWaste (
    wasteid INT PRIMARY KEY,
    wastename VARCHAR(255),
);

CREATE TABLE MyDimZone (
    zoneid INT PRIMARY KEY,
    zonename VARCHAR(255)
);

CREATE TABLE MyFactTrips (
    tripid INT PRIMARY KEY,
    wasteid INT,
    zoneid INT,
    collected INT,
    unit VARCHAR(255),
    dateid INT
);