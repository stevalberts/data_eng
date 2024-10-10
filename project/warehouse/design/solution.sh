# Design the dimension table softcartDimDate
CREATE TABLE softcartDimDate (
    dateid INT PRIMARY KEY,
    month INT,
    monthname VARCHAR(20),
    year INT,
    day INT,
    weekday INT,
    weekdayname VARCHAR(20)
);

# Design the dimension table softcartDimCategory
CREATE TABLE softcartDimCategory (
    categoryid INT PRIMARY KEY,
    categoryname VARCHAR(255)
);

# Design the dimension table softcartDimItem
CREATE TABLE softcartDimItem (
    itemid INT PRIMARY KEY,
    itemname VARCHAR(255),
    categoryid INT,
    FOREIGN KEY (categoryid) REFERENCES softcartDimCategory(categoryid)
);

# Design the dimension table softcartDimCountry
CREATE TABLE softcartDimCountry (
    countryid INT PRIMARY KEY,
    countryname VARCHAR(255)
);

# Design the fact table softcartFactSales
CREATE TABLE softcartFactSales (
    salesid INT PRIMARY KEY,
    dateid INT,
    orderid INT,
    itemid INT,
    quantity INT,
    price DECIMAL(10, 2),
    total DECIMAL(10, 2),
    countryid INT,
    FOREIGN KEY (countryid) REFERENCES softcartDimCountry(countryid),
    FOREIGN KEY (dateid) REFERENCES softcartDimDate(dateid),
    FOREIGN KEY (itemid) REFERENCES softcartDimItem(itemid),
);


# staging-# \c softcart
# psql (14.13 (Ubuntu 14.13-0ubuntu0.22.04.1), server 13.2)
# You are now connected to database "softcart" as user "postgres".
# softcart-# \dt
#                 List of relations
#  Schema |        Name         | Type  |  Owner   
# --------+---------------------+-------+----------
#  public | softcartdimcategory | table | postgres
#  public | softcartdimcountry  | table | postgres
#  public | softcartdimdate     | table | postgres
#  public | softcartdimitem     | table | postgres
#  public | softcartfactsales   | table | postgres
# (5 rows)

# softcart-# psql -d softcart -U postgres
# softcart-# \dt
#                 List of relations
#  Schema |        Name         | Type  |  Owner   
# --------+---------------------+-------+----------
#  public | softcartdimcategory | table | postgres
#  public | softcartdimcountry  | table | postgres
#  public | softcartdimdate     | table | postgres
#  public | softcartdimitem     | table | postgres
#  public | softcartfactsales   | table | postgres
# (5 rows)

# softcart-# pg_dump -U postgres -d softcart --schema-only -f schema.sql
# softcart-# psql -d staging -f schema.sql
# softcart-# \dt
#                 List of relations
#  Schema |        Name         | Type  |  Owner   
# --------+---------------------+-------+----------
#  public | softcartdimcategory | table | postgres
#  public | softcartdimcountry  | table | postgres
#  public | softcartdimdate     | table | postgres
#  public | softcartdimitem     | table | postgres
#  public | softcartfactsales   | table | postgres
# (5 rows)

# softcart-# 

