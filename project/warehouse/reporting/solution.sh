# Navigate to the reporting directory
cd /project/warehouse/reporting/

# ================================
# LOADING DATA
# ================================
# Copy data from CSV files into the DimDate table
\copy public."DimDate"(dateid, date, Year, Quarter, QuarterName, Month, Monthname, Day, Weekday, WeekdayName) 
FROM 'DimDate.csv' 
DELIMITER ',' 
CSV HEADER;  -- Use comma as delimiter and include header row

# Copy data from CSV files into the DimCategory table
\copy public."DimCategory"(categoryid, category) 
FROM 'DimCategory.csv' 
DELIMITER ',' 
CSV HEADER;  -- Use comma as delimiter and include header row

# Copy data from CSV files into the DimCountry table
\copy public."DimCountry"(countryid, country) 
FROM 'DimCountry.csv' 
DELIMITER ',' 
CSV HEADER;  -- Use comma as delimiter and include header row

# Copy data from CSV files into the FactSales table
\copy public."FactSales"(orderid, dateid, countryid, categoryid, amount) 
FROM 'FactSales.csv' 
DELIMITER ',' 
CSV HEADER;  -- Use comma as delimiter and include header row



# ================================
# QUERIES FOR DATA ANALYTICS
# ================================
#Create a grouping sets query
SELECT 
    COALESCE(c.country, 'Total') AS country,
    COALESCE(cat.category, 'Total') AS category,
    SUM(f.amount) AS total_sales
FROM 
    "FactSales" f
LEFT JOIN 
    "DimCountry" c ON f.countryid = c.countryid
LEFT JOIN 
    "DimCategory" cat ON f.categoryid = cat.categoryid
GROUP BY 
    GROUPING SETS (
        (c.country, cat.category),
        (c.country),
        (cat.category),
        ()
    )
ORDER BY 
    country, category;

# Create a rollup query
SELECT 
    EXTRACT(YEAR FROM d.full_date) AS year,
    COALESCE(c.country, 'Total') AS country,
    SUM(f.amount) AS total_sales
FROM 
    public."FactSales" f
JOIN 
    public."DimCountry" c ON f.countryid = c.countryid
JOIN 
    public."DimDate" d ON f.dateid = d.dateid
GROUP BY 
    ROLLUP(EXTRACT(YEAR FROM d.full_date), c.country)
ORDER BY 
    year, country;

# Create a cube query
SELECT 
    d.year AS year,
    COALESCE(c.country, 'Total') AS country,
    AVG(f.amount) AS average_sales
FROM 
    public."FactSales" f
JOIN 
    public."DimCountry" c ON f.countryid = c.countryid
JOIN 
    public."DimDate" d ON f.dateid = d.dateid
GROUP BY 
    CUBE(d.year, c.country)
ORDER BY 
    year, country;


# Create an MQT
CREATE MATERIALIZED VIEW total_sales_per_country AS
SELECT 
    c.country,
    SUM(f.amount) AS total_sales
FROM 
    public."FactSales" f
JOIN 
    public."DimCountry" c ON f.countryid = c.countryid
GROUP BY 
    c.country;






