#!/bin/bash

# Database connection details
DB_USER="root"
DB_PASS="A4YNjIQB00It2jiOJRDaEQ1R"
DB_NAME="sales"
TABLE_NAME="sales_data"
OUTPUT_FILE="sales_data.sql"

# Export data
mysqldump -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" "$TABLE_NAME" > "$OUTPUT_FILE"

# Check if the export was successful
if [ $? -eq 0 ]; then
    echo "Data exported successfully to $OUTPUT_FILE"
else
    echo "Error exporting data"
fi