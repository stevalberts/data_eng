# Food Price Data ETL to Postgres DB (Supabase)

This project automates the ETL (Extract, Transform, Load) process for food price data from an external source (HDX) to a PostgreSQL database on Supabase. The data is used for food price and market prediction in a structured, queryable format.

## Overview

![ETL Process Sequence Diagram](https://www.plantuml.com/plantuml/png/dOwnJiCm48RtFCMlJcNe2sH0HHG4KWT46iHMNPrhQpMse_D5YBUdhXMHCD5kPz__tVSlev9exzg2Z0P17Lb04NNHaQWrjYElc3rjrgYCs4vdQPl7QUHbzOdvO_M15IcX2hqOlhkapvjfz2r4FGX7pAk-enr5la1foCGGA8eQYtjaNOdttYCrYLcVlrFWR2Znp6gEI0qQKYfIdyKOPkt5_-u7V1JP2Fcu3-8k2MX0L194SsH7I0lICJVKBF7g8ujcp7eDQApRdbyCfLtBnP2SD7em9n--McuE39iFJDWhL8xan7ghBDDNw4nmBZHMlsypZWxyKxedezjEhE5j-hn5--V4M5oNM8S3OcyCnuWWO94W4ViqaZaYT2oaDlXYoRxfNVi3)

This script:

1. Downloads food price data from an HDX URL (CSV).
2. Transforms the data into a star schema format with three dimensions (`dim_date`, `dim_market`, `dim_commodity`) and a fact table (`fact_price_data`).
3. Inserts data into Supabase tables in batches, improving performance and ensuring data integrity.

## Prerequisites

* Python 3.7 or higher
* Supabase account with API key and URL
* Environment variables file (.env) containing Supabase credentials:

```
SUPABASE_URL=<your_supabase_url>
SUPABASE_KEY=<your_supabase_key>
```

## Project Setup

1. **Clone the Repository**:

```
git clone <repository-url>
cd <repository-name>
```

2. **Install Required Packages**:

```
pip install pandas supabase-py python-dotenv
```

3. **Set Up Environment Variables**:
   * Create a `.env` file in the root directory with Supabase credentials.

4. **Run the Script**:

```
python etl_food_price.py
```

## Script Breakdown

* **Data Extraction**: Loads data from HDX CSV URL into a pandas DataFrame.
* **Data Transformation**:
   * Processes `dim_date`, `dim_market`, and `dim_commodity` tables with unique values.
   * Generates `date_id`, `market_id`, and `commodity_id` mappings for use in the fact table.
* **Data Loading**:
   * Batch inserts data into Supabase to optimize performance.
   * Logs success or failure for each insert to aid in debugging and monitoring.

## Table Schemas

The database schema follows a star schema model:

1. **dim_date**: Stores unique dates with day, month, and year breakdowns.
2. **dim_market**: Contains unique market information, including location coordinates.
3. **dim_commodity**: Holds unique commodity details.
4. **fact_price_data**: The fact table with `date_id`, `market_id`, `commodity_id`, and pricing information.

## Notes

* **Error Handling**: The script logs each operation's status for tracking.
* **Batch Insertion**: Uses `upsert` where supported, to avoid duplicate entries.