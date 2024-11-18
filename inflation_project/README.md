# Food Price Prediction and Inflation Analysis Pipeline

## Overview
This project builds a data pipeline to support the weekly ingestion, processing, and analysis of food price data for inflation monitoring and predictive modeling. It enables agricultural market stakeholders to understand trends in food prices and anticipate inflationary pressures. Data is cleaned, transformed, and stored on AWS S3, creating a foundation for continuous analysis and forecasting. Power BI integration provides dynamic reports on historical trends, price fluctuations, and forecasted data, empowering data-driven decision-making.

Automation with Apache Airflow ensures that the pipeline updates consistently each week, maintaining data quality and providing scalable storage for long-term analysis.

## Key Features
- **Automated Data Ingestion**: Fetches and stores new food price data weekly on AWS S3.
- **Data Processing**: Cleans and standardizes raw data for use in analysis and forecasting.
- **Data Aggregation**: Computes metrics on a weekly and monthly basis to track price trends.
- **Predictive Modeling**: Applies forecasting models to predict food price movements.
- **Inflation Analysis**: Tracks indicators to analyze and forecast inflationary trends.
- **Power BI Reporting**: Delivers interactive reports on food price trends and inflation analysis.
- **Apache Airflow Automation**: Automates end-to-end data processing for weekly updates.

## Project Structure
- **data/**: Sample data and links to AWS S3 paths for storing raw, processed, and aggregated data.
- **src/**: Includes modules for ingestion, cleaning, transformation, analysis, and modeling.
  - **ingestion/**: Scripts for data ingestion and S3 uploads.
  - **cleaning/**: Scripts for data cleaning and preprocessing.
  - **transformation/**: Scripts for data transformation and feature engineering.
  - **analysis/**: Weekly report generation and trend analysis.
  - **models/**: Forecasting models for price prediction.
  - **utils/**: Helper functions.
- **notebooks/**: Jupyter notebooks for exploratory data analysis and model training.
- **reports/**: Power BI files, dashboard templates, and visualization assets.
- **config/**: Configuration files with paths, credentials, and settings.
- **tests/**: Scripts for unit tests to ensure data quality and consistency.
- **logs/**: Log files for tracking pipeline operations.
- **airflow_dags/**: Apache Airflow DAGs to orchestrate weekly tasks.

## Getting Started
1. **Setup**: Configure AWS S3 paths and credentials in the `config` files.
2. **Run Pipeline**: Use Apache Airflow to schedule and automate data ingestion and processing.
3. **Power BI Reports**: Connect Power BI to AWS S3 or a database for up-to-date visualizations.

## Dependencies
Refer to `requirements.txt` for a list of required Python libraries.

## Contact
For questions, please reach out to the project maintainer.