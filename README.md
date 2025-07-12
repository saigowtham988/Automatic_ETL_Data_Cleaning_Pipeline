Automatic ETL Data Cleaning Pipeline (AWS + PySpark)
A highly configurable, cloud-native ETL pipeline built with AWS and PySpark to automate data ingestion, cleaning, transformation, and validation for analytics and machine learning applications.

Overview
This project implements a robust, serverless ETL (Extract, Transform, Load) pipeline on AWS using PySpark. It automates the process of ingesting raw data from Amazon S3, applying comprehensive data cleaning and transformation rules, and storing processed data back into S3 in an optimized format (e.g., Parquet). Designed for extensibility and reusability, the pipeline is fully customizable via AWS Glue job parameters, eliminating the need for code modifications.
The pipeline ensures production-ready data quality by handling diverse datasets, enforcing validation rules, detecting outliers, and generating detailed data quality reports.

Key Features

Flexible Data Ingestion:
Supports CSV, JSON, and Parquet formats from S3.


Configurable Data Cleaning:
Automatic whitespace trimming for string columns.
Standardization of string case (lowercase, uppercase, or title case).
Removal of non-alphanumeric characters from specified columns.


Intelligent Type Conversion:
Explicit type casting based on user-provided mappings.
Automatic type inference for strings to numeric, boolean, date, or timestamp formats.
Robust handling of numeric overflow (truncate, nullify, or convert to string).


Comprehensive Null Handling:
Drop rows with nulls in critical columns (e.g., identifiers).
Configurable null-filling strategies for non-critical columns (strings, numbers, dates, booleans).


Advanced Data Quality & Validation:
Custom Validation Rules: Apply user-defined rules with actions like dropping rows, setting nulls, or quarantining invalid records.
Outlier Detection: Supports IQR, Z-score, or simplified isolation forest methods for numeric columns.
Duplicate Removal: Configurable deduplication based on specific columns or entire rows.
Schema Validation: Optional validation against a predefined schema.


Data Profiling & Metrics:
Generates detailed data quality reports with completeness, consistency, and validity metrics, saved to S3.


Data Enrichment:
Add derived columns using Spark SQL expressions.


Auditability:
Adds metadata columns (e.g., ETL timestamps, Glue job IDs, source paths, row hashes) for traceability.


Performance Optimization:
Dynamic repartitioning, caching, and Spark configuration tuning for efficient processing.




Architecture
This pipeline leverages a serverless, event-driven architecture on AWS:

Data Landing: Raw data is uploaded to an S3 bucket (e.g., s3://your-raw-data-bucket/raw_data_folder/).
Event Trigger: S3 ObjectCreated events trigger a Lambda function.
Crawler Initiation: The Lambda function starts an AWS Glue Crawler to infer the schema and update the Glue Data Catalog.
Job Orchestration: An Amazon EventBridge rule detects the crawler's SUCCEEDED state and triggers a second Lambda function.
ETL Execution: The second Lambda function launches the AWS Glue ETL job, which runs the generalized_data_cleaner.py script on a dedicated Spark cluster.
Cleaned Data Output: Processed data is written to a target S3 bucket (e.g., s3://your-transformed-data-bucket/transformed_data_folder/) in Parquet format.
Data Quality Metrics: A comprehensive data quality report is saved to S3.
Quarantine Zone: Invalid records are optionally written to a quarantine S3 location for further analysis.

This design ensures scalability, cost-efficiency, and fault tolerance.

Technologies

AWS Services: S3, Glue (ETL, Crawler), Lambda, EventBridge
Framework: Apache Spark (PySpark)
Language: Python


Usage
The pipeline is configured via AWS Glue Job Parameters, allowing customization without code changes. Below are key parameters:
Required Parameters

--JOB_NAME: Unique name for the Glue job run.
--S3_SOURCE_PATH: Input data path (e.g., s3://your-raw-bucket/data/).
--S3_TARGET_PATH: Output data path (e.g., s3://your-cleaned-bucket/data/).

Optional Parameters (Examples)

--S3_QUARANTINE_PATH: s3://your-quarantine-bucket/invalid_records/
--S3_METRICS_PATH: s3://your-metrics-bucket/dq_reports/
--INPUT_FORMAT: csv (or json, parquet)
--OUTPUT_FORMAT: parquet (or csv, json)
--CRITICAL_NULL_COLUMNS: "customer_id,order_id"
--TYPE_CONVERSION_MAP: '{"order_date":"date", "sales_amount":"decimal(10,2)", "is_active":"boolean"}'
--FILL_NULL_STRINGS_WITH: "N/A"
--STANDARDIZE_STRING_CASE: "lower"
--CUSTOM_VALIDATION_RULES_JSON: '[{"column": "age", "rule": "age < 0", "action": "drop_row", "rule_name": "negative_age_check"}]'
--DATE_FORMATS_TO_TRY: "yyyy-MM-dd,MM/dd/yyyy"
--ENABLE_OUTLIER_DETECTION: "true"
--OUTLIER_METHOD: "iqr"


Project Structure
Automatic_ETL_Data_Cleaning_Pipeline/
├── README.md                           # Project documentation
└── glue_scripts/
    ├── generalized_data_cleaner.py     # Main PySpark ETL script
    └── lambda_functions/
        ├── trigger_glue_crawler.py     # Lambda to trigger Glue Crawler
        └── trigger_glue_job.py         # Lambda to trigger Glue ETL Job


Setup Instructions

Configure S3 Buckets:
Create raw, transformed, quarantine, and metrics S3 buckets.


Set Up Glue Crawler:
Configure a Glue Crawler to catalog raw data in the source S3 bucket.


Deploy Lambda Functions:
Deploy trigger_glue_crawler.py and trigger_glue_job.py as Lambda functions.
Assign appropriate IAM roles for S3, Glue, and EventBridge access.


Configure EventBridge:
Create a rule to trigger the second Lambda function on Glue Crawler SUCCEEDED events.


Create Glue ETL Job:
Upload generalized_data_cleaner.py to S3.
Create a Glue job, specifying the script path and required parameters.


Test the Pipeline:
Upload a sample dataset to the raw S3 bucket.
Monitor the pipeline via AWS CloudWatch and verify outputs in the target S3 bucket.




Example
To process a CSV file with customer data:

Upload customers.csv to s3://your-raw-bucket/data/.
The S3 event triggers the pipeline, which:
Infers the schema via Glue Crawler.
Cleans the data (e.g., trims whitespace, converts types, removes duplicates).
Writes cleaned data to s3://your-cleaned-bucket/data/ in Parquet.
Saves a data quality report to s3://your-metrics-bucket/dq_reports/.




Author
Sai Gowtham Reddy Udumula

Version
1.0

License
This project is licensed under the MIT License.
