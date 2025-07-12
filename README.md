Automatic ETL Data Cleaning Pipeline (AWS + PySpark)
This project implements a highly configurable and robust ETL (Extract, Transform, Load) pipeline on AWS using PySpark, designed to automatically clean and transform diverse datasets. It demonstrates a cloud-native approach to data quality, ensuring data is production-ready for analytics and machine learning applications.

Project Overview
The core objective of this pipeline is to automate the process of ingesting raw data from Amazon S3, applying a wide range of data cleaning and transformation rules, and then storing the processed data back into S3. The pipeline is built with extensibility and reusability in mind, allowing its behavior to be customized via job parameters without altering the underlying code.

Key Features
Flexible Data Ingestion: Supports reading data in CSV, JSON, and Parquet formats from S3.

Configurable Data Cleaning:

Automatic trimming of whitespace from all string columns.

Standardization of string case (lowercase, uppercase, or title case).

Removal of non-alphanumeric characters from specified columns.

Intelligent Type Conversion:

Explicit type casting for columns based on a provided mapping.

Automatic inference and conversion for string columns to appropriate data types (numeric, boolean, date, timestamp) by attempting multiple common formats.

Robust handling of numeric overflow during casting (truncation, nullification, or conversion to string).

Comprehensive Null Handling:

Ability to drop entire rows if critical identifier columns contain nulls.

Configurable strategies for filling nulls in non-critical columns based on their data type (strings, numbers, dates, booleans).

Advanced Data Quality & Validation:

Custom Validation Rules: Apply user-defined validation rules with flexible actions (e.g., drop invalid rows, set values to null, set default values, flag records, or quarantine).

Outlier Detection: Integrated methods for identifying outliers in numeric columns (IQR, Z-score, simplified isolation forest).

Duplicate Removal: Configurable to remove duplicates based on a subset of columns or all columns.

Schema Validation: Optional validation of input data schema against a predefined structure.

Data Profiling & Metrics: Generates a comprehensive data quality report, including overall completeness, consistency, validity scores, and detailed column-level statistics. This report is written to S3.

Data Enrichment: Supports the addition of new derived columns using powerful Spark SQL expressions.

Auditability: Automatically adds metadata columns such as ETL load timestamps, Glue job IDs, source paths, and a unique row hash for traceability and lineage tracking.

Performance Optimization: Includes Spark configuration tuning, dynamic repartitioning based on data volume, and caching mechanisms to optimize processing performance.

Architecture & Flow
This pipeline is designed as an an event-driven, serverless architecture on AWS:

Data Landing: Raw data files (e.g., CSVs) are uploaded to a designated S3 bucket (e.g., s3://your-raw-data-bucket/raw_data_folder/).

Event Trigger: An S3 event notification (e.g., ObjectCreated) for the raw data folder triggers an AWS Lambda function.

Crawler Initiation: This initial Lambda function's role is to start an AWS Glue Crawler. The Lambda function completes quickly, avoiding timeouts.

Metadata Cataloging: The Glue Crawler processes the newly uploaded file(s), infers the schema, and updates the AWS Glue Data Catalog, making the data discoverable.

Job Orchestration (EventBridge): Upon successful completion of the Glue Crawler, an Amazon EventBridge rule detects the Crawler State Change event (SUCCEEDED).

ETL Job Trigger: The EventBridge rule then triggers a second AWS Lambda function.

Main ETL Execution: This second Lambda function starts the main AWS Glue ETL job (running the generalized_data_cleaner.py script). This Glue job runs on its own dedicated Spark cluster, allowing for long-running transformations.

Cleaned Data Output: The Glue ETL job performs all the cleaning, transformation, and validation steps, then writes the cleaned data to a separate S3 bucket (e.g., s3://your-transformed-data-bucket/transformed_data_folder/) in a optimized format like Parquet.

Data Quality Metrics Output: If enabled, a data quality report is also written to S3.

Quarantine Zone: Invalid records (based on validation rules) are optionally written to a separate S3 quarantine location for further investigation.

This design ensures a robust, scalable, and cost-effective data cleaning solution.

Technologies Used
AWS Services: S3, Glue (ETL, Crawler), Lambda, EventBridge

Frameworks: Apache Spark (PySpark)

Languages: Python

How to Use (AWS Glue Job Parameters)
This script's behavior is highly customizable through AWS Glue Job Parameters. When configuring the Glue job in the AWS Console, you would provide values for these parameters:

Required Parameters:

--JOB_NAME: Unique name for the Glue job run.

--S3_SOURCE_PATH: Full S3 path to the input raw data (e.g., s3://your-raw-bucket/data/).

--S3_TARGET_PATH: Full S3 path where cleaned data will be written (e.g., s3://your-cleaned-bucket/data/).

Optional Parameters (Examples):

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
├── README.md                           # This file
└── glue_scripts/
    └── generalized_data_cleaner.py     # The main PySpark ETL script
    └── lambda_functions/               # Optional: Lambda functions for orchestration
        ├── trigger_glue_crawler.py     # Lambda to trigger Glue Crawler
        └── trigger_glue_job.py         # Lambda to trigger Glue ETL Job (triggered by EventBridge)

Author
Sai Gowtham Reddy Udumula


Version
1.0
