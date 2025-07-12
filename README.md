# 🚀 Automatic ETL Data Cleaning Pipeline (AWS + PySpark)

A **scalable, serverless, cloud-native ETL pipeline** built with **AWS Glue**, **PySpark**, and **Lambda** to automate **data ingestion**, **cleaning**, **transformation**, and **validation** — tailored for modern data analytics and machine learning workflows.

---

## 📌 Overview

This project delivers a **fully configurable**, production-ready **ETL pipeline** using **PySpark on AWS Glue**, automating every step from raw data ingestion in **Amazon S3** to clean, optimized output in **Parquet** format.

Without touching code, you can customize the pipeline via **AWS Glue Job Parameters**, enabling quick adaptations to diverse datasets and requirements.

---

## ⚙️ Key Features

### 🔄 **Flexible Data Ingestion**

* Supports input formats: **CSV**, **JSON**, **Parquet**
* Ingests data directly from Amazon S3

### 🧹 **Configurable Data Cleaning**

* Trims whitespaces from string fields
* Standardizes case (lower, upper, title)
* Removes non-alphanumeric characters (configurable)

### 🔀 **Intelligent Type Conversion**

* Supports explicit type mapping
* Automatically infers types from strings → numeric, boolean, datetime
* Handles overflow gracefully (truncate, nullify, cast to string)

### ❌ **Comprehensive Null Handling**

* Drop rows with nulls in key columns
* Apply custom fill strategies for others (e.g., strings → `"N/A"`)

### ✅ **Advanced Validation & Quality Checks**

* Custom validation rules (drop/quarantine/set null)
* Outlier detection: **IQR**, **Z-score**, or **isolation forest**
* Duplicate removal (row-level or column-specific)
* Optional schema validation

### 📊 **Data Profiling & Metrics**

* Generates detailed **Data Quality Reports**: Completeness, Consistency, Validity
* Stores reports in S3

### 🧐 **Data Enrichment**

* Add derived columns using **Spark SQL expressions**

### 🗒️ **Auditability**

* Adds metadata: ETL timestamps, job IDs, source paths, row hashes

### ⚡ **Performance Optimization**

* Smart repartitioning, caching, and Spark config tuning for efficiency

---

## 🏗️ Architecture

```text
📀 Raw S3 Data  ← s3://your-raw-data-bucket/
     ⬇
S3 Event Trigger
     ⬇
Lambda (trigger_glue_crawler.py)
     ⬇
AWS Glue Crawler
     ⬇
EventBridge Rule (Crawler SUCCEEDED)
     ⬇
Lambda (trigger_glue_job.py)
     ⬇
AWS Glue ETL Job (PySpark)
     ⬇
Cleaned Data • DQ Reports • Quarantine Output (all in S3)
```

---

## 🧠 Technologies Used

* **AWS Services**: S3, Glue (Crawler + ETL), Lambda, EventBridge
* **Processing Framework**: Apache Spark (PySpark)
* **Language**: Python

---

## 🔧 Usage

You can control the pipeline using **AWS Glue Job Parameters** — no code changes required.

### ✅ Required Parameters

| Parameter          | Description                  |
| ------------------ | ---------------------------- |
| `--JOB_NAME`       | Unique Glue job name         |
| `--S3_SOURCE_PATH` | Path to raw data in S3       |
| `--S3_TARGET_PATH` | Output path for cleaned data |

### 🔄 Optional Parameters (Examples)

```bash
--S3_QUARANTINE_PATH="s3://your-quarantine-bucket/"
--S3_METRICS_PATH="s3://your-metrics-bucket/dq_reports/"
--INPUT_FORMAT="csv"
--OUTPUT_FORMAT="parquet"
--CRITICAL_NULL_COLUMNS="customer_id,order_id"
--TYPE_CONVERSION_MAP='{"order_date":"date","amount":"decimal(10,2)"}'
--STANDARDIZE_STRING_CASE="lower"
--FILL_NULL_STRINGS_WITH="N/A"
--CUSTOM_VALIDATION_RULES_JSON='[{"column":"age","rule":"age < 0","action":"drop_row"}]'
--ENABLE_OUTLIER_DETECTION="true"
--OUTLIER_METHOD="iqr"
--DATE_FORMATS_TO_TRY="yyyy-MM-dd,MM/dd/yyyy"
```

---

## 📁 Project Structure

```
Automatic_ETL_Data_Cleaning_Pipeline/
├── README.md
└── glue_scripts/
    ├── data_cleaner.py          # Main PySpark ETL logic
    └── lambda_functions/
        ├── trigger_glue_crawler.py          # Lambda to start Glue Crawler
        └── trigger_glue_job.py              # Lambda to start Glue Job
```

---

## 🚀 Setup Instructions

1. **Create S3 Buckets**: Raw, transformed, quarantine, metrics
2. **Glue Crawler**: Setup to infer schema from the raw S3 path
3. **Lambda Deployment**:

   * Deploy both Lambda functions
   * Assign necessary IAM permissions
4. **EventBridge Rule**: Trigger Glue job Lambda when crawler finishes
5. **Glue Job**:

   * Upload `generalized_data_cleaner.py` to S3
   * Create Glue job with appropriate parameters
6. **Test It**: Upload a CSV/JSON/Parquet file to the raw bucket — the pipeline does the rest!

---

## 📌 Example Run

Upload a `customers.csv` file to:

```
s3://your-raw-bucket/data/customers.csv
```

The pipeline will:

* Trigger the Glue Crawler & ETL job
* Clean the data (trim strings, convert types, deduplicate, etc.)
* Output to:

  * `s3://your-cleaned-bucket/data/` (Parquet)
  * `s3://your-metrics-bucket/dq_reports/` (Data quality report)
  * `s3://your-quarantine-bucket/` (Invalid records, if any)

---

## 👤 Author

**Sai Gowtham Reddy Udumula**

---

## 📄 License

This project is licensed under the **MIT License**.
Feel free to fork, contribute, or reuse with credit.
