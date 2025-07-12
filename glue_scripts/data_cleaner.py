"""
AWS Glue ETL - Comprehensive Generalized Data Cleaning Job

This script provides a highly configurable and robust PySpark-based ETL framework
for cleaning and transforming diverse datasets, primarily from S3, within an AWS Glue environment.
It is designed to handle common data quality issues, enforce data standards, and provide
detailed data quality metrics.

Key Features:
- **Flexible Data Ingestion:** Supports CSV, JSON, Parquet input formats from S3.
- **Configurable Cleaning:**
    - Trimming whitespace and standardizing string case (lower, upper, title).
    - Removing non-alphanumeric characters from specified columns.
- **Intelligent Type Conversion:**
    - Explicit type casting based on a provided mapping.
    - Automatic inference and conversion for string columns to numeric, boolean, date, and timestamp types (trying multiple date/time formats).
    - Robust handling of numeric overflow (truncate, null, or string conversion).
- **Comprehensive Null Handling:**
    - Dropping rows with nulls in critical identifier columns.
    - Filling nulls in non-critical columns based on data type (strings, numbers, dates, booleans) with configurable default values.
- **Advanced Data Quality & Validation:**
    - **Custom Validation Rules:** Apply user-defined rules with configurable actions (drop row, set null, set default, flag record, quarantine).
    - **Outlier Detection:** Supports IQR, Z-score, and simplified isolation methods for numeric columns.
    - **Quarantine Mechanism:** Segregates invalid records for further analysis without halting the main pipeline.
    - **Duplicate Removal:** Configurable by a subset of columns or across all columns.
- **Data Profiling & Metrics:** Generates and writes a comprehensive data quality report including completeness, consistency, validity scores, and column-level statistics.
- **Data Enrichment:** Supports adding custom derived columns using Spark SQL expressions.
- **Auditability:** Adds ETL load timestamps, job IDs, source paths, and row hashes for traceability.
- **Performance Optimization:** Includes Spark configuration tuning, dynamic repartitioning, and caching.

Usage:
This script is intended to be run as an AWS Glue ETL job. All cleaning and transformation
behaviors are controlled via Glue Job Parameters.

Required Job Parameters:
- `--JOB_NAME`: Name of the Glue job.
- `--S3_SOURCE_PATH`: S3 path to the input raw data (e.g., s3://your-bucket/raw_data/).
- `--S3_TARGET_PATH`: S3 path to write the cleaned and transformed data (e.g., s3://your-bucket/transformed_data/).

Optional Job Parameters (examples):
- `--S3_QUARANTINE_PATH`: S3 path for invalid/quarantined records.
- `--S3_METRICS_PATH`: S3 path for data quality metrics reports.
- `--INPUT_FORMAT`: Input file format (e.g., 'csv', 'json', 'parquet'). Default: 'csv'.
- `--OUTPUT_FORMAT`: Output file format (e.g., 'parquet', 'csv', 'json'). Default: 'parquet'.
- `--CRITICAL_NULL_COLUMNS`: "customer_id,order_id"
- `--TYPE_CONVERSION_MAP`: '{"order_date":"date", "sales_amount":"decimal(10,2)", "is_active":"boolean"}'
- `--FILL_NULL_STRINGS_WITH`: "N/A"
- `--STANDARDIZE_STRING_CASE`: "lower"
- `--CUSTOM_VALIDATION_RULES_JSON`: '[{"column": "age", "rule": "age < 0", "action": "drop_row"}]'
- `--DATE_FORMATS_TO_TRY`: "yyyy-MM-dd,MM/dd/yyyy"

Author: [Sai Gowtham Reddy Udumula]
"""
import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, lit, to_date, to_timestamp, trim, lower, upper, regexp_replace, coalesce, current_timestamp, expr, isnan, isnull
from pyspark.sql.types import (
    DecimalType, DateType, StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType, TimestampType, StructField, StructType
)
from datetime import datetime
import re
import logging
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Enums for better type safety
class ValidationAction(Enum):
    DROP_ROW = "drop_row"
    SET_NULL = "set_null"
    SET_DEFAULT = "set_default"
    FLAG_RECORD = "flag_record"
    QUARANTINE = "quarantine"

class DataQualityLevel(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

class StringCaseMode(Enum):
    LOWER = "lower"
    UPPER = "upper"
    TITLE = "title"
    NONE = "none"

# Data classes for configuration
@dataclass
class DataQualityMetrics:
    """Data quality metrics collection"""
    total_records: int = 0
    duplicate_records: int = 0
    null_records: int = 0
    invalid_records: int = 0
    quarantined_records: int = 0
    type_conversion_failures: int = 0
    validation_failures: int = 0
    completeness_score: float = 0.0
    consistency_score: float = 0.0
    validity_score: float = 0.0
    overall_quality_score: float = 0.0
    column_metrics: Dict[str, Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.column_metrics is None:
            self.column_metrics = {}

@dataclass
class ValidationRule:
    """Enhanced validation rule configuration"""
    column: str
    rule_name: str
    expression: str
    action: ValidationAction
    severity: DataQualityLevel
    default_value: Optional[Any] = None
    error_message: Optional[str] = None
    enabled: bool = True

@dataclass
class DerivedColumn:
    """Enhanced derived column configuration"""
    name: str
    expression: str
    data_type: Optional[str] = None
    description: Optional[str] = None
    dependencies: Optional[List[str]] = None
    
@dataclass
class SchemaValidation:
    """Schema validation configuration"""
    required_columns: List[str]
    optional_columns: List[str]
    column_types: Dict[str, str]
    max_columns: Optional[int] = None
    min_columns: Optional[int] = None

class EnhancedGlueETL:
    """Enhanced AWS Glue ETL processor with comprehensive data cleaning capabilities"""
    
    def __init__(self, args: Dict[str, Any]):
        self.args = args
        self.metrics = DataQualityMetrics()
        self.quarantine_records = []
        self.processing_errors = []
        
        # Initialize Spark and Glue contexts
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(args['JOB_NAME'], args)
        
        # Configure Spark for better performance
        self._configure_spark()
        
        # Parse configuration
        self._parse_configuration()
        
        logger.info("Enhanced Glue ETL processor initialized successfully")
    
    def _configure_spark(self):
        """Configure Spark settings for optimal performance"""
        spark_conf = self.spark.conf
        
        # Memory and serialization optimizations
        spark_conf.set("spark.sql.adaptive.enabled", "true")
        spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark_conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        spark_conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark_conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Cache and checkpoint optimizations
        spark_conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
        spark_conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
        
        logger.info("Spark configuration optimized for performance")
    
    def _parse_configuration(self):
        """Parse and validate all configuration parameters"""
        # Basic paths
        self.source_path = self.args['S3_SOURCE_PATH']
        self.target_path = self.args['S3_TARGET_PATH']
        self.quarantine_path = self.args.get('S3_QUARANTINE_PATH', f"{self.target_path}/quarantine")
        self.metrics_path = self.args.get('S3_METRICS_PATH', f"{self.target_path}/metrics")
        
        # File format configurations
        self.input_format = self.args.get('INPUT_FORMAT', 'csv').lower()
        self.output_format = self.args.get('OUTPUT_FORMAT', 'parquet').lower()
        self.compression = self.args.get('COMPRESSION', 'snappy').lower()
        
        # CSV specific options
        self.csv_delimiter = self.args.get('CSV_DELIMITER', ',')
        self.csv_quote_char = self.args.get('CSV_QUOTE_CHAR', '"')
        self.csv_escape_char = self.args.get('CSV_ESCAPE_CHAR', '\\')
        self.csv_header = self.args.get('CSV_HEADER', 'true').lower() == 'true'
        self.csv_infer_schema = self.args.get('CSV_INFER_SCHEMA', 'false').lower() == 'true'
        
        # Processing options
        self.enable_schema_validation = self.args.get('ENABLE_SCHEMA_VALIDATION', 'false').lower() == 'true'
        self.enable_data_profiling = self.args.get('ENABLE_DATA_PROFILING', 'true').lower() == 'true'
        self.enable_advanced_analytics = self.args.get('ENABLE_ADVANCED_ANALYTICS', 'false').lower() == 'true'
        self.enable_quarantine = self.args.get('ENABLE_QUARANTINE', 'true').lower() == 'true'
        
        # Memory and performance settings
        self.max_records_per_partition = int(self.args.get('MAX_RECORDS_PER_PARTITION', '1000000'))
        self.min_partitions = int(self.args.get('MIN_PARTITIONS', '1'))
        self.max_partitions = int(self.args.get('MAX_PARTITIONS', '200'))
        
        # Parse complex configurations
        self.critical_null_columns = self._parse_comma_separated('CRITICAL_NULL_COLUMNS')
        self.type_conversion_map = self._parse_json('TYPE_CONVERSION_MAP')
        self.drop_duplicate_subset = self._parse_comma_separated('DROP_DUPLICATE_SUBSET')
        self.remove_non_alphanumeric_columns = self._parse_comma_separated('REMOVE_NON_ALPHANUMERIC_FROM_COLUMNS')
        
        # Null filling strategies
        self.fill_null_strings_with = self.args.get('FILL_NULL_STRINGS_WITH')
        self.fill_null_numbers_with = self.args.get('FILL_NULL_NUMBERS_WITH')
        self.fill_null_dates_with = self.args.get('FILL_NULL_DATES_WITH')
        self.fill_null_booleans_with = self.args.get('FILL_NULL_BOOLEANS_WITH')
        
        # String processing options
        self.string_case_mode = StringCaseMode(self.args.get('STANDARDIZE_STRING_CASE', 'none').lower())
        self.trim_strings = self.args.get('TRIM_STRINGS', 'true').lower() == 'true'
        self.remove_extra_spaces = self.args.get('REMOVE_EXTRA_SPACES', 'true').lower() == 'true'
        
        # Date and timestamp formats
        self.date_formats = self._parse_comma_separated('DATE_FORMATS_TO_TRY') or [
            "yyyy-MM-dd", "MM/dd/yyyy", "M/d/yyyy", "dd-MM-yyyy", "dd/MM/yyyy",
            "yyyy/MM/dd", "yyyyMMdd", "MM-dd-yyyy", "dd.MM.yyyy", "yyyy.MM.dd"
        ]
        self.timestamp_formats = self._parse_comma_separated('TIMESTAMP_FORMATS_TO_TRY') or [
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "MM/dd/yyyy HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS", "dd/MM/yyyy HH:mm:ss",
            "yyyy/MM/dd HH:mm:ss", "yyyyMMdd HH:mm:ss", "MM-dd-yyyy HH:mm:ss"
        ]
        
        # Advanced configurations
        self.validation_rules = self._parse_validation_rules()
        self.derived_columns = self._parse_derived_columns()
        self.schema_validation = self._parse_schema_validation()
        
        # Numeric processing options
        self.numeric_precision = int(self.args.get('NUMERIC_PRECISION', '38'))
        self.numeric_scale = int(self.args.get('NUMERIC_SCALE', '10'))
        self.handle_numeric_overflow = self.args.get('HANDLE_NUMERIC_OVERFLOW', 'truncate').lower()
        
        # Outlier detection
        self.enable_outlier_detection = self.args.get('ENABLE_OUTLIER_DETECTION', 'false').lower() == 'true'
        self.outlier_method = self.args.get('OUTLIER_METHOD', 'iqr').lower()  # iqr, zscore, isolation
        self.outlier_threshold = float(self.args.get('OUTLIER_THRESHOLD', '3.0'))
        
        # Data sampling for large datasets
        self.enable_sampling = self.args.get('ENABLE_SAMPLING', 'false').lower() == 'true'
        self.sample_fraction = float(self.args.get('SAMPLE_FRACTION', '0.1'))
        self.sample_seed = int(self.args.get('SAMPLE_SEED', '42'))
        
        logger.info("Configuration parsed successfully")
    
    def _parse_comma_separated(self, arg_name: str) -> List[str]:
        """Parse comma-separated string into list"""
        value = self.args.get(arg_name, '')
        return [item.strip() for item in value.split(',') if item.strip()]
    
    def _parse_json(self, arg_name: str) -> Dict[str, Any]:
        """Parse JSON string into dictionary"""
        json_str = self.args.get(arg_name, '{}')
        try:
            return json.loads(json_str) if json_str else {}
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in {arg_name}: {e}")
            return {}
    
    def _parse_validation_rules(self) -> List[ValidationRule]:
        """Parse validation rules from configuration"""
        rules_json = self._parse_json('CUSTOM_VALIDATION_RULES_JSON')
        rules = []
        
        if isinstance(rules_json, list):
            for rule_dict in rules_json:
                try:
                    rule = ValidationRule(
                        column=rule_dict.get('column'),
                        rule_name=rule_dict.get('rule_name', 'unnamed_rule'),
                        expression=rule_dict.get('expression') or rule_dict.get('rule'),
                        action=ValidationAction(rule_dict.get('action', 'drop_row')),
                        severity=DataQualityLevel(rule_dict.get('severity', 'medium')),
                        default_value=rule_dict.get('default_value') or rule_dict.get('default'),
                        error_message=rule_dict.get('error_message'),
                        enabled=rule_dict.get('enabled', True)
                    )
                    rules.append(rule)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid validation rule: {rule_dict}. Error: {e}")
        
        return rules
    
    def _parse_derived_columns(self) -> List[DerivedColumn]:
        """Parse derived columns from configuration"""
        columns_json = self._parse_json('CUSTOM_DERIVED_COLUMNS_JSON')
        columns = []
        
        if isinstance(columns_json, list):
            for col_dict in columns_json:
                try:
                    column = DerivedColumn(
                        name=col_dict.get('name'),
                        expression=col_dict.get('expression'),
                        data_type=col_dict.get('type') or col_dict.get('data_type'),
                        description=col_dict.get('description'),
                        dependencies=col_dict.get('dependencies', [])
                    )
                    columns.append(column)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid derived column: {col_dict}. Error: {e}")
        
        return columns
    
    def _parse_schema_validation(self) -> Optional[SchemaValidation]:
        """Parse schema validation configuration"""
        schema_json = self._parse_json('SCHEMA_VALIDATION_CONFIG')
        
        if schema_json:
            try:
                return SchemaValidation(
                    required_columns=schema_json.get('required_columns', []),
                    optional_columns=schema_json.get('optional_columns', []),
                    column_types=schema_json.get('column_types', {}),
                    max_columns=schema_json.get('max_columns'),
                    min_columns=schema_json.get('min_columns')
                )
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid schema validation config: {e}")
        
        return None
    
    def read_data(self) -> DataFrame:
        """Read data from source with comprehensive error handling"""
        logger.info(f"Reading data from: {self.source_path}")
        
        try:
            if self.input_format == 'csv':
                source_data = self.glue_context.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={
                        "paths": [self.source_path],
                        "recurse": True,
                        "groupFiles": "inPartition",
                        "groupSize": "134217728"  # 128MB
                    },
                    format="csv",
                    format_options={
                        "withHeader": str(self.csv_header).lower(),
                        "separator": self.csv_delimiter,
                        "quoteChar": self.csv_quote_char,
                        "escapeChar": self.csv_escape_char,
                        "multiline": "true",
                        "inferSchema": str(self.csv_infer_schema).lower()
                    },
                    transformation_ctx="source_data_read"
                )
            elif self.input_format == 'json':
                source_data = self.glue_context.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={"paths": [self.source_path], "recurse": True},
                    format="json",
                    transformation_ctx="source_data_read"
                )
            elif self.input_format == 'parquet':
                source_data = self.glue_context.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={"paths": [self.source_path], "recurse": True},
                    format="parquet",
                    transformation_ctx="source_data_read"
                )
            else:
                raise ValueError(f"Unsupported input format: {self.input_format}")
            
            df = source_data.toDF()
            
            # Initial data sampling if enabled
            if self.enable_sampling and df.count() > 10000000:  # 10M records
                logger.info(f"Sampling {self.sample_fraction * 100}% of data for processing")
                df = df.sample(fraction=self.sample_fraction, seed=self.sample_seed)
            
            # Optimize partitioning
            df = self._optimize_partitioning(df)
            
            # Cache if dataset is reasonable size
            if df.count() < 50000000:  # 50M records
                df.cache()
            
            self.metrics.total_records = df.count()
            logger.info(f"Successfully read {self.metrics.total_records} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading data: {e}")
            raise
    
    def _optimize_partitioning(self, df: DataFrame) -> DataFrame:
        """Optimize DataFrame partitioning for better performance"""
        current_partitions = df.rdd.getNumPartitions()
        optimal_partitions = min(
            max(
                int(df.count() / self.max_records_per_partition),
                self.min_partitions
            ),
            self.max_partitions
        )
        
        if optimal_partitions != current_partitions:
            logger.info(f"Repartitioning from {current_partitions} to {optimal_partitions} partitions")
            df = df.repartition(optimal_partitions)
        
        return df
    
    def validate_schema(self, df: DataFrame) -> DataFrame:
        """Validate DataFrame schema against expected schema"""
        if not self.enable_schema_validation or not self.schema_validation:
            return df
        
        logger.info("Validating schema...")
        
        current_columns = set(df.columns)
        required_columns = set(self.schema_validation.required_columns)
        
        # Check required columns
        missing_columns = required_columns - current_columns
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check column count constraints
        if self.schema_validation.min_columns and len(current_columns) < self.schema_validation.min_columns:
            raise ValueError(f"Too few columns. Expected at least {self.schema_validation.min_columns}, got {len(current_columns)}")
        
        if self.schema_validation.max_columns and len(current_columns) > self.schema_validation.max_columns:
            raise ValueError(f"Too many columns. Expected at most {self.schema_validation.max_columns}, got {len(current_columns)}")
        
        # Validate column types
        for col_name, expected_type in self.schema_validation.column_types.items():
            if col_name in current_columns:
                current_type = str(df.schema[col_name].dataType)
                if expected_type.lower() not in current_type.lower():
                    logger.warning(f"Column {col_name} type mismatch. Expected {expected_type}, got {current_type}")
        
        logger.info("Schema validation completed successfully")
        return df
    
    def profile_data(self, df: DataFrame) -> Dict[str, Any]:
        """Generate comprehensive data profiling report"""
        if not self.enable_data_profiling:
            return {}
        
        logger.info("Starting data profiling...")
        
        profile = {
            'total_records': df.count(),
            'total_columns': len(df.columns),
            'column_profiles': {}
        }
        
        for column in df.columns:
            col_type = str(df.schema[column].dataType)
            col_profile = {
                'data_type': col_type,
                'null_count': df.filter(col(column).isNull()).count(),
                'null_percentage': 0.0,
                'unique_count': df.select(column).distinct().count(),
                'uniqueness_percentage': 0.0
            }
            
            # Calculate percentages
            if profile['total_records'] > 0:
                col_profile['null_percentage'] = (col_profile['null_count'] / profile['total_records']) * 100
                col_profile['uniqueness_percentage'] = (col_profile['unique_count'] / profile['total_records']) * 100
            
            # Type-specific profiling
            if 'string' in col_type.lower():
                col_profile.update(self._profile_string_column(df, column))
            elif any(t in col_type.lower() for t in ['int', 'long', 'float', 'double', 'decimal']):
                col_profile.update(self._profile_numeric_column(df, column))
            elif 'date' in col_type.lower() or 'timestamp' in col_type.lower():
                col_profile.update(self._profile_datetime_column(df, column))
            elif 'boolean' in col_type.lower():
                col_profile.update(self._profile_boolean_column(df, column))
            
            profile['column_profiles'][column] = col_profile
            self.metrics.column_metrics[column] = col_profile
        
        logger.info("Data profiling completed")
        return profile
    
    def _profile_string_column(self, df: DataFrame, column: str) -> Dict[str, Any]:
        """Profile string column statistics"""
        stats = df.select(
            spark_min(length(col(column))).alias('min_length'),
            spark_max(length(col(column))).alias('max_length'),
            avg(length(col(column))).alias('avg_length')
        ).collect()[0]
        
        return {
            'min_length': stats['min_length'],
            'max_length': stats['max_length'],
            'avg_length': round(stats['avg_length'], 2) if stats['avg_length'] else 0,
            'empty_string_count': df.filter(col(column) == "").count(),
            'whitespace_only_count': df.filter(regexp_replace(col(column), r'\s+', '') == "").count()
        }
    
    def _profile_numeric_column(self, df: DataFrame, column: str) -> Dict[str, Any]:
        """Profile numeric column statistics"""
        stats = df.select(
            spark_min(col(column)).alias('min_value'),
            spark_max(col(column)).alias('max_value'),
            avg(col(column)).alias('mean'),
            stddev(col(column)).alias('std_dev'),
            variance(col(column)).alias('variance')
        ).collect()[0]
        
        # Calculate percentiles
        percentiles = df.select(column).approxQuantile(column, [0.25, 0.5, 0.75], 0.01)
        
        return {
            'min_value': stats['min_value'],
            'max_value': stats['max_value'],
            'mean': round(stats['mean'], 4) if stats['mean'] else 0,
            'std_dev': round(stats['std_dev'], 4) if stats['std_dev'] else 0,
            'variance': round(stats['variance'], 4) if stats['variance'] else 0,
            'q1': percentiles[0] if len(percentiles) > 0 else None,
            'median': percentiles[1] if len(percentiles) > 1 else None,
            'q3': percentiles[2] if len(percentiles) > 2 else None,
            'zero_count': df.filter(col(column) == 0).count(),
            'negative_count': df.filter(col(column) < 0).count()
        }
    
    def _profile_datetime_column(self, df: DataFrame, column: str) -> Dict[str, Any]:
        """Profile datetime column statistics"""
        stats = df.select(
            spark_min(col(column)).alias('min_date'),
            spark_max(col(column)).alias('max_date')
        ).collect()[0]
        
        return {
            'min_date': stats['min_date'],
            'max_date': stats['max_date'],
            'date_range_days': (stats['max_date'] - stats['min_date']).days if stats['min_date'] and stats['max_date'] else 0
        }
    
    def _profile_boolean_column(self, df: DataFrame, column: str) -> Dict[str, Any]:
        """Profile boolean column statistics"""
        true_count = df.filter(col(column) == True).count()
        false_count = df.filter(col(column) == False).count()
        total_non_null = true_count + false_count
        
        return {
            'true_count': true_count,
            'false_count': false_count,
            'true_percentage': (true_count / total_non_null * 100) if total_non_null > 0 else 0,
            'false_percentage': (false_count / total_non_null * 100) if total_non_null > 0 else 0
        }
    
    def clean_strings(self, df: DataFrame) -> DataFrame:
        """Comprehensive string cleaning"""
        logger.info("Starting string cleaning...")
        
        string_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        
        for col_name in string_columns:
            # Basic string cleaning
            if self.trim_strings:
                df = df.withColumn(col_name, trim(col(col_name)))
            
            # Remove extra spaces
            if self.remove_extra_spaces:
                df = df.withColumn(col_name, regexp_replace(col(col_name), r'\s+', ' '))
            
            # Case standardization
            if self.string_case_mode == StringCaseMode.LOWER:
                df = df.withColumn(col_name, lower(col(col_name)))
            elif self.string_case_mode == StringCaseMode.UPPER:
                df = df.withColumn(col_name, upper(col(col_name)))
            elif self.string_case_mode == StringCaseMode.TITLE:
                # This is a basic title case, might need more advanced regex for complex cases
                df = df.withColumn(col_name,
                    regexp_replace(lower(col(col_name)), r'\b\w', lambda m: m.group(0).upper()))
            
            # Remove non-alphanumeric characters if specified
            if col_name in self.remove_non_alphanumeric_columns:
                df = df.withColumn(col_name, regexp_replace(col(col_name), r'[^a-zA-Z0-9\s]', ''))
        
        logger.info(f"String cleaning completed for {len(string_columns)} columns")
        return df
    
    def perform_type_conversions(self, df: DataFrame) -> DataFrame:
        """Enhanced type conversion with comprehensive error handling"""
        logger.info("Starting type conversions...")
        
        conversion_failures = 0
        
        for field in df.schema.fields:
            col_name = field.name
            current_type = str(field.dataType)
            target_type = self.type_conversion_map.get(col_name)

            # Skip if column is already of the desired type and no explicit conversion is needed
            if target_type and target_type.lower() in current_type.lower():
                logger.info(f"Column '{col_name}' is already of type {current_type}. Skipping explicit conversion.")
                continue
            
            # Check for explicit type conversion
            if target_type:
                df, success = self._convert_column_type(df, col_name, target_type, explicit=True)
                if not success:
                    conversion_failures += 1
            
            # Automatic type inference for string columns not explicitly mapped or failed explicit cast
            elif isinstance(field.dataType, StringType):
                df, success = self._auto_infer_and_convert(df, col_name)
                if not success:
                    conversion_failures += 1
        
        self.metrics.type_conversion_failures = conversion_failures
        logger.info(f"Type conversions completed with {conversion_failures} failures")
        return df
    
    def _convert_column_type(self, df: DataFrame, col_name: str, target_type: str, explicit: bool = False) -> Tuple[DataFrame, bool]:
        """Convert column to target type with error handling"""
        try:
            original_count = df.filter(col(col_name).isNotNull()).count()
            
            if target_type.lower() == "date":
                df = self._convert_to_date(df, col_name)
            elif target_type.lower() == "timestamp":
                df = self._convert_to_timestamp(df, col_name)
            elif target_type.lower() == "boolean":
                df = self._convert_to_boolean(df, col_name)
            elif target_type.lower() in ["decimal", "numeric"]:
                df = self._convert_to_decimal(df, col_name)
            elif target_type.lower() in ["int", "integer"]:
                df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
            elif target_type.lower() in ["long", "bigint"]:
                df = df.withColumn(col_name, col(col_name).cast(LongType()))
            elif target_type.lower() in ["float", "real"]:
                df = df.withColumn(col_name, col(col_name).cast(FloatType()))
            elif target_type.lower() in ["double", "float8"]:
                df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
            elif target_type.lower() == "string":
                df = df.withColumn(col_name, col(col_name).cast(StringType()))
            else:
                # Handle custom decimal types like decimal(10,2)
                if target_type.lower().startswith("decimal("):
                    precision_scale = target_type[8:-1].split(',')
                    if len(precision_scale) == 2:
                        precision = int(precision_scale[0].strip())
                        scale = int(precision_scale[1].strip())
                        df = df.withColumn(col_name, col(col_name).cast(DecimalType(precision, scale)))
                    else:
                        df = df.withColumn(col_name, col(col_name).cast(DecimalType(self.numeric_precision, self.numeric_scale)))
                else:
                    df = df.withColumn(col_name, col(col_name).cast(target_type))
            
            # Check conversion success rate
            converted_count = df.filter(col(col_name).isNotNull()).count()
            success_rate = converted_count / original_count if original_count > 0 else 1.0
            
            if success_rate < 0.8 and explicit:  # Less than 80% success rate for explicit conversions
                logger.warning(f"Low conversion success rate for {col_name} to {target_type}: {success_rate:.2%}. Consider reviewing data or mapping.")
            
            logger.info(f"Converted {col_name} to {target_type} (success rate: {success_rate:.2%})")
            return df, True
            
        except Exception as e:
            logger.error(f"Failed to convert {col_name} to {target_type}: {e}")
            return df, False
    
    def _convert_to_date(self, df: DataFrame, col_name: str) -> DataFrame:
        """Convert column to date with multiple format attempts"""
        # Create a chain of `when` conditions to try each format
        cast_col_expression = lit(None).cast(DateType())
        for fmt in self.date_formats:
            cast_col_expression = when(to_date(col(col_name), fmt).isNotNull(), to_date(col(col_name), fmt)).otherwise(cast_col_expression)
        
        return df.withColumn(col_name, cast_col_expression)
    
    def _convert_to_timestamp(self, df: DataFrame, col_name: str) -> DataFrame:
        """Convert column to timestamp with multiple format attempts"""
        # Create a chain of `when` conditions to try each format
        cast_col_expression = lit(None).cast(TimestampType())
        for fmt in self.timestamp_formats:
            cast_col_expression = when(to_timestamp(col(col_name), fmt).isNotNull(), to_timestamp(col(col_name), fmt)).otherwise(cast_col_expression)
        
        return df.withColumn(col_name, cast_col_expression)
    
    def _convert_to_boolean(self, df: DataFrame, col_name: str) -> DataFrame:
        """Convert column to boolean with comprehensive value mapping"""
        return df.withColumn(col_name,
            when(lower(trim(col(col_name))).isin(['true', 't', 'yes', 'y', '1', 'on']), True)
            .when(lower(trim(col(col_name))).isin(['false', 'f', 'no', 'n', '0', 'off']), False)
            .otherwise(None).cast(BooleanType())
        )
    
    def _convert_to_decimal(self, df: DataFrame, col_name: str) -> DataFrame:
        """Convert column to decimal with overflow handling"""
        try:
            # First try with configured precision and scale
            df_converted = df.withColumn(col_name, col(col_name).cast(DecimalType(self.numeric_precision, self.numeric_scale)))
            
            # Identify values that became null due to cast failure (potential overflow or invalid format)
            # This is a common way to detect overflow/truncation during cast
            overflow_or_invalid_count = df_converted.filter(col(col_name).isNull()).count() - df.filter(col(col_name).isNull()).count()
            
            if overflow_or_invalid_count > 0:
                logger.warning(f"Decimal conversion issues detected in {col_name}: {overflow_or_invalid_count} values could not be cast to Decimal({self.numeric_precision},{self.numeric_scale}).")
                
                if self.handle_numeric_overflow == 'truncate':
                    # Truncate to max/min values if they exceed bounds after a broader cast
                    # This requires casting to a larger decimal first to identify true overflows
                    df_temp_large_decimal = df.withColumn("temp_col_large_decimal", col(col_name).cast(DecimalType(38,18))) # Larger decimal type
                    
                    max_val = Decimal(10**self.numeric_precision - 1) / Decimal(10**self.numeric_scale)
                    min_val = -max_val
                    
                    df_converted = df_temp_large_decimal.withColumn(col_name,
                        when(col("temp_col_large_decimal").isNotNull() & (col("temp_col_large_decimal") > max_val), lit(max_val).cast(DecimalType(self.numeric_precision, self.numeric_scale)))
                        .when(col("temp_col_large_decimal").isNotNull() & (col("temp_col_large_decimal") < min_val), lit(min_val).cast(DecimalType(self.numeric_precision, self.numeric_scale)))
                        .otherwise(col("temp_col_large_decimal").cast(DecimalType(self.numeric_precision, self.numeric_scale))) # Re-cast to target precision/scale
                    ).drop("temp_col_large_decimal")
                    logger.info(f"Handled overflow in '{col_name}' by truncating values.")
                
                elif self.handle_numeric_overflow == 'null':
                    # Values that failed cast are already null, so no further action needed here
                    logger.info(f"Handled overflow in '{col_name}' by setting values to NULL.")
                
                elif self.handle_numeric_overflow == 'string':
                    # Convert problematic values back to string
                    df_converted = df.withColumn(col_name,
                        when(col(col_name).cast(DecimalType(self.numeric_precision, self.numeric_scale)).isNull() & col(col_name).isNotNull(), col(col_name).cast(StringType()))
                        .otherwise(col(col_name).cast(DecimalType(self.numeric_precision, self.numeric_scale)))
                    )
                    logger.info(f"Handled overflow in '{col_name}' by converting problematic values to StringType.")
            
            return df_converted
            
        except Exception as e:
            logger.error(f"Decimal conversion failed for {col_name}: {e}. Returning original DataFrame for this column.")
            return df # Return original DF if the whole conversion logic fails
    
    def _auto_infer_and_convert(self, df: DataFrame, col_name: str) -> Tuple[DataFrame, bool]:
        """Auto-infer and convert string column to appropriate type based on sample data."""
        logger.info(f"Attempting to auto-infer type for string column: '{col_name}'")
        try:
            # Collect a sample of non-null values for inference
            sample_values = df.select(col(col_name)).filter(col(col_name).isNotNull()).limit(10000).rdd.map(lambda row: row[0]).collect()
            
            if not sample_values:
                logger.info(f"No non-null values in '{col_name}' to infer type. Keeping as StringType.")
                return df, True
            
            # Prioritize more specific types first
            # Try boolean conversion
            if self._is_boolean_column(sample_values):
                return self._convert_column_type(df, col_name, "boolean")
            
            # Try numeric conversion
            if self._is_numeric_column(sample_values):
                if self._is_integer_column(sample_values):
                    return self._convert_column_type(df, col_name, "long") # Use long to prevent overflow for integers
                else:
                    return self._convert_column_type(df, col_name, "decimal")
            
            # Try timestamp conversion (more specific than date)
            if self._is_timestamp_column(sample_values):
                return self._convert_column_type(df, col_name, "timestamp")
            
            # Try date conversion
            if self._is_date_column(sample_values):
                return self._convert_column_type(df, col_name, "date")
            
            logger.info(f"Could not infer a more specific type for '{col_name}'. Keeping as StringType.")
            return df, True # No conversion, but considered successful as it's still a string
            
        except Exception as e:
            logger.error(f"Auto-inference failed for '{col_name}': {e}. Keeping as StringType.")
            return df, False
    
    def _is_numeric_column(self, values: List[str]) -> bool:
        """Checks if a significant majority of non-null string values can be cast to a numeric type."""
        if not values: return False
        numeric_count = 0
        for value in values:
            try:
                # Attempt to convert to float after cleaning common numeric formatting (e.g., commas)
                float(str(value).strip().replace(',', ''))
                numeric_count += 1
            except (ValueError, TypeError):
                pass
        return (numeric_count / len(values)) > 0.95 # Require high confidence for auto-conversion
    
    def _is_integer_column(self, values: List[str]) -> bool:
        """Checks if a significant majority of non-null string values can be cast to an integer type."""
        if not values: return False
        integer_count = 0
        for value in values:
            try:
                val = float(str(value).strip().replace(',', ''))
                if val.is_integer():
                    integer_count += 1
            except (ValueError, TypeError):
                pass
        return (integer_count / len(values)) > 0.98 # Even higher confidence for integer
    
    def _is_boolean_column(self, values: List[str]) -> bool:
        """Checks if a significant majority of non-null string values are boolean-like."""
        if not values: return False
        boolean_map = {'true', 'false', 't', 'f', 'yes', 'no', 'y', 'n', '1', '0', 'on', 'off'}
        boolean_count = sum(1 for value in values if str(value).lower().strip() in boolean_map)
        return (boolean_count / len(values)) > 0.95 # High confidence
    
    def _is_date_column(self, values: List[str]) -> bool:
        """Checks if a significant majority of non-null string values can be parsed as dates."""
        if not values: return False
        date_parse_success_count = 0
        for value in values:
            for fmt in self.date_formats:
                try:
                    datetime.strptime(str(value), fmt.replace('y', '%Y').replace('M', '%m').replace('d', '%d'))
                    date_parse_success_count += 1
                    break # Found a format, move to next value
                except (ValueError, TypeError):
                    pass
        return (date_parse_success_count / len(values)) > 0.90 # Good confidence
    
    def _is_timestamp_column(self, values: List[str]) -> bool:
        """Checks if a significant majority of non-null string values can be parsed as timestamps."""
        if not values: return False
        timestamp_parse_success_count = 0
        for value in values:
            for fmt in self.timestamp_formats:
                try:
                    datetime.strptime(str(value), fmt.replace('y', '%Y').replace('M', '%m').replace('d', '%d').replace('H', '%H').replace('m', '%M').replace('s', '%S').replace('S', '%f'))
                    timestamp_parse_success_count += 1
                    break
                except (ValueError, TypeError):
                    pass
        return (timestamp_parse_success_count / len(values)) > 0.90
    
    def handle_nulls(self, df: DataFrame) -> DataFrame:
        """Comprehensive null handling strategy"""
        logger.info("Starting null handling...")
        
        # Drop rows with critical nulls first
        if self.critical_null_columns:
            existing_critical_columns = [c for c in self.critical_null_columns if c in df.columns]
            if existing_critical_columns:
                initial_count = df.count()
                df = df.na.drop(subset=existing_critical_columns)
                dropped_count = initial_count - df.count()
                self.metrics.null_records += dropped_count # Count records dropped due to critical nulls
                logger.info(f"Dropped {dropped_count} rows with critical nulls in columns: {existing_critical_columns}")
        
        # Fill nulls based on column types for non-critical columns
        for field in df.schema.fields:
            col_name = field.name
            
            if col_name in self.critical_null_columns or col_name.startswith('etl_'): # Skip critical and audit columns
                continue
            
            null_count_before_fill = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
            
            if null_count_before_fill > 0:
                if isinstance(field.dataType, StringType) and self.fill_null_strings_with is not None:
                    df = df.withColumn(col_name, coalesce(col(col_name), lit(self.fill_null_strings_with)))
                    logger.info(f"Filled {null_count_before_fill} nulls/NaNs in string column '{col_name}' with '{self.fill_null_strings_with}'.")
                
                elif isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                    if self.fill_null_numbers_with is not None:
                        try:
                            fill_value = float(self.fill_null_numbers_with)
                            df = df.withColumn(col_name, coalesce(col(col_name), lit(fill_value).cast(field.dataType)))
                            logger.info(f"Filled {null_count_before_fill} nulls/NaNs in numeric column '{col_name}' with '{fill_value}'.")
                        except ValueError:
                            logger.warning(f"Invalid numeric fill value '{self.fill_null_numbers_with}' for column '{col_name}'. Skipping fill.")
                
                elif isinstance(field.dataType, (DateType, TimestampType)):
                    if self.fill_null_dates_with is not None:
                        try:
                            if self.fill_null_dates_with.lower() == "current_timestamp":
                                fill_value_expr = current_timestamp()
                            else:
                                # Try to cast the fill value string to the column's specific date/timestamp type
                                if isinstance(field.dataType, DateType):
                                    fill_value_expr = to_date(lit(self.fill_null_dates_with), self.date_formats[0]) # Use first format
                                else: # TimestampType
                                    fill_value_expr = to_timestamp(lit(self.fill_null_dates_with), self.timestamp_formats[0]) # Use first format
                            
                            df = df.withColumn(col_name, coalesce(col(col_name), fill_value_expr))
                            logger.info(f"Filled {null_count_before_fill} nulls/NaNs in date/timestamp column '{col_name}' with '{self.fill_null_dates_with}'.")
                        except Exception as e:
                            logger.warning(f"Invalid date/timestamp fill value '{self.fill_null_dates_with}' for column '{col_name}': {e}. Skipping fill.")
                
                elif isinstance(field.dataType, BooleanType):
                    if self.fill_null_booleans_with is not None:
                        try:
                            fill_value = self.fill_null_booleans_with.lower() == 'true'
                            df = df.withColumn(col_name, coalesce(col(col_name), lit(fill_value).cast(BooleanType())))
                            logger.info(f"Filled {null_count_before_fill} nulls/NaNs in boolean column '{col_name}' with '{fill_value}'.")
                        except Exception as e:
                            logger.warning(f"Invalid boolean fill value '{self.fill_null_booleans_with}' for column '{col_name}': {e}. Skipping fill.")
            else:
                logger.debug(f"No nulls/NaNs found in column '{col_name}' or no fill strategy provided for its type.")
        
        logger.info("Null handling completed")
        return df
    
    def apply_validation_rules(self, df: DataFrame) -> DataFrame:
        """Apply custom validation rules with comprehensive actions"""
        if not self.validation_rules:
            return df
        
        logger.info("Applying validation rules...")
        
        quarantine_df = None
        
        for rule in self.validation_rules:
            if not rule.enabled or rule.column not in df.columns:
                logger.info(f"Skipping disabled or non-existent column rule: {rule.rule_name} on {rule.column}")
                continue
            
            try:
                condition = expr(rule.expression)
                initial_count = df.count()
                
                if rule.action == ValidationAction.DROP_ROW:
                    df = df.filter(~condition)
                    dropped_count = initial_count - df.count()
                    self.metrics.validation_failures += dropped_count
                    logger.info(f"Rule '{rule.rule_name}': Dropped {dropped_count} rows from {rule.column}")
                
                elif rule.action == ValidationAction.SET_NULL:
                    df = df.withColumn(rule.column, when(condition, lit(None)).otherwise(col(rule.column)))
                    logger.info(f"Rule '{rule.rule_name}': Set values to NULL in {rule.column}")
                
                elif rule.action == ValidationAction.SET_DEFAULT:
                    if rule.default_value is not None:
                        df = df.withColumn(rule.column,
                            when(condition, lit(rule.default_value).cast(df.schema[rule.column].dataType))
                            .otherwise(col(rule.column)))
                        logger.info(f"Rule '{rule.rule_name}': Set default values in {rule.column}")
                    else:
                        logger.warning(f"Rule '{rule.rule_name}': Default value not provided for SET_DEFAULT action. Skipping.")
                
                elif rule.action == ValidationAction.FLAG_RECORD:
                    flag_col = f"{rule.column}_{rule.rule_name}_flag"
                    df = df.withColumn(flag_col, when(condition, lit(True)).otherwise(lit(False)))
                    logger.info(f"Rule '{rule.rule_name}': Added validation flag column '{flag_col}'")
                
                elif rule.action == ValidationAction.QUARANTINE:
                    if self.enable_quarantine:
                        invalid_records = df.filter(condition)
                        if invalid_records.count() > 0:
                            # Add metadata to quarantined records
                            quarantine_records_with_reason = invalid_records.withColumn(
                                "quarantine_reason", lit(rule.rule_name)
                            ).withColumn(
                                "quarantine_timestamp", current_timestamp()
                            ).withColumn(
                                "original_row_hash", md5(concat_ws("|", *[col(c) for c in df.columns if c not in ['etl_load_timestamp', 'etl_job_id', 'etl_source_path']]))
                            )
                            
                            if quarantine_df is None:
                                quarantine_df = quarantine_records_with_reason
                            else:
                                # Ensure schema compatibility before union
                                existing_cols = set(quarantine_df.columns)
                                new_cols = set(quarantine_records_with_reason.columns)
                                
                                # Add missing columns to both DataFrames with nulls
                                for missing_col in new_cols - existing_cols:
                                    quarantine_df = quarantine_df.withColumn(missing_col, lit(None))
                                for missing_col in existing_cols - new_cols:
                                    quarantine_records_with_reason = quarantine_records_with_reason.withColumn(missing_col, lit(None))
                                
                                # Reorder columns to match for union
                                quarantine_records_with_reason = quarantine_records_with_reason.select(quarantine_df.columns)
                                
                                quarantine_df = quarantine_df.unionByName(quarantine_records_with_reason, allowMissingColumns=True)
                            
                            # Remove quarantined records from main DataFrame
                            df = df.filter(~condition)
                            quarantined_count = initial_count - df.count()
                            self.metrics.quarantined_records += quarantined_count
                            logger.info(f"Rule '{rule.rule_name}': Quarantined {quarantined_count} records from {rule.column}")
                        else:
                            logger.info(f"Rule '{rule.rule_name}': No records to quarantine for {rule.column}")
                    else:
                        logger.info(f"Quarantine is disabled. Rule '{rule.rule_name}' would have quarantined records.")
            
            except Exception as e:
                logger.error(f"Error applying validation rule '{rule.rule_name}' on column '{rule.column}': {e}. Skipping rule.")
        
        # Write quarantine records if any
        if quarantine_df is not None and self.enable_quarantine:
            self._write_quarantine_data(quarantine_df)
        
        logger.info(f"Validation rules applied. Total validation failures: {self.metrics.validation_failures}")
        return df
    
    def detect_and_handle_outliers(self, df: DataFrame) -> DataFrame:
        """Detect and handle outliers in numeric columns"""
        if not self.enable_outlier_detection:
            return df
        
        logger.info("Starting outlier detection...")
        
        numeric_columns = [f.name for f in df.schema.fields
                           if isinstance(f.dataType, (IntegerType, LongType, FloatType, DoubleType, DecimalType))]
        
        for col_name in numeric_columns:
            if self.outlier_method == 'iqr':
                df = self._detect_outliers_iqr(df, col_name)
            elif self.outlier_method == 'zscore':
                df = self._detect_outliers_zscore(df, col_name)
            elif self.outlier_method == 'isolation':
                df = self._detect_outliers_isolation(df, col_name)
        
        logger.info("Outlier detection completed")
        return df
    
    def _detect_outliers_iqr(self, df: DataFrame, col_name: str) -> DataFrame:
        """Detect outliers using IQR method"""
        try:
            quantiles = df.select(col_name).approxQuantile(col_name, [0.25, 0.75], 0.01)
            if len(quantiles) == 2:
                q1, q3 = quantiles
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                outlier_flag_col = f"{col_name}_outlier_iqr"
                df = df.withColumn(outlier_flag_col,
                    when((col(col_name) < lower_bound) | (col(col_name) > upper_bound), True)
                    .otherwise(False))
                
                outlier_count = df.filter(col(outlier_flag_col) == True).count()
                logger.info(f"Detected {outlier_count} IQR outliers in {col_name}")
            
        except Exception as e:
            logger.error(f"IQR outlier detection failed for {col_name}: {e}")
        
        return df
    
    def _detect_outliers_zscore(self, df: DataFrame, col_name: str) -> DataFrame:
        """Detect outliers using Z-score method"""
        try:
            stats = df.select(avg(col(col_name)).alias('mean'), stddev(col(col_name)).alias('std')).collect()[0]
            
            if stats['std'] and stats['std'] > 0:
                mean_val = stats['mean']
                std_val = stats['std']
                
                outlier_flag_col = f"{col_name}_outlier_zscore"
                df = df.withColumn(outlier_flag_col,
                    when(spark_abs((col(col_name) - mean_val) / std_val) > self.outlier_threshold, True)
                    .otherwise(False))
                
                outlier_count = df.filter(col(outlier_flag_col) == True).count()
                logger.info(f"Detected {outlier_count} Z-score outliers in {col_name}")
            
        except Exception as e:
            logger.error(f"Z-score outlier detection failed for {col_name}: {e}")
        
        return df
    
    def _detect_outliers_isolation(self, df: DataFrame, col_name: str) -> DataFrame:
        """Detect outliers using a simplified isolation method (quantile-based)"""
        try:
            # Using quantiles as a proxy for isolation forest due to complexity of full implementation in Glue
            quantiles = df.select(col_name).approxQuantile(col_name, [0.01, 0.99], 0.01)
            if len(quantiles) == 2:
                lower_bound, upper_bound = quantiles
                
                outlier_flag_col = f"{col_name}_outlier_isolation"
                df = df.withColumn(outlier_flag_col,
                    when((col(col_name) < lower_bound) | (col(col_name) > upper_bound), True)
                    .otherwise(False))
                
                outlier_count = df.filter(col(outlier_flag_col) == True).count()
                logger.info(f"Detected {outlier_count} isolation (quantile-based) outliers in {col_name}")
            
        except Exception as e:
            logger.error(f"Isolation outlier detection failed for {col_name}: {e}")
        
        return df
    
    def remove_duplicates(self, df: DataFrame) -> DataFrame:
        """Remove duplicates with configurable strategy"""
        logger.info("Starting duplicate removal...")
        
        initial_count = df.count()
        
        if self.drop_duplicate_subset:
            existing_columns = [c for c in self.drop_duplicate_subset if c in df.columns]
            if existing_columns:
                df = df.dropDuplicates(subset=existing_columns)
                logger.info(f"Removed duplicates based on subset: {existing_columns}")
            else:
                logger.warning("No valid columns found in duplicate subset, using all columns")
                df = df.dropDuplicates()
        else:
            df = df.dropDuplicates()
        
        duplicate_count = initial_count - df.count()
        self.metrics.duplicate_records = duplicate_count
        logger.info(f"Removed {duplicate_count} duplicate records")
        
        return df
    
    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        """Add custom derived columns"""
        if not self.derived_columns:
            return df
        
        logger.info("Adding derived columns...")
        
        for derived_col in self.derived_columns:
            try:
                # Check dependencies
                if derived_col.dependencies:
                    missing_deps = [dep for dep in derived_col.dependencies if dep not in df.columns]
                    if missing_deps:
                        logger.warning(f"Missing dependencies for {derived_col.name}: {missing_deps}. Skipping derived column.")
                        continue
                
                # Add the derived column
                df = df.withColumn(derived_col.name, expr(derived_col.expression))
                
                # Cast to specified type if provided
                if derived_col.data_type:
                    df = df.withColumn(derived_col.name, col(derived_col.name).cast(derived_col.data_type))
                
                logger.info(f"Added derived column: {derived_col.name}")
                
            except Exception as e:
                logger.error(f"Failed to add derived column {derived_col.name}: {e}. Skipping.")
        
        logger.info("Derived columns added successfully")
        return df
    
    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        """Add comprehensive audit columns"""
        logger.info("Adding audit columns...")
        
        # Add timestamp
        df = df.withColumn("etl_load_timestamp", current_timestamp())
        
        # Add job ID
        df = df.withColumn("etl_job_id", lit(self.args.get('JOB_RUN_ID', 'unknown')))
        
        # Add source path
        df = df.withColumn("etl_source_path", lit(self.source_path))
        
        # Add row hash for tracking (excluding audit columns from hash calculation)
        non_audit_cols = [c for c in df.columns if not c.startswith('etl_')]
        if non_audit_cols:
            df = df.withColumn("etl_row_hash", md5(concat_ws("|", *[col(c) for c in non_audit_cols])))
        else:
            df = df.withColumn("etl_row_hash", lit(None).cast(StringType())) # No columns to hash
        
        # Add data quality score if enabled
        if self.enable_data_profiling:
            df = df.withColumn("etl_quality_score", lit(self.metrics.overall_quality_score))
        
        logger.info("Audit columns added successfully")
        return df
    
    def calculate_quality_metrics(self, df: DataFrame) -> None:
        """Calculate comprehensive data quality metrics"""
        logger.info("Calculating quality metrics...")
        
        total_records = df.count()
        self.metrics.total_records = total_records
        
        # Calculate completeness (non-null percentage)
        total_non_audit_fields = len([c for c in df.columns if not c.startswith('etl_')])
        total_null_values_in_non_audit_cols = 0
        for col_name in [c for c in df.columns if not c.startswith('etl_')]:
            total_null_values_in_non_audit_cols += df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
        
        total_possible_values = total_records * total_non_audit_fields
        
        self.metrics.completeness_score = ((total_possible_values - total_null_values_in_non_audit_cols) / total_possible_values * 100) if total_possible_values > 0 else 100
        
        # Consistency (duplicate percentage is already captured in self.metrics.duplicate_records)
        # Recalculate consistency score based on actual unique records after processing
        unique_records_after_dedup = df.select([c for c in df.columns if not c.startswith('etl_')]).distinct().count()
        self.metrics.consistency_score = (unique_records_after_dedup / total_records * 100) if total_records > 0 else 100
        
        # Validity (successful validation percentage)
        # self.metrics.validation_failures and self.metrics.type_conversion_failures are accumulated
        total_invalid_records_after_processing = self.metrics.validation_failures + self.metrics.type_conversion_failures + self.metrics.quarantined_records
        self.metrics.validity_score = ((total_records - total_invalid_records_after_processing) / total_records * 100) if total_records > 0 else 100
        
        # Calculate overall quality score
        self.metrics.overall_quality_score = (
            self.metrics.completeness_score * 0.4 +
            self.metrics.consistency_score * 0.3 +
            self.metrics.validity_score * 0.3
        )
        
        logger.info(f"Quality metrics calculated - Overall Score: {self.metrics.overall_quality_score:.2f}%")
    
    def _write_quarantine_data(self, quarantine_df: DataFrame) -> None:
        """Write quarantine records to separate location"""
        try:
            quarantine_dynamic_frame = DynamicFrame.fromDF(quarantine_df, self.glue_context, "quarantine_data")
            
            self.glue_context.write_dynamic_frame.from_options(
                frame=quarantine_dynamic_frame,
                connection_type="s3",
                connection_options={"path": self.quarantine_path},
                format="parquet",
                transformation_ctx="quarantine_data_write"
            )
            
            logger.info(f"Quarantine data written to: {self.quarantine_path}")
            
        except Exception as e:
            logger.error(f"Failed to write quarantine data: {e}")
    
    def write_data(self, df: DataFrame) -> None:
        """Write processed data to target location"""
        logger.info(f"Writing cleaned data to: {self.target_path}")
        
        try:
            # Convert to DynamicFrame
            cleaned_dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, "cleaned_data")
            
            # Write options based on format
            if self.output_format == 'parquet':
                self.glue_context.write_dynamic_frame.from_options(
                    frame=cleaned_dynamic_frame,
                    connection_type="s3",
                    connection_options={"path": self.target_path},
                    format="parquet",
                    format_options={"compression": self.compression},
                    transformation_ctx="cleaned_data_write"
                )
            elif self.output_format == 'csv':
                self.glue_context.write_dynamic_frame.from_options(
                    frame=cleaned_dynamic_frame,
                    connection_type="s3",
                    connection_options={"path": self.target_path},
                    format="csv",
                    format_options={
                        "withHeader": "true",
                        "separator": self.csv_delimiter,
                        "quoteChar": self.csv_quote_char
                    },
                    transformation_ctx="cleaned_data_write"
                )
            elif self.output_format == 'json':
                self.glue_context.write_dynamic_frame.from_options(
                    frame=cleaned_dynamic_frame,
                    connection_type="s3",
                    connection_options={"path": self.target_path},
                    format="json",
                    transformation_ctx="cleaned_data_write"
                )
            
            logger.info("Data written successfully")
            
        except Exception as e:
            logger.error(f"Error writing data: {e}")
            raise
    
    def write_metrics(self) -> None:
        """Write data quality metrics to S3"""
        try:
            metrics_dict = asdict(self.metrics)
            metrics_dict['processing_timestamp'] = datetime.now().isoformat()
            metrics_dict['job_name'] = self.args['JOB_NAME']
            metrics_dict['source_path'] = self.source_path
            metrics_dict['target_path'] = self.target_path
            
            # Convert to DataFrame and write
            metrics_df = self.spark.createDataFrame([metrics_dict])
            metrics_dynamic_frame = DynamicFrame.fromDF(metrics_df, self.glue_context, "metrics_data")
            
            self.glue_context.write_dynamic_frame.from_options(
                frame=metrics_dynamic_frame,
                connection_type="s3",
                connection_options={"path": self.metrics_path},
                format="json",
                transformation_ctx="metrics_write"
            )
            
            logger.info(f"Metrics written to: {self.metrics_path}")
            
        except Exception as e:
            logger.error(f"Failed to write metrics: {e}")
    
    def run(self) -> None:
        """Main execution pipeline"""
        try:
            logger.info("Starting Enhanced Glue ETL Job")
            
            # 1. Read data
            df = self.read_data()
            
            # 2. Validate schema
            df = self.validate_schema(df)
            
            # 3. Profile data
            profile = self.profile_data(df)
            
            # 4. Clean strings
            df = self.clean_strings(df)
            
            # 5. Perform type conversions
            df = self.perform_type_conversions(df)
            
            # 6. Handle nulls
            df = self.handle_nulls(df)
            
            # 7. Apply validation rules
            df = self.apply_validation_rules(df)
            
            # 8. Detect outliers
            df = self.detect_and_handle_outliers(df)
            
            # 9. Remove duplicates
            df = self.remove_duplicates(df)
            
            # 10. Add derived columns
            df = self.add_derived_columns(df)
            
            # 11. Add audit columns
            df = self.add_audit_columns(df)
            
            # 12. Calculate final quality metrics
            self.calculate_quality_metrics(df)
            
            # 13. Write processed data
            self.write_data(df)
            
            # 14. Write data quality metrics
            if self.enable_data_profiling:
                self.write_metrics()
            
            logger.info("Enhanced Glue ETL Job finished successfully.")
            
        except Exception as e:
            logger.error(f"Enhanced Glue ETL Job failed: {e}", exc_info=True)
            self.processing_errors.append(str(e))
            raise # Re-raise the exception to fail the Glue job
        finally:
            self.job.commit() # Commit the Glue job even if there's an error


# Main entry point for Glue job
if __name__ == '__main__':
    # Job parameters for configuration
    # S3_SOURCE_PATH: Path to the input data (e.g., s3://your-bucket/raw_data/)
    # S3_TARGET_PATH: Path to write the cleaned data (e.g., s3://your-bucket/transformed_data/)
    # S3_QUARANTINE_PATH: Optional path for quarantined records (defaults to target_path/quarantine)
    # S3_METRICS_PATH: Optional path for data quality metrics (defaults to target_path/metrics)
    # INPUT_FORMAT: Input file format (e.g., 'csv', 'json', 'parquet')
    # OUTPUT_FORMAT: Output file format (e.g., 'parquet', 'csv', 'json')
    # COMPRESSION: Output compression codec (e.g., 'snappy', 'gzip', 'brotli')
    # CSV_DELIMITER: Delimiter for CSV files (e.g., ',')
    # CSV_QUOTE_CHAR: Quote character for CSV files (e.g., '"')
    # CSV_ESCAPE_CHAR: Escape character for CSV files (e.g., '\\')
    # CSV_HEADER: 'true' or 'false' if CSV has a header row
    # CSV_INFER_SCHEMA: 'true' or 'false' to infer schema for CSV
    # ENABLE_SCHEMA_VALIDATION: 'true' or 'false' to enable schema validation
    # ENABLE_DATA_PROFILING: 'true' or 'false' to enable data profiling and metrics
    # ENABLE_ADVANCED_ANALYTICS: 'true' or 'false' to enable advanced analytics (e.g., outlier detection)
    # ENABLE_QUARANTINE: 'true' or 'false' to enable quarantining of invalid records
    # MAX_RECORDS_PER_PARTITION: Target number of records per Spark partition
    # MIN_PARTITIONS: Minimum number of Spark partitions
    # MAX_PARTITIONS: Maximum number of Spark partitions
    # CRITICAL_NULL_COLUMNS: Comma-separated list of columns where nulls mean drop row (e.g., "customer_id,order_id")
    # TYPE_CONVERSION_MAP: JSON string for explicit type conversions (e.g., '{"col1":"date","col2":"decimal(10,2)"}')
    # DROP_DUPLICATE_SUBSET: Comma-separated list of columns to consider for duplicates (e.g., "order_id,product_id")
    # FILL_NULL_STRINGS_WITH: Value to fill nulls in string columns (e.g., "N/A")
    # FILL_NULL_NUMBERS_WITH: Value to fill nulls in numeric columns (e.g., "0")
    # FILL_NULL_DATES_WITH: Value to fill nulls in date/timestamp columns (e.g., "2000-01-01" or "current_timestamp")
    # FILL_NULL_BOOLEANS_WITH: Value to fill nulls in boolean columns (e.g., "false")
    # STANDARDIZE_STRING_CASE: 'lower', 'upper', or 'none' for string case standardization
    # REMOVE_NON_ALPHANUMERIC_FROM_COLUMNS: Comma-separated list of columns to clean non-alphanumeric characters
    # CUSTOM_DERIVED_COLUMNS_JSON: JSON string for custom derived columns (e.g., '[{"name": "col_name", "expression": "sql_expr", "type": "str_type"}]')
    # CUSTOM_VALIDATION_RULES_JSON: JSON string for custom validation rules (e.g., '[{"column": "col_name", "rule": "sql_expr_condition", "action": "drop_row|set_null|set_default|flag_record|quarantine", "default": "value"}]')
    # DATE_FORMATS_TO_TRY: Comma-separated list of date formats to attempt for string-to-date conversion
    # TIMESTAMP_FORMATS_TO_TRY: Comma-separated list of timestamp formats to attempt for string-to-timestamp conversion
    # NUMERIC_PRECISION: Default precision for decimal types (e.g., '38')
    # NUMERIC_SCALE: Default scale for decimal types (e.g., '10')
    # HANDLE_NUMERIC_OVERFLOW: 'truncate', 'null', or 'string' for handling numeric overflow during cast
    # ENABLE_OUTLIER_DETECTION: 'true' or 'false' to enable outlier detection
    # OUTLIER_METHOD: 'iqr', 'zscore', or 'isolation' for outlier detection method
    # OUTLIER_THRESHOLD: Threshold for outlier detection (e.g., 3.0 for Z-score)
    # ENABLE_SAMPLING: 'true' or 'false' to enable initial data sampling for very large datasets
    # SAMPLE_FRACTION: Fraction of data to sample (e.g., '0.1' for 10%)
    # SAMPLE_SEED: Seed for sampling for reproducibility

    glue_etl_processor = EnhancedGlueETL(getResolvedOptions(sys.argv, [
        'JOB_NAME', 'S3_SOURCE_PATH', 'S3_TARGET_PATH',
        '--S3_QUARANTINE_PATH', '--S3_METRICS_PATH',
        '--INPUT_FORMAT', '--OUTPUT_FORMAT', '--COMPRESSION',
        '--CSV_DELIMITER', '--CSV_QUOTE_CHAR', '--CSV_ESCAPE_CHAR', '--CSV_HEADER', '--CSV_INFER_SCHEMA',
        '--ENABLE_SCHEMA_VALIDATION', '--ENABLE_DATA_PROFILING', '--ENABLE_ADVANCED_ANALYTICS', '--ENABLE_QUARANTINE',
        '--MAX_RECORDS_PER_PARTITION', '--MIN_PARTITIONS', '--MAX_PARTITIONS',
        '--CRITICAL_NULL_COLUMNS', '--TYPE_CONVERSION_MAP', '--DROP_DUPLICATE_SUBSET',
        '--FILL_NULL_STRINGS_WITH', '--FILL_NULL_NUMBERS_WITH', '--FILL_NULL_DATES_WITH', '--FILL_NULL_BOOLEANS_WITH',
        '--STANDARDIZE_STRING_CASE', '--REMOVE_NON_ALPHANUMERIC_FROM_COLUMNS',
        '--CUSTOM_DERIVED_COLUMNS_JSON', '--CUSTOM_VALIDATION_RULES_JSON',
        '--DATE_FORMATS_TO_TRY', '--TIMESTAMP_FORMATS_TO_TRY',
        '--NUMERIC_PRECISION', '--NUMERIC_SCALE', '--HANDLE_NUMERIC_OVERFLOW',
        '--ENABLE_OUTLIER_DETECTION', '--OUTLIER_METHOD', '--OUTLIER_THRESHOLD',
        '--ENABLE_SAMPLING', '--SAMPLE_FRACTION', '--SAMPLE_SEED'
    ]))
    glue_etl_processor.run()
