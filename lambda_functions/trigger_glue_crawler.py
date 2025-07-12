"""
AWS Lambda Function: Glue Crawler Trigger

This Lambda function serves as an event-driven trigger for an AWS Glue Crawler.
It is designed to be invoked by Amazon S3 event notifications (e.g., `s3:ObjectCreated:*`)
when new data files are landed in a designated S3 bucket.

The function's primary responsibility is to initiate the specified AWS Glue Crawler.
It operates asynchronously, meaning it starts the crawler and then completes its
own execution without waiting for the crawler to finish. This design adheres to
serverless best practices by keeping Lambda functions lightweight and preventing
timeout issues for long-running crawler operations.

The successful completion of the Glue Crawler can then be monitored via
Amazon EventBridge (CloudWatch Events) to trigger subsequent steps in an ETL pipeline,
such as starting an AWS Glue ETL job.

Configuration:
- Requires an environment variable `GLUE_CRAWLER_NAME` to specify the name of the
  Glue Crawler to be started.
- Optional `LOG_LEVEL` environment variable for detailed logging control.

Error Handling:
- Gracefully handles cases where the specified Glue Crawler is already running.
- Logs and returns appropriate error responses for configuration issues or unexpected AWS API errors.

Author: Sai Gowtham Reddy Udumula
"""

import os
import json
import boto3
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    """
    Main entry point for the Lambda function.

    Args:
        event (dict): The event payload that triggered the Lambda function.
                      Typically an S3 event notification.
        context (object): The Lambda runtime context object.

    Returns:
        dict: A dictionary containing the HTTP status code and a JSON-formatted body
              indicating the outcome of the operation.
    """
    crawler_name = os.environ.get('GLUE_CRAWLER_NAME')

    if not crawler_name:
        logger.error("Configuration Error: GLUE_CRAWLER_NAME environment variable is not set.")
        return {
            'statusCode': 500,
            'body': json.dumps('Configuration Error: GLUE_CRAWLER_NAME not set.')
        }

    logger.info(f"Received event: {json.dumps(event)}")

    try:
        # Log S3 event details for traceability, if available
        if 'Records' in event and len(event['Records']) > 0:
            s3_record = event['Records'][0].get('s3')
            if s3_record:
                bucket_name = s3_record['bucket']['name']
                object_key = s3_record['object']['key']
                logger.info(f"S3 event detected for bucket: '{bucket_name}', key: '{object_key}'")
            else:
                logger.warning("S3 record not found in the event payload. Proceeding to start crawler based on pre-configured path.")
        else:
            logger.warning("No S3 records found in the event. Assuming manual invocation or non-S3 event source.")

        logger.info(f"Attempting to start AWS Glue Crawler: '{crawler_name}'")

        # Start the Glue Crawler. This call is non-blocking.
        response = glue_client.start_crawler(Name=crawler_name)
        
        logger.info(f"Successfully initiated Glue Crawler '{crawler_name}'. AWS API Response: {json.dumps(response)}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully initiated Glue Crawler: {crawler_name}')
        }

    except glue_client.exceptions.CrawlerRunningException:
        logger.warning(f"Glue Crawler '{crawler_name}' is already running. Skipping start request to avoid conflicts.")
        return {
            'statusCode': 200,
            'body': json.dumps(f"Glue Crawler '{crawler_name}' is already running.")
        }
    except Exception as e:
        logger.error(f"An unexpected error occurred while attempting to start Glue Crawler '{crawler_name}': {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error starting Glue Crawler: {str(e)}')
        }
