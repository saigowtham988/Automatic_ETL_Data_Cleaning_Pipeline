"""
AWS Lambda Function: Glue ETL Job Trigger

This Lambda function serves as an event-driven trigger for an AWS Glue ETL Job.
It is designed to be invoked by Amazon EventBridge (CloudWatch Events) rules,
typically in response to the successful completion of an AWS Glue Crawler
or other upstream processes in an ETL pipeline.

The function's primary responsibility is to initiate the specified AWS Glue ETL Job,
passing any necessary parameters (e.g., S3 source/target paths, cleaning configurations).
It operates asynchronously, starting the Glue job and then completing its own
execution without waiting for the ETL job to finish. This ensures the Lambda
function remains lightweight and within its execution limits.

Configuration:
- Requires an environment variable `GLUE_JOB_NAME` to specify the name of the
  Glue ETL Job to be started.
- Optional environment variables (`S3_SOURCE_PATH_ENV`, `S3_TARGET_PATH_ENV`)
  to pass S3 paths to the Glue job.
- Optional `GLUE_JOB_ARGUMENTS_JSON` environment variable to pass a JSON string
  of additional, customizable arguments to the Glue job.
- Optional `LOG_LEVEL` environment variable for detailed logging control.

Error Handling:
- Logs and returns appropriate error responses for missing configurations,
  invalid JSON arguments, or if the specified Glue Job does not exist.
- Provides comprehensive logging for monitoring job initiation.

Author: Sai Gowtham reddy Udumula

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
                      Typically an EventBridge event from a Glue Crawler state change.
        context (object): The Lambda runtime context object.

    Returns:
        dict: A dictionary containing the HTTP status code and a JSON-formatted body
              indicating the outcome of the operation.
    """
    glue_job_name = os.environ.get('GLUE_JOB_NAME')
    
    if not glue_job_name:
        logger.error("Configuration Error: GLUE_JOB_NAME environment variable is not set.")
        return {
            'statusCode': 500,
            'body': json.dumps('Configuration Error: GLUE_JOB_NAME not set.')
        }

    logger.info(f"Received event: {json.dumps(event)}")

    job_arguments = {}

    # --- Extract dynamic arguments from event (if applicable) ---
    # This section is highly dependent on the EventBridge rule's input transformer
    # For example, if the EventBridge rule passes the S3 path from the crawler event,
    # you would parse it here. For this generic template, we primarily rely on env vars.
    try:
        if 'detail' in event and event['detail'].get('state') == 'SUCCEEDED':
            crawler_name = event['detail'].get('crawlerName')
            logger.info(f"Triggered by successful completion of Glue Crawler: '{crawler_name}'")
            # Further logic could be added here to dynamically determine S3_SOURCE_PATH
            # based on the crawler name or other event details, if the crawler's output
            # path is consistent or discoverable.
    except Exception as e:
        logger.warning(f"Could not extract dynamic arguments from event detail: {e}. Relying on environment variables/defaults.")

    # --- Add S3 paths from environment variables (common practice for fixed paths) ---
    s3_source_path_env = os.environ.get('S3_SOURCE_PATH_ENV')
    s3_target_path_env = os.environ.get('S3_TARGET_PATH_ENV')

    if s3_source_path_env:
        job_arguments['--S3_SOURCE_PATH'] = s3_source_path_env
        logger.info(f"Using S3_SOURCE_PATH from environment: {s3_source_path_env}")
    else:
        logger.warning("S3_SOURCE_PATH_ENV environment variable not set. Glue job might require this argument to be passed.")

    if s3_target_path_env:
        job_arguments['--S3_TARGET_PATH'] = s3_target_path_env
        logger.info(f"Using S3_TARGET_PATH from environment: {s3_target_path_env}")
    else:
        logger.warning("S3_TARGET_PATH_ENV environment variable not set. Glue job might require this argument to be passed.")

    # --- Parse additional job arguments from JSON environment variable ---
    additional_job_args_json = os.environ.get('GLUE_JOB_ARGUMENTS_JSON')
    if additional_job_args_json:
        try:
            parsed_args = json.loads(additional_job_args_json)
            job_arguments.update(parsed_args)
            logger.info(f"Added custom job arguments from GLUE_JOB_ARGUMENTS_JSON: {parsed_args}")
        except json.JSONDecodeError:
            logger.error(f"Configuration Error: GLUE_JOB_ARGUMENTS_JSON is not a valid JSON string: {additional_job_args_json}")
            return {
                'statusCode': 500,
                'body': json.dumps('Configuration Error: GLUE_JOB_ARGUMENTS_JSON is invalid.')
            }

    logger.info(f"Attempting to start AWS Glue Job: '{glue_job_name}' with arguments: {json.dumps(job_arguments)}")

    try:
        # Start the Glue ETL Job. This call is non-blocking.
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments=job_arguments
        )
        
        job_run_id = response.get('JobRunId')
        logger.info(f"Successfully initiated Glue Job '{glue_job_name}'. Job Run ID: '{job_run_id}'. AWS API Response: {json.dumps(response)}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully initiated Glue Job: {glue_job_name}. Job Run ID: {job_run_id}')
        }

    except glue_client.exceptions.EntityNotFoundException:
        logger.error(f"Glue Job '{glue_job_name}' not found. Please ensure the job exists in AWS Glue and the name is correct.", exc_info=True)
        return {
            'statusCode': 404,
            'body': json.dumps(f"Error: Glue Job '{glue_job_name}' not found.")
        }
    except Exception as e:
        logger.error(f"An unexpected error occurred while attempting to start Glue Job '{glue_job_name}': {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error starting Glue Job: {str(e)}')
        }
