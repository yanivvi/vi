# Stock Data Analysis and Deployment Project
This project includes two main scripts: `app.py` for analyzing stock data using Apache Spark, and `deploy.py` for deploying resources and the analysis job to AWS using Boto3. 
The analysis focuses on calculating various stock-related metrics, while the deployment script automates the setup of AWS S3, Glue databases, tables, and jobs.

# Overview
## app.py
Analyzes stock price data, calculating metrics like daily returns, average daily returns, most traded stocks, and annualized standard deviations. The results are saved to CSV files in an AWS S3 bucket.

## deploy.py
Handles the deployment of necessary AWS resources (S3 buckets, Glue database, and tables) and uploads the analysis script and data to S3. It also creates and executes a Glue job to run the analysis script in AWS.

## Prerequisites
* Python 3.x
* Apache Spark
* AWS CLI installed and configured
* Boto3 Python library
* An AWS account with permissions to create and manage S3, Glue databases, tables, and jobs.

## Setup
Install Python Dependencies:
`pip install pyspark boto3`
AWS CLI Configuration: Ensure the AWS CLI is configured with your credentials:
`aws configure`
Environment Variables: Set the GLUE_ROLE environment variable to the role ARN for AWS Glue access:
`export GLUE_ROLE='arn:aws:iam::<account number>:role/<glue role>'`

# How to Use
## Running app.py Locally:
Make sure Spark is installed and configured on your machine.
Execute app.py using Spark-submit to perform the stock data analysis locally.

`python app.py`
## Deploying with deploy.py:
Run deploy.py to set up the AWS environment and deploy the analysis job to AWS Glue.

`python deploy.py`

### This script will:
Create an S3 bucket and upload the necessary files.
Set up a Glue database and tables based on the analysis output.
Create and execute a Glue job to run app.py in AWS.

## Detailed Description
### app.py
Data Processing: Uses PySpark to load, process, and analyze stock data.
Metrics Calculated: Daily returns, average daily returns, most traded stocks by volume, annualized standard deviations, and return rates over specified intervals.

### deploy.py
AWS Resources: Automates the creation of S3 buckets, uploading files, creating Glue databases and tables, and managing Glue jobs.
Glue Job: Deploys app.py as an AWS Glue job, leveraging AWS's managed ETL service for data analysis.

### Note
Modify deploy.py as needed to match your AWS environment, particularly the S3 bucket names and file paths.
Ensure the AWS role provided to deploy.py has the necessary permissions for the resources it attempts to create and manage.
