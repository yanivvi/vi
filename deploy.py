import boto3
import os

def create_bucket(bucket_name):
    s3 = boto3.client('s3')
    try:
        s3.create_bucket(Bucket=bucket_name)
        msg = f"Bucket '{bucket_name}' created successfully."
    except Exception as e:
        msg = e
    print(msg)

def upload_file_to_s3(bucket_name, file_name, sub_folder=None, object_name=None):
    s3 = boto3.client('s3')
    if object_name is None:
        object_name = file_name
    if sub_folder:
        object_name = f"{sub_folder}/{object_name}"
    try:
        response = s3.upload_file(file_name, bucket_name, object_name)
        msg = f"File {file_name} uploaded to {bucket_name}/{object_name}"
    except Exception as e:
        msg = e
    print(msg)

def db_creation(db_name):
    # Initialize a boto3 client
    glue = boto3.client('glue')

    # Create the Glue database
    try:
        glue.create_database(
            DatabaseInput={
                'Name': db_name
            }
        )
        msg = f"Database '{db_name}' created successfully."
    except Exception as e:
        msg = e
    print(msg)

def table_creation(db_name, bucket_name, table_name, columns_defintion):

    glue = boto3.client('glue')

    template_input = {
        'Name': '',
        'StorageDescriptor': {
            'Columns': [],
            'Location': '',
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'Compressed': False,
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                'Parameters': {
                    'field.delim': ',',  
                    'serialization.format': ','
                }
            },
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'csv',
            'compressionType': 'none',
            'typeOfData': 'file'
        }
    }

    table_input=template_input
    table_input["Name"] = table_name
    table_input["StorageDescriptor"]["Columns"] = columns_defintion
    table_input["StorageDescriptor"]["Location"] = f"s3://{bucket_name}/output/{table_name}"
    

    # Create the table
    try:
        response = glue.create_table(
            DatabaseName=db_name,
            TableInput=table_input
        )
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Error creating table: {str(e)}")

def job_creation(job_name,bucket_name):
    # Initialize the Glue client
    glue_client = boto3.client('glue') 

    # Define the job properties
    script_location = f"s3://{bucket_name}/scripts/app.py"  
    # make sure to export the role name in the environment variables
    glue_role = os.environ["GLUE_ROLE"]

    try:
        # Create the Glue job
        response = glue_client.create_job(
            Name=job_name,
            Description='Glue job created via Boto3',
            Role=glue_role,
            ExecutionProperty={
                'MaxConcurrentRuns': 1
            },
            Command={
                'Name': 'glueetl',
                'ScriptLocation': script_location,
                'PythonVersion': '3' 
            },
            GlueVersion='2.0', 
            WorkerType='Standard', 
            NumberOfWorkers=2,  
            Timeout=60
        )

        print(f"Glue job '{job_name}' created successfully.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Glue job '{job_name}' already exists.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def job_execution(job_name):
    # # Initialize the Glue client
    glue = boto3.client('glue') 
    try:
        # Start the Glue job
        print(f"Starting Glue job {job_name}")
        start_response = glue.start_job_run(JobName=job_name)
        job_run_id = start_response['JobRunId']
    except Exception as e:
        print(f"Error starting Glue job run: {str(e)}")

def main():
    db_name = 'home_assignment_yv'
    s3_bucket = 'aws-glue-home-assignment-yv'

    # create s3 bucket
    create_bucket(s3_bucket)

    # upload files to s3
    upload_file_to_s3(s3_bucket,'app.py','scripts')
    upload_file_to_s3(s3_bucket,'data/stock_prices.csv')

    # glue database
    db_creation(db_name)

    # glue tables
    table_name = 'q1_avg_return'
    columns_defintion = [
                {'Name': 'date', 'Type': 'string'},
                {'Name': 'avg_retruns', 'Type': 'string'}
            ]
    table_creation(db_name, s3_bucket, table_name, columns_defintion)

    table_name = 'q2_avg_frequency'
    columns_defintion = [
                {'Name': 'ticker', 'Type': 'string'},
                {'Name': 'frequency', 'Type': 'string'}
            ]
    table_creation(db_name, s3_bucket, table_name, columns_defintion)

    table_name = 'q3_annualized_std'
    columns_defintion = [
                {'Name': 'ticker', 'Type': 'string'},
                {'Name': 'annualized_std_returns', 'Type': 'string'}
            ]
    table_creation(db_name, s3_bucket, table_name, columns_defintion)

    table_name = 'q4_top_days_range_return_dates'
    columns_defintion = [
                {'Name': 'ticker_date', 'Type': 'string'},
                {'Name': 'return_rate_change_within_range', 'Type': 'string'}
            ]
    table_creation(db_name, s3_bucket, table_name, columns_defintion)

    # glue job
    job_name = 'home-assignment-yv'
    job_creation(job_name,s3_bucket)
    job_execution(job_name)

if __name__ == '__main__':
    main()
