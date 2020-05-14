import boto3
import configparser
import sys
import os

config = configparser.ConfigParser()
config.read("dl.cfg")

aws_access_key = config.get("AWS", "AWS_ACCESS_KEY_ID")
aws_secret_key = config.get("AWS", "AWS_SECRET_ACCESS_KEY")
aws_region = config.get("AWS", "AWS_REGION")
aws_s3_log_uri = config.get("AWS", "AWS_S3_LOG_URI")
aws_ec2_key_name = config.get("AWS", "AWS_EC2_KEY_NAME")
aws_ec2_subnet_id = config.get("AWS", "AWS_EC2_SUBNET_ID")

os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key
os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_key

# Adapted from https://medium.com/@kulasangar/create-an-emr-cluster-and-submit-a-job-using-boto3-c34134ef68a0
connection = boto3.client(
    "emr",
    region_name=aws_region,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

s3_local_file = 'etl.py'
s3_install_requirements_file = 'install-requirements.sh'
s3_bucket = 'million-song'
s3_key = 'code/{local_file}'.format(local_file=s3_local_file)
s3_install_key = 'code/{local_file}'.format(local_file=s3_install_requirements_file)
s3_uri = 's3://{bucket}/{key}'.format(bucket=s3_bucket, key=s3_key)
s3_install_uri = 's3://{bucket}/{key}'.format(bucket=s3_bucket, key=s3_install_key)

s3 = boto3.resource('s3')

# Check if bucket already exists
s3_bucket_exists = False
for bucket in s3.buckets.all():
    if bucket.name == s3_bucket:
        s3_bucket_exists = True


s3 = boto3.client('s3', region_name=aws_region)

# Create bucket if it does not exist
if not s3_bucket_exists:
    s3.create_bucket(
            Bucket=s3_bucket,
            CreateBucketConfiguration={
                'LocationConstraint': aws_region
                }
            )

# upload etl.py to s3_bucket
#s3 = boto3.resource('s3')
s3 = boto3.client(
    "s3",
    region_name=aws_region,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

s3.upload_file(s3_local_file, s3_bucket, s3_key)
s3.upload_file(s3_install_requirements_file, s3_bucket, s3_install_key)

# Create AWS EMR cluster
# Adapted from https://stackoverflow.com/questions/36706512/how-do-you-automate-pyspark-jobs-on-emr-using-boto3-or-otherwise
connection = boto3.client(
    "emr",
    region_name=aws_region,
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

response = connection.run_job_flow(
    Name='million_song_emr_job_boto3',
    LogUri=aws_s3_log_uri,
    ReleaseLabel='emr-5.20.0',
    Applications=[
        {
            'Name': 'Spark'
        },
    ],
    EbsRootVolumeSize=10,
    Instances={
        'MasterInstanceType': 'm5.xlarge',
        'SlaveInstanceType': 'm5.xlarge',
        'InstanceCount': 3,
        'TerminationProtected': False,
        'Ec2KeyName': aws_ec2_key_name,
        'Ec2SubnetId': aws_ec2_subnet_id
    },
    BootstrapActions=[
        {
            'Name': 'Maximize Spark Default Config',
            'ScriptBootstrapAction': {
                'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config',
            }
        },
        {
            'Name': 'Install Required pip Modules',
            'ScriptBootstrapAction': {
                'Path': s3_install_uri,
            }
        },

    ],
    Steps=[
    {
        'Name': 'Setup Debugging',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    },
    {
        'Name': 'setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', s3_uri, '/home/hadoop/']
        }
    },
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/etl.py']
        }
    }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole'
)

print ('cluster created with the step...', response['JobFlowId'])
