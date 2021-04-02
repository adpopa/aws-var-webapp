import boto3

REGION_NAME = 'us-east-1'

# create EMR clent obj
def get_emr_client():
    try:
        return boto3.client('emr', region_name=REGION_NAME)
    except Exception as e:
        logger.info(str(e))
        exit(0)


def lambda_handler(event, context):
    BUCKET = 'emr-stocks'

    MAPPER_FILE = 'mapStock.py'
    MAPPER_PATH = 's3://{}/{}'.format(BUCKET, MAPPER_FILE)

    A = int(event['A'])
    REDUCER_FILE = 'reduceAverageStock.py'
    REDUCER_FILE_ARGS = 'reduceAverageStock.py {}'.format(A)
    REDUCER_PATH = 's3://{}/{}'.format(BUCKET, REDUCER_FILE)

    STOCK_NAME = event['stockName']
    INPUT_FILE = '{}.csv'.format(STOCK_NAME)
    INPUT_PATH = 's3://{}/input/{}'.format(BUCKET, INPUT_FILE)

    CLUSTER_NAME = 'A{}{}'.format(A,STOCK_NAME)
    OUTPUT_PATH = 's3://{}/output/{}'.format(BUCKET,CLUSTER_NAME)

    KEY_PAIR = 'us-east-1'

    try:
        # Method to launch EMR cluster and run jobs
        get_emr_client().run_job_flow(
            Name=CLUSTER_NAME,
            ReleaseLabel='emr-5.30.0',  # EMR version

            # Configuration for EMR cluster
            Instances={
                'InstanceGroups': [
                    {'Name': 'master',
                     'InstanceRole': 'MASTER',
                     'InstanceType': 'm5.xlarge',
                     'InstanceCount': 1,
                     },
                    {'Name': 'core',
                     'InstanceRole': 'CORE',
                     'InstanceType': 'm5.xlarge',
                     'InstanceCount': 1,
                     },

                ],
                'Ec2KeyName': KEY_PAIR
            },
            # Hadoop streaming command to start the map reduce job
            Steps=[
                {'Name': 'streaming step lambda',
                 'ActionOnFailure': 'TERMINATE_CLUSTER',
                 'HadoopJarStep': {
                     'Jar': 'command-runner.jar',
                     'Args': [
                         'hadoop-streaming',
                         '-files',
                         '{},{},{}'.format(MAPPER_PATH, REDUCER_PATH,
                                           INPUT_PATH),
                         '-mapper', MAPPER_FILE,
                         '-reducer', REDUCER_FILE_ARGS,
                         '-input', INPUT_PATH,
                         '-output', OUTPUT_PATH
                     ]}
                 }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
        )
        return 'Wait for the EMR to compute'
    except Exception as e:
        print(e)
        return str(e)
