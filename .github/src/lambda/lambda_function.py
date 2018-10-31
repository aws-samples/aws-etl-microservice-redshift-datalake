#!/usr/bin/env python

from __future__ import print_function

# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
# 
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


import sys
import boto3

##################

def lambda_handler(event, context):
    print("lambda arn: %s"  % context.invoked_function_arn)
    
    # Get Account ID from lambda function arn in the context
    currentAccount = context.invoked_function_arn.split(":")[4]
    print("Account ID= %s" % currentAccount)
    
    currentRegion = context.invoked_function_arn.split(":")[3]
    print("Current region: %s" %currentRegion)

    try:
        cw = boto3.client(service_name = 'cloudwatch', region_name = currentRegion)
    except:
        print("Connecting to Cloudwatch service in region %s failed. Exception %s " % currentRegion %sys.exc_info()[1] )
        cw.put_metric_data(
                Namespace='Lambda-ETL',
                MetricData=[
                    {
                        'MetricName': 'Error',
                        'Value': 1,
                        'Unit': 'Count'
                    }
                ]
            )
        sys.exit(1)
    
    try:
        glue = boto3.client(service_name='glue', region_name=currentRegion, endpoint_url='https://glue.us-east-1.amazonaws.com')
    except:
        print("Connecting to Glue service in region %s failed. Exception %s " % currentRegion %sys.exc_info()[1] )
        cw.put_metric_data(
                Namespace='Lambda-ETL',
                MetricData=[
                    {
                        'MetricName': 'Error',
                        'Value': 1,
                        'Unit': 'Count'
                    }
                ]
            )
        sys.exit(1)
    
    try:
        print("Writing into Cloudwatch under namespace Lambda-ETL ...")
        cw.put_metric_data(
                Namespace='Lambda-ETL',
                MetricData=[
                    {
                        'MetricName': 'Invocation count',
                        'Value': 1,
                        'Unit': 'Count'
                    }
                ]
            )
    except:
        print("Writing into Cloudwatch from Lambda function failed: exception %s" %sys.exc_info()[1])
        sys.exit(1)

    if event is not None:
        prefixedTablename = event['PrefixedTablename']
        snapshotYN = event['SnapshotYN']
        fileFormat = event['FileFormat']
        writeMode = event['WriteMode']
        s3FilePath = event['s3FilePath']
        jdbcUrl = event['JdbcUrl']
        glueRSConnection = event['GlueRSConnection']
        redshiftRoleArn = event['RedshiftRoleArn']
        dayPartitionKey = event['DayPartitionKey']
        dayPartitionValue = event['DayPartitionValue']
    else:
        print("Please provide the mandatory paramaters.")
        print(prefixedTablename)
        print(jdbcUrl)
        print(s3FilePath)
        print(redshiftRoleArn)
        print("Aborting ...")
        cw.put_metric_data(
                Namespace='Lambda-ETL',
                MetricData=[
                    {
                        'MetricName': 'Error',
                        'Value': 1,
                        'Unit': 'Count'
                    }
                ]
            )
        sys.exit(1)
    
    print("prefixedTablename = %s" %prefixedTablename)
    print("jdbcUrl = %s" %jdbcUrl)
    print("s3FilePath = %s" %s3FilePath)
    print("glueRSConnection = %s" %glueRSConnection)
    print("redshiftRoleArn = %s" %redshiftRoleArn)
    
    ## Create Glue job with the parameters provided in the input
    try:
        print("Trying to launch Glue job")
        scriptLocation = 's3://aws-glue-scripts-' + currentAccount + '-' + currentRegion + '/admin/unload-table-part.py'
        print("Script location: %s" % scriptLocation)
        
        myJob = glue.create_job(Name=prefixedTablename + "_ExtractToDataLake", \
                            Role='AWSGlueServiceRoleDefault', \
                            Command={'Name': 'glueetl', 'ScriptLocation': scriptLocation},\
                            Connections= {'Connections' : [glueRSConnection]},\
                            MaxRetries = 1, \
                            ExecutionProperty = {'MaxConcurrentRuns': 1}, \
                            DefaultArguments = {"--TempDir": "s3://aws-glue-temporary-" + currentAccount + '-' + currentRegion + "/temp" }
                            )
        print("Glue job %s created" %myJob['Name'])
                            
        myJobRun = glue.start_job_run( \
                            JobName=myJob['Name'], \
                            Arguments = { \
                                        '--prefixed_table_name': prefixedTablename, \
                                        '--write_mode': writeMode, \
                                        '--snapshot_yn': snapshotYN, \
                                        '--file_format': fileFormat, \
                                        '--s3path_datalake' : s3FilePath, \
                                        '--jdbc_url' :jdbcUrl, \
                                        '--redshift_role_arn' : redshiftRoleArn, \
                                        '--day_partition_key':   dayPartitionKey, \
                                        '--day_partition_value':  dayPartitionValue} 
                                     )
    except:
        print("Creating Glue job failed. Exception: %s" %sys.exc_info()[1])
        cw.put_metric_data(
                Namespace='Lambda-ETL',
                MetricData=[
                    {
                        'MetricName': 'Error',
                        'Value': 1,
                        'Unit': 'Count'
                    }
                ]
            )
        sys.exit(1)
    
    ## Glue returns control here immediately 
    print("Jobrun ID %s" %myJobRun['JobRunId'])
    status = glue.get_job_run(JobName=myJob['Name'], RunId=myJobRun['JobRunId'])
    print("Jobrun status %s" %status['JobRun']['JobRunState'])
