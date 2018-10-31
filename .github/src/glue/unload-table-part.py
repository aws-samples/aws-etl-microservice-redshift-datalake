# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.



import sys
import os
import boto3
import base64
import datetime
from boto.kms.exceptions import NotFoundException
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql import SQLContext

## @params: [TempDir, JOB_NAME]

currentRegion='us-east-1'

cw = boto3.client('cloudwatch', region_name = currentRegion)

args = getResolvedOptions(sys.argv, [ 'TempDir',
                                        'JOB_NAME',
                                        's3path_datalake',
                                        'jdbc_url',
                                        'redshift_role_arn',
                                        'prefixed_table_name',
                                        'write_mode',
                                        'snapshot_yn',
                                        'file_format',
                                        'day_partition_key',
                                        'day_partition_value'
                                    ]
                        )

#### Read the parameters
s3pathDatalake = args['s3path_datalake']
jdbcUrl =  args['jdbc_url']
redshiftRoleArn = args['redshift_role_arn']
prefixedTableName = args['prefixed_table_name']
snapshotYN = args['snapshot_yn']
writeMode = args['write_mode']
fileFormat = args['file_format']
dayPartitionKey = args['day_partition_key']
dayPartitionValue = args['day_partition_value']

#### Check for the mandatory parameters
if s3pathDatalake is None:
    print("Necessary parameter \"s3path_datalake\" is missing. Please provide the parameter  \"s3path_datalake\" while submitting the Glue API job.")
if jdbcUrl is None:
    print("Necessary parameter \"jdbcUrl\" is missing. Please provide the parameter \"jdbcUrl\" while submitting the Glue API job.")
if redshiftRoleArn is None:
    print("Necessary parameter \"redshiftRoleArn\" is missing. Please provide the parameter \"redshiftRoleArn\" while submitting the Glue API job.")
if prefixedTableName is None:
    print("Necessary parameter \"prefixedTableName\" is missing. Please provide the parameter \"prefixedTableName\" while submitting the Glue API job.")

if s3pathDatalake is None or jdbcUrl is None or redshiftRoleArn is None or prefixedTableName is None:
    print("Error: one or more manadatory paramters are not provided.")
    cw.put_metric_data(
            Namespace='Glue-ETL',
            MetricData=[
                {
                    'MetricName': 'Invocation errors',
                    'Timestamp': datetime.datetime.utcnow(),
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
        )
    sys.exit(1) 
    
#### Initialize optional paramters
if snapshotYN is None:
    snapshotYN = 'n'
if writeMode is None:
    writeMode = 'append'
if fileFormat is None:
    writeMode = 'parquet'
if dayPartitionKey is None:
    dayPartitionKey = 'snapshot_day'
if dayPartitionValue is None:
    dayPartitionValue = datetime.now().strftime("%Y-%m-%d")

try:
    cw.put_metric_data(
            Namespace='Glue-ETL',
            MetricData=[
                {
                    'MetricName': 'Invocation count',
                    'Dimensions': [{ 'Name': 'TableName', 'Value': args['prefixed_table_name']}],
                    'Timestamp': datetime.datetime.utcnow(),
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
        )
except:
    print "Reading argument from Lambda function failed: exception %s" %sys.exc_info()[1]
    cw.put_metric_data(
            Namespace='Glue-ETL',
            MetricData=[
                {
                    'MetricName': 'Invocation errors',
                    'Timestamp': datetime.datetime.utcnow(),
                    'Value': 1,
                    'Unit': 'Count'
                }
            ]
        )
    sys.exit(1) 

#### Retrieve schema name to build the s3 path for datalake
schemaName = prefixedTableName.split(".")[0]
tableName = prefixedTableName.split(".")[1]

print("s3pathDatalake: %s" % s3pathDatalake)
print("redshiftRoleArn: %s" % redshiftRoleArn)
print("schemaName: %s" % schemaName)
print("tableName: %s" % tableName)
print("snapshotYN: %s" % snapshotYN)
print("writeMode: %s" % writeMode)
print("fileFormat: %s" % fileFormat)
print("dayPartitionKey: %s" % dayPartitionKey)
print("dayPartitionValue: %s" % dayPartitionValue)


#### Build the pushdown query for Redshift and s3 path
extract_sql = ""
filePath = ""
if snapshotYN.lower() == 'n':
    extract_sql = "select * from " + prefixedTableName
    filePath = s3pathDatalake + "/" + schemaName + "/" + fileFormat + "/" + tableName + "/"
elif snapshotYN.lower() == 'y':
    extract_sql = 'select * from ' + prefixedTableName + ' where ' + dayPartitionKey +  ' = \'' +  dayPartitionValue + '\' order by '  + dayPartitionKey
    filePath = s3pathDatalake + "/"  + schemaName + "/" + fileFormat + "/" + tableName + "/" + dayPartitionKey + "_key" + "=" + dayPartitionValue + "/"
    print("Filepath: %s" % s3pathDatalake)
    print("extract_sql: %s" % extract_sql)


#### Initialize Spark and Glue context
sc = SparkContext()
sql_context = SQLContext(sc)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### Connect to the JDBC database/ Redshift
try:
    print("Connecting to jdbc url %s" % jdbcUrl)
    datasource0 = sql_context.read\
                .format("com.databricks.spark.redshift")\
                .option("url", jdbcUrl)\
                .option("query", extract_sql)\
                .option("aws_iam_role", redshiftRoleArn)\
                .option("tempdir", args['TempDir'])\
                .load()
    print("Connected successfully to jdbc url %s" % jdbcUrl)
    datasource0.printSchema()

except: 
    print "JDBC connection to %s failed" % jdbcUrl
    cw.put_metric_data(
            Namespace = 'Glue-ETL',
            MetricData = [{
                'MetricName': 'Invocation errors',
                'Dimensions': [{
                'Name': 'TableName',
                'Value': args['prefixed_table_name']
                }],
            'Timestamp': datetime.datetime.utcnow(),
            'Value': 1,
            'Unit': 'Count'
            }]
            )
    sys.exit(1)


try:
    if fileFormat == 'csv':
        datasource0.write.mode(writeMode).csv(filePath)
    elif fileFormat == 'orc':
        datasource0.write.mode(writeMode).orc(filePath)
    elif fileFormat == 'parquet':
        datasource0.write.mode(writeMode).parquet(filePath)
        
except:
    cw.put_metric_data(
            Namespace = 'Glue-ETL',
            MetricData = [{
                'MetricName': 'Invocation errors',
                'Dimensions': [{
                'Name': 'TableName',
                'Value': args['prefixed_table_name']
                }],
            'Timestamp': datetime.datetime.utcnow(),
            'Value': 1,
            'Unit': 'Count'
            }]
            )
    print("Data format conversion failed %s" % sys.exc_info()[1])
    sys.exit(1) 
job.commit()
