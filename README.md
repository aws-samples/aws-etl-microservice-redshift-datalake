## ETL microservice using AWS Lambda and AWS Glue to move partitioned table from Redshift to AWS Datalake

AWS Lambda based microservice built using AWS Glue API to migrate tables off of Amazon Redshift into AWS (S3) data lake.

## License Summary

This sample code is made available under a modified MIT license. See the LICENSE file.

## Overview
Amazon Redshift is a peta-byte scale, massively parallel processing (MPP) and columnar Data Warehouse (DW) service. Customers run their workload to transform raw data into a data model that can be consumed readily for analysis rather than running transformation on the raw data during analysis. Raw data may come from multiple sources which often comes in different format or standard. Typically customers take advantage of Redshift's massively parallel processing capability to apply SQL transformations such as filter, projection, join, aggregate etc. Once the trasnformed data materialized into a table it can be queried over and over again without complex SQLs and the business users can get insight into the business processes or trends on social media, traffic pattern, website click pattern etc.

As the data grows in your DW you start realizing that a large chunk of the data residing in the Redshift table are retrieved infrequently. You may consider unloading historical data from your DW to reclaim storage in the Redshift table, which can potentially improve query performance on such tables. As this data is unloaded into S3 you are now making the data processing architecture more
generic, not constrained by use of one specific AWS service.

We will create a simple template using AWS Lambda and AWS Glue which you can use to move a partitions from a Redshift table into s3. It will create objects in s3 in Hive partitioned table and in columnar form which you can choose during setting up the schedule in AWS Lambda. 

## Architecture

We will use an AWS Lambda function acting as the ETL microservice template. You can use that template to schedule partition unload of as many tables you have in Redshift. The scheduling can be done in AWS Cloudwatch as Cron.

The Lambda function accepts necessary parameters to identify which table to unload, optionally if the table is partitioned you can provide the partition value. You will also provide the unload destination path as s3 location of your data lake, the file format of the unloaded data. The lambda function will first create an AWS Glue job. The name of the job is schemaname.tablename_ExtractToDataLake where schemaname.tablename is the prefixed table name in your Redshift cluster. 

Any subsequent invocation of the lambda function with the same tablename as its input parameter will instantiate a new job run id. This way you can browse the AWS Glue job by entering the schemamame.tablename format in the AWS Glue > Jobs page. Just keep in mind the number of jobs created by the Lambda function will be counted towards the AWS Glue Limits which is 25 jobs per AWS account. This is a soft limit and you can request a limit increase by contacting the AWS Support.

## Lambda function

I’ve used AWS Glue Jobs API to create job and submit job. To manage the job submission I have created a Lambda function which is the mircroservice to unload my table's data from Redshift into S3. The Start_Job_Run API used in this Lambda function is an asynchronous API which means Lambda function will not time out during the execution time of the AWS Glue job.

```python
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
```

## AWS Glue Job
The AWS Glue job used in this blog is a pyspark code. This pyspark code is a generic code for unloading any Redshift table- partitioned or non-partitioned, into s3. The pyspark code is stored in an s3 bucket which is owned by the AWS account owner. AWS Glue creates this s3 bucket-  which is named as aws-glue-scripts-aws-account-number-region, when you create an AWS Glue job from the AWS Management Console. 

```python
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
```
The key here is the databricks driver for Redshift to connect to the Redshift cluster using JDBC and execute the push down predicate for filering the table's data on the partition key.

## Invocation

Job invocation is done through CLoudwatch Events rule. The rule's target is set as the AWS lambda function with the input parameters passed as JSON input text, a sample is given below
```
{   "PrefixedTablename": "demo_master.orders",   "SnapshotYN": "y",   "FileFormat": "parquet",   "WriteMode": "overwrite",   "s3FilePath": "s3://tpcdhawspsa/sampledatalake/",   "JdbcUrl": "jdbc:redshift://aod-lab-redshiftcluster-corqdpn6wh03.ckh0do3morgd.us-east-1.redshift.amazonaws.com:8192/awspsars?user=rsadmin&password=Welcome123",   "GlueRSConnection": "tpc-rs",   "RedshiftRoleArn": "arn:aws:iam::413094830157:role/rsLabGroupPolicy-SpectrumRole",   "DayPartitionKey": "o_orderdate",   "DayPartitionValue": "1998-07-21" }
```
If you do not enter the DayPartitionValue then the current date will be considered as the DayPartitionValue.

To monitor the individual table unload I have used cloudwatch events to log both Success and Failure of each job. The AWS Lambda keeps in the namespace "Lambda-ETL" and AWS Glue is under "Glue-ETL".
