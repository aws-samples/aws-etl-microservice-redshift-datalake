## ETL microservice using AWS Lambda and AWS Glue to move partitioned table from Redshift to AWS Datalake

AWS Lambda based microservice built using AWS Glue API to migrate tables off of Amazon Redshift into AWS (S3) data lake.

## License Summary

This sample code is made available under a modified MIT license. See the LICENSE file.

## Overview
Amazon Redshift is a peta-byte scale, massively parallel processing (MPP) and columnar Data Warehouse (DW) service. Customers run their workload to transform raw data into a data model that can be consumed readily for analysis. Raw data may come from various data sources of disparate technology and of different format. Typically customers take advantage of Redshift's massively parallel processing capability to apply SQL transformations such as filter, projection, join, aggregate etc. on the raw data. Once the transformed data gets materialized into tables thety can be queried over and over again without executing complex SQLs and the business users can get insight into the business processes or trends on social media, traffic pattern, website click pattern etc.

As the data grows in your DW you start realizing that a large chunk of the data residing in the Redshift table are retrieved infrequently. You may consider unloading historical data from your DW to reclaim storage in the Redshift table, which can potentially improve query performance on such tables. As this data is unloaded into S3 you are now making the data processing architecture more
generic, not constrained by use of one specific AWS service.

We will create a simple template using AWS Lambda and AWS Glue which you can use to move a partitions from a Redshift table into s3. It will create objects in s3 in Hive partitioned table and in columnar form which you can choose during setting up the schedule in AWS Lambda. 

## Architecture

We will use an AWS Lambda function acting as the ETL microservice template. You can use that template to schedule partition unload of as many tables you have in Redshift. The scheduling can be done in AWS Cloudwatch as Cron.

The Lambda function accepts necessary parameters to identify which table to unload, optionally if the table is partitioned you can provide the partition value. You will also provide the unload destination path as s3 location of your data lake, the file format of the unloaded data. The lambda function will first create an AWS Glue job. The name of the job is schemaname.tablename_ExtractToDataLake where schemaname.tablename is the prefixed table name in your Redshift cluster. 

Any subsequent invocation of the lambda function with the same tablename as its input parameter will instantiate a new job run id. This way you can browse the AWS Glue job by entering the schemamame.tablename format in the AWS Glue > Jobs page. Just keep in mind the number of jobs created by the Lambda function will be counted towards the AWS Glue Limits which is 25 jobs per AWS account. This is a soft limit and you can request a limit increase by contacting the AWS Support.

[![Launch Stack](https://s3.amazonaws.com/cloudformation-examples/cloudformation-launch-stack.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=&templateURL=https://s3.amazonaws.com/awspsa-redshift-lab/cfn-templates-redshift-lab/ETL_MicroService_LambdaGlue.json)

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

## AWS Lambda Invocation

Job invocation is done through CLoudwatch Events rule. The rule's target is set as the AWS lambda function with the input parameters passed as JSON input text, a sample is given below
```
{   "PrefixedTablename": "demo_master.orders",   "SnapshotYN": "y",   "FileFormat": "parquet",   "WriteMode": "overwrite",   "s3FilePath": "s3://tpcdhawspsa/sampledatalake/",   "JdbcUrl": "jdbc:redshift://aod-lab-redshiftcluster-corqdpn6wh03.ckh0do3morgd.us-east-1.redshift.amazonaws.com:8192/awspsars?user=rsadmin&password=Welcome123",   "GlueRSConnection": "tpc-rs",   "RedshiftRoleArn": "arn:aws:iam::413094830157:role/rsLabGroupPolicy-SpectrumRole",   "DayPartitionKey": "o_orderdate",   "DayPartitionValue": "1998-07-21" }
```
![lambda-invocation-cw-rule](https://github.com/aws-samples/aws-etl-microservice-redshift-datalake/blob/master/lambda-invocation-cw-rule.png)

If you do not enter the DayPartitionValue then the current date will be considered as the DayPartitionValue.

To monitor the individual table unload I have used cloudwatch events to log both Success and Failure of each job. The AWS Lambda keeps in the namespace "Lambda-ETL" and AWS Glue is under "Glue-ETL".

## VPC configuration

Now let's talk about the VPC and IAM configuration necessary to execute this ETL microservice. I will discuss the EC2 resources launched by Lambda is created in a private Subnet within my VPC. It is reccommended to create 2 subnets in different AZ for redundancy.
The private subnets are attached to a routing table that has an outbound traffic target to a [NAT gateway](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-nat-gateway.html). For connectivity to s3 I have created an s3 endpoint in my VPC. This endpoint will also show up in the routing table.

![vpc-configuarion-natgateway](https://github.com/aws-samples/aws-etl-microservice-redshift-datalake/blob/master/vpc-configuarion-natgateway.png)

Since AWS Lambda and AWS Glue will reach out to the Redshift cluster we  need the Elastic IP Address of the NAT gateway which is needed to create the security group. This security group will be attached to the Redshift cluster.

![vpc-configuarion-natgateway-elasticIP](https://github.com/aws-samples/aws-etl-microservice-redshift-datalake/blob/master/vpc-configuarion-natgateway-elasticIP.png)

The security group *nat-sg-redshift* has one entry in its inbound rule -

![vpc-configuarion-natgateway-securitygroup](https://github.com/aws-samples/aws-etl-microservice-redshift-datalake/blob/master/vpc-configuarion-natgateway-securitygroup.png)

## IAM Roles
The IAM roles will let AWS Lambda and AWS Glue access other AWS services needed for the this ETL microservice. These services are S3, Cloudwatch, AWS Glue and VPC. Two IAM roles will get created through the cloudformation template- one with "LambdaExecutionRole" and the other with "AWSGlueServiceRoleDefault" in the name.

Once the roles are created you will need to modify the "%AWSGlueServiceRoleDefault%" role to attach the below inline policy because the AWSGlueServiceRole managed policy does not have iam:passrole action to resource AWSGLueService. 

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": "arn:aws:iam::413094830157:role/AWSGlueService*"
        }
    ]
}

```

## AWS Glue Data Connection
The AWS Glue job makes jdbc call to the Redshift cluster which requires a AWS Glue Database Connection. To setup a database connection called you need to go to the AWS Glue console and navigate to Databases > Connections then click on "Add connection".

![glue-dataconnection-jdbc](https://github.com/aws-samples/aws-etl-microservice-redshift-datalake/blob/master/glue-dataconnection-jdbc.png)

Following considerations are necessary for the data connection

* Subnet: Select the private subnet with the NAT gateway attached to it. 
* Security groups: Check security group that has an entry of the the NAT gateway elastic IP. 

![glue-dataconnection-subnet-secgroup](https://github.com/aws-samples/aws-etl-microservice-redshift-datalake/blob/master/glue-dataconnection-subnet-secgroup.png)

## Cloudwatch dashboard
The AWS Lambda and AWS Glue jobs will create two notifications- success and failure, each under Lambda-ETL and Glue-ETL namespace.
You can use those metrics to build some dashboard for monitoring your jobs processes.

![cwalarm-metrics](https://github.com/aws-samples/aws-etl-microservice-redshift-datalake/blob/master/cwalarm-metrics.png)

![cwalarm-dashboard](https://github.com/aws-samples/aws-etl-microservice-redshift-datalake/blob/master/cwalarm-dashboard.png)


## Conclusion
As you schedule the lambda function for any of your tables data unload into s3, it will start putting your Redshift data into s3 in partition_key = partition_value folder as below.

![datalake-s3-folder](https://github.com/aws-samples/aws-etl-microservice-redshift-datalake/blob/master/datalake-s3-folder.png)

