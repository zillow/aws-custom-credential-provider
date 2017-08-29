Custom AWS credential provider
=======
A custom credential provider for EMRFS that assumes a configurable role name for HDFS-based applications.
This library implements [custom EMRFS credential provider](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-credentialsprovider.html) for your Hadoop or Spark applications on EMR, so it can access S3 storage with a configurable AWS assume role.

# Usage
To use this library, follow these steps:
1. Download custom credential provider jar or build it from source.
2. Upload the custom credential jar to EMR cluster, and move it to one of the folders searchable in java *CLASSPATH* (e.g. /usr/share/aws/emr/emrfs/lib)
3. At the beginning of your Hadoop or Spark application, set `amz-assume-role-arn` to the AWS role name you plan to use:
```scala
spark.sparkContext.hadoopConfiguration.set("amz-assume-role-arn", jobArgs.params.getOrElse("roleName", "your-aws-role-arn"))
```

# Note
1. You should configure the role name before any other call to read/write from S3 file system. Otherwise the credential provider will not take effect.
2. You can not change the assume role in the same Hadoop or Spark application. Once it's set at the beginning of the application, the role name stays fixed until the application finishes.
3. If you have multiple custom credential providers for EMRFS, only one of them will take effect.

# How does custom credential provider work
When Hadoop/Spark applications access HDFS using `S3://` URI prefix, EMRFS will be initialized to handle the IO calls. At initialization, EMRFS looks for credential providers available including: (1) custom credential providers in current `CLASSPATH` (2) other default credential providers (local AWS profiles and EC2 profiles). After getting credential providers, EMRFS tries the credential providers according to their priority (custom providers have the highest priority). The first credential provider that returns a non-null `AWSCredentials` object wins and the credential provider will be cached for the entire application. HDFS will also periodically calls the cached credential provider to validate and renew the credentials.

Our custom credential provider gets the role name through `Hadoop` configuration mechanism, and calls `AWS Security Token Service (STS)` to return a valid `AWSCredentials` during initialization, and renews it during the entire application session.
