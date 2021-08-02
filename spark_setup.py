from pyspark.sql import SparkSession

def create_spark_session(KEY, SECRET):
    """
    Returns a spark session with the right configuration.
    Hadoop is also configured to read from S3.
    """
    spark = SparkSession \
        .builder \
        .appName("S3 Data Lake") \
        .config("spark.executor.heartbeatInterval", "3600s")\
        .config("spark.network.timeout", "36000s")\
        .config("spark.driver.memory", "16g")\
        .config("spark.driver.maxResultSize", "16g")\
        .config("spark.executor.memory", "16g")\
        .config("spark.python.worker.memory", "2g")\
        .getOrCreate()
        
    # Some other configs
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", KEY)
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", SECRET)

    #spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    return spark