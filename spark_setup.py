from pyspark.sql import SparkSession

def create_spark_session(KEY, SECRET):
    spark = SparkSession \
        .builder \
        .appName("S3 JSON TEST") \
        .getOrCreate()
        
    # Some other configs
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", KEY)
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", SECRET)

    #spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    return spark