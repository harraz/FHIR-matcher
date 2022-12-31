import pyspark
from delta import *
from libfunctions import *

s3 = boto3.resource('s3')

builder = make_spark_builder()

spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-hadoop-cloud_2.12:3.3.0"]).getOrCreate()
print(spark.sparkContext)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

sc =  spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
sc._jsc.hadoopConfiguration().set("fs.s3a.fast.upload", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.fast.upload.buffer", "bytebuffer")

s3BucketName = 'synthea-output'
s3FolderName =  'dupes' # 'dupes' Bundles

df = spark.read.format("delta").load('s3a://' + s3BucketName + '/' + 'output/delta/encounters')
df.show()