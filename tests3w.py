import pyspark
from pyspark.sql import SparkSession

WRITE_PATH = "s3a://synthea-output/output"

spark = SparkSession \
    .builder \
    .appName("Test Writing to s3") \
    .config("spark.pyspark.python", "python") \
    .config("spark.sql.parquet.compression.codec", "gzip") \
    .getOrCreate()

data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]

df=spark.createDataFrame(data,columns)
df.show()

df.write.parquet(WRITE_PATH, mode="overwrite")
