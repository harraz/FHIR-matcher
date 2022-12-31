# FHIR-matcher


## spark submit command line that works
pyspark --packages io.delta:delta-core_2.12:1.2.1 --conf spark.jars.packages=io.delta:delta-core_2.12:1.2.1 --packages "org.apache.spark:spark-hadoop-cloud_2.12:3.3.0" --packages "org.apache.hadoop:hadoop-aws:3.2.2" --conf spark.jars.repositories=https://maven-central.storage-download.googleapis.com/maven2/
