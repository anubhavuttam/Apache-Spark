from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
hadoop_ver = spark.sparkContext._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()
print("Hadoop version:", hadoop_ver)
