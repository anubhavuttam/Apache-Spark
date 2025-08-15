from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///C:/Users/Kakashi/Desktop/Apache-Spark/fakefriends-header.csv")


print("Avg Friends by Age:")
people.groupBy("age").avg("friends").sort("age").show()


spark.stop()