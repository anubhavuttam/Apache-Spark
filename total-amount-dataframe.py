from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpent").getOrCreate()


schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("idk", IntegerType(), True), \
                     StructField("amount", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///C:/Users/Kakashi/Desktop/Apache-Spark/customer-orders.csv")
df.printSchema()

totalByCustomer = df.groupBy("customerID").agg(func.sum("amount").alias("totalAmount"))

totalByCustomerSort = totalByCustomer.sort(func.desc("totalAmount"))

totalByCustomerSort.show()

spark.stop()
