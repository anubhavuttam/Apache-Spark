from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("C:/Users/Kakashi/Desktop/Apache-Spark/Marvel-names.txt")

lines = spark.read.text("C:/Users/Kakashi/Desktop/Apache-Spark/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

minConnectionCount = connections.agg(func.min("connections")).first()[0]
    
mostObscure = connections.sort(func.col("connections").asc()).first()

mostObscureName = names.filter(func.col("id") == mostObscure[0]).select("name").first()

print(mostObscureName[0] + " is the most obscure superhero with " + str(mostObscure[1]) + " co-appearances.")

minConnections = connections.filter(func.col("connections") == minConnectionCount)

minConnectionsWithNames = minConnections.join(names,"id")

print("The following are the most obscure superheros with minimum Connections " + str(minConnectionCount) + " connection(s): ")

minConnectionsWithNames.select("name").show()