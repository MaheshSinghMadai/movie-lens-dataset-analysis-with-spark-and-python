from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField,IntegerType, StringType

spark = SparkSession.builder.appName("MostObsecureSuperhero").getOrCreate()

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]
)

names = spark.read.schema(schema).option("sep", " ").csv("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/Sample-Dataset/Marvel-Names.txt")
lines = spark.read.text("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/Sample-Dataset/Marvel-Graph.txt")

#extract the hero ID (first number on the line)
#count the connections of each. -1 for excluding oneself in the count
#group by each connection and calculate the sum 
# final result = (heroID, total_coapperances)
connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
    .withColumn("connections",func.size(func.split(func.col("value"), " ")) - 1)\
    .groupBy("id").agg(func.sum("connections").alias("connections"))

#minimum connection value
minConnectionCount = connections.agg(func.min("connections")).first()[0]

#filter the hero who has minimum connection count
minConnections = connections.filter(func.col("connections") == minConnectionCount)

#join with names dataframe on id to get the superhero name and select the name
minConnectionsWithNames = minConnections.join(names, "id")
minConnectionsWithNames.select("name").show()

spark.stop()