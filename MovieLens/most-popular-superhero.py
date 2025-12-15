from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField,IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]
)

names = spark.read.schema(schema).option("sep", " ").csv("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/Sample-Dataset/Marvel-Names.txt")
lines = spark.read.text("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/Sample-Dataset/Marvel-Graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])\
    .withColumn("connections",func.size(func.split(func.col("value"), " ")) - 1)\
    .groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

mostPopularMovieName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularMovieName[0] + "is the most popular superhero with " + str(mostPopular[1]) + " co-appearances")

spark.stop()