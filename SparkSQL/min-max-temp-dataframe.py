from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType 

spark = SparkSession.builder.appName("WordCount").getOrCreate()

#custom schema
schema = StructType([\
    StructField("stationID", StringType(), True),\
    StructField("date", IntegerType(), True),\
    StructField("measure_type", StringType(), True),\
    StructField("temperature", FloatType(), True),
])

df = spark.read.schema(schema).csv("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/Sample-Dataset/1800.csv")
df.printSchema()

minTemp = df.filter(df.measure_type == "TMIN")

stationTemp = minTemp.select("stationID","temperature")

minTempByStation = stationTemp.groupBy("stationID").min("temperature")
minTempByStation.show()

results = minTempByStation.collect()

for result in results:
    print(result)

spark.stop()

