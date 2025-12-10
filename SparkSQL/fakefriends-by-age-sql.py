from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("FriendsCountByAgeSparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), 
                      age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/Sample-Dataset/fakefriends.csv")
people = lines.map(mapper)

schemaPoeple = spark.createDataFrame(people).cache()
schemaPoeple.createOrReplaceTempView("people")

teenager = spark.sql("Select * from people where age between 13 and 19")

for teen in teenager.collect():
    print(teen)

schemaPoeple.groupBy("age").count().orderBy("age").show()
spark.stop()

