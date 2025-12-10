from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

data = spark.read.option("header","true").option("inferSchema","true").csv("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/Sample-Dataset/fakefriends-header.csv")

# print("Here is our infered schema")
# data.printSchema()

# print("Let's display the name column")
# data.select("name").show()

# print("Filter out anyone above age 21")
# data.filter(data.age > 21).show()

# print("Group by Age")
# data.groupBy("age").count().show()

# print("Group by Age")
# data.groupBy("age").count().show()

selectedColumns = data.select("age","friends")
selectedColumns.groupBy("age").agg(func.round(func.avg("friends"),2)).sort("age").show()


spark.stop()

