from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType 

spark = SparkSession.builder.appName("totalSpendingByCustomer").getOrCreate()

#custom schema
schema = StructType([\
    StructField("customerID", IntegerType(), True),\
    StructField("itemID", IntegerType(), True),\
    StructField("amount", FloatType(), True)
])

df = spark.read.schema(schema).csv("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/Sample-Dataset/customer-orders.csv")

columns = df.select("customerID","amount")

spendingByCustomerID = columns.groupBy("customerID").agg(func.round(func.sum("amount"),2).alias("total_spent"))
spendingByCustomerIDSorted = spendingByCustomerID.sort("total_spent")
spendingByCustomerIDSorted.show(spendingByCustomerID.count())

spark.stop()

