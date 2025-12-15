from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf, udf
from pyspark.sql.types import IntegerType
import re

#user defined table function 
@udtf(returnType="hashtag: string")
class HashTagExtractor:
    def eval(self, text: str):
        if text:
            hashtags = re.findall(r"#\w+", text)
            for hashtag in hashtags:
                yield(hashtag,)

# user defined function (udf)
def count_hashtags(text: str):
    if text:
        return len(re.findall(r"#\w+", text))
    return 0

#initialize spark session
spark = SparkSession.builder.appName("UDF and UDTF examples").config("spark.sql.execution.pythonUDTF.enabled","True").getOrCreate()

#register custom hashtag extractor functions
spark.udtf.register("extract_hashtags", HashTagExtractor)
spark.udf.register("count_hashtags", count_hashtags)

print("UDTF Example: ")
spark.sql("Select * from extract_hashtags('Welcome to ApachaeSpark and #BigData!')").show()

print("UDF Example: ")
spark.sql("Select count_hashtags('Welcome to ApachaeSpark and #BigData!') as hashtag_count").show()


#using both udf and udtf with a dataframe
data = [("Learning #AI with #ML",),("Exploring #DataScience",),("No hashtags here",)]
df = spark.createDataFrame(data,["text"])

#apply udf in a dataframe query
df.selectExpr("text","count_hashtags(text) as num_hashtags").show()

#apply udtf with a LATERAL JOIN
df.createOrReplaceTempView("tweets")
spark.sql("select text, hashtag from tweets, LATERAL extract_hashtags(text)").show()

spark.stop()