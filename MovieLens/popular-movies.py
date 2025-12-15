from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField,IntegerType, LongType
import codecs

#function to load movie names
def load_movie_names():
    movie_names = {}
    with codecs.open("C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/ml-100k/u.item","r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

#broadcast movie name 
movies_name_dict = spark.sparkContext.broadcast(load_movie_names())

#create a schema
movielens_schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

#load up movie data as a dataframe
movie_df =  spark.read.option("sep","\t").schema(movielens_schema).csv("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/ml-100k/u.data")

#movies count
movies_count = movie_df.groupBy("movieID").count()

def lookupName(movieID):
    return movies_name_dict.value[movieID]

lookupNameDf = func.udf(lookupName)

#add movie title column
movies_with_names = movies_count.withColumn("movieTitle", lookupNameDf(func.col("movieID")))

#sort the results
movies_with_names_sorted = movies_with_names.orderBy(func.desc("count"))

#display top 10 movies
movies_with_names_sorted.show(10)

spark.stop()