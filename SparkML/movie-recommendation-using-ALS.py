from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs

#function to load movie names
def load_movie_names():
    movie_names = {}
    with codecs.open("C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/ml-100k/u.item","r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names

spark = SparkSession.builder.appName("ALSExample").getOrCreate()

#create a schema
movielens_schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

names = load_movie_names()

ratings = spark.read.option("sep","\t").schema(movielens_schema).csv("C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/ml-100k/u.data")

print("Training recommendation model...")

#ALS - Alternating Least Square
als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID").setRatingCol("rating")

model = als.fit(ratings)

#manual construction of dataframe
userID = int(sys.argv[1])           # for passing userID through console
userSchema = StructType([StructField("userID", IntegerType(), True)])
users = spark.createDataFrame([[userID,]], userSchema)

#generate 10 recommendations for the user ID passed
recommendations = model.recommendForUserSubset(users, 10).collect()

print("Top 10 recommendations for user ID: "+ str(userID))

for userRecs in recommendations:
    myRecs = userRecs[1]
    for rec in myRecs:
        movie = rec[0]
        rating = rec[1]
        movieName = names[movie]
        print(movieName + str(rating))