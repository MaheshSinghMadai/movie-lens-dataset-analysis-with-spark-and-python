from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType
import sys

def computeCosineSimilarity(spark, data):
    #compute xx, xy and yy columns
    pairScores = data.withColumn("xx", func.col("ratings1")* func.col("ratings1"))\
                        .withColumn("yy", func.col("ratings2")* func.col("ratings2"))\
                        .withColumn("xy", func.col("ratings1")* func.col("ratings2"))
    
    #compute numerator, denominator and numPairs columns
    calculateSimilarity = (
    pairScores.groupBy("movie1", "movie2")
    .agg(
        func.sum(func.col("xy")).alias("numerator"),
        (func.sqrt(func.sum(func.col("xx"))) * 
         func.sqrt(func.sum(func.col("yy")))).alias("denominator"),
        func.count(func.col("xy")).alias("numPairs")
         )
    )

    # Calculate score and select only needed columns
    result = (
        calculateSimilarity.withColumn(
            "score",
            func.when(
                func.col("denominator") != 0,
                func.col("numerator") / func.col("denominator")
            ).otherwise(0)
        )
        .select("movie1", "movie2", "score", "numPairs")
    )                 
    return result

def getMovieName(movieNames, movieId):
    result = movieNames.filter(func.col("movieID") == movieId).select("movieTitle").collect()[0]
    return result[0]

spark = SparkSession.builder.appName("MovieSimilarities").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#create schema for movieName and movie
movieNameSchema = StructType(
    [
        StructField("movieID", IntegerType(), True),
        StructField("movieTitle", StringType(), True)
    ]
)

movieSchema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", IntegerType(), True)
    ]
)

# apply above created schema
movieNames = spark.read.option("sep","|") \
                        .option("charset","ISO-8859-1")\
                        .schema(movieNameSchema) \
                        .csv("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/ml-100k/u.item")

movies = spark.read.option("sep","\t") \
                        .schema(movieSchema) \
                        .csv("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/ml-100k/u.data")

ratings = movies.select("userId","movieId","rating")

moviePairs = ratings.alias("ratings1") \
    .join(
        ratings.alias("ratings2"),
        (func.col("ratings1.userId") == func.col("ratings2.userId")) &
        (func.col("ratings1.movieId") < func.col("ratings2.movieId"))
    ) \
    .select(
        func.col("ratings1.movieId").alias("movie1"),
        func.col("ratings2.movieId").alias("movie2"),
        func.col("ratings1.rating").alias("ratings1"),
        func.col("ratings2.rating").alias("ratings2")
    )

moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

if (len(sys.argv) > 1):
    scoreThresold = 0.97
    coOccurenceThreshold = 50.0

    movieId = int(sys.argv[1])

    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieId) | (func.col("movie2") == movieId)) &
        (func.col("score") > scoreThresold) & (func.col("numPairs") > coOccurenceThreshold)
    )

    results = filteredResults.sort(func.col("score").desc()).take(10)

    print("Top 10 similar movies for " + getMovieName(movieNames, movieId))

    for result in results:
        similarMovieId = result.movie1
        if(similarMovieId == movieId):
            similarMovieId = result.movie2
        
        print(getMovieName(movieNames, similarMovieId) + "\tscore: " + str(result.score) + "\tstrength: " + str(result.numPairs))