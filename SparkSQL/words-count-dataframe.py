from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputText = spark.read.text("file:///C:/Users/MrKillShOtzz/source/repos/movie-analysis-apache-spark/Sample-Dataset/book.txt")

words = inputText.select(func.explode(func.split(inputText.value,"\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

wordCounts = lowercaseWords.groupBy("word").count()

wordCountsSorted = wordCounts.sort("count")
wordCountsSorted.show(wordCountsSorted.count())

spark.stop()

