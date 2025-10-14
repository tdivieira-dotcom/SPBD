#@title WordFrequecy Spark Core 1.1b)
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("WordFrequency1.1a") \
    .getOrCreate()

sc = spark.sparkContext

try:
  lines = sc.textFile('os_maias.txt') \
            .filter(lambda line: len(line) > 0) \
            .map(lambda line: line.strip()) \
            .map(lambda line: unicode(line).lower()) \
            .map(lambda line: line.translate(str.maketrans('','', string.punctuation+'«»')))

  words = lines.flatMap(lambda line : line.split()) \
            .map(lambda word: (word,1))\
            .reduceByKey( lambda a,b: a+b)

  
  reverse_words=sortByKey(ascending=False) #Ordenamos as keys ao contrário (Z-A)

  for word, count in reverse_words.take(10):
    print(f"{word}:{count}")

except Exception as e:
  print(e)

