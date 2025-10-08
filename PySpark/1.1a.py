#@title WordFrequecy Spark Core 1.1a)
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
            .map(lambda line: line.translate(str.maketrans('','', string.punctuation+'')))

  words = lines.flatMap(lambda line : line.split()) \
            .map(lambda word: (word,1))\
            .reduceByKey( lambda a,b: a+b)

  
  
  for w in words.collect(10):
    print(w)

except Exception as e:
  print(e)

