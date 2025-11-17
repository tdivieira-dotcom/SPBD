#@title 1.1a)
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('wordfrequency1.1a)').getOrCreate()
sc = spark.sparkContext

try :
  lines = sc.textFile('os_maias.txt') \
            .filter( lambda line : len(line) > 1 )

  structured_lines = lines.map( lambda line : Row( line = line, listOfWords = line.split(' ') ) )

  wordsOfLine = spark.createDataFrame( structured_lines )

  wordsOfLine.printSchema()

  wordsOfLine.show(5)
except Exception as err:
  print(err)
  sc.stop()
