
#@title Weblog Analysis3
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Weblog Analysis3") \
    .getOrCreate()

sc = spark.sparkContext

try:
  lines = sc.textFile('web.log') \
            .map(lambda line: line.split('\t')) \
            .filter( lambda line: len(line) == 6)

    
  tuple = lines.map(lambda line: (line[0][0:18],line[4]),line[1])\      #crias tuples timestamp,URL, IP
          .reduceByKey( lambda a, b : a | b ) \     # agregar os IPs se forem repetidos ficam como um conjunto
          .sortByKey()           #ordenar por timestamp

for t in tuple.collect():   #collect devolve a lista toda do RDD enquanto que take devolve os primeiros x elementos para take(x)
    print(t)
except Exception as e:
  print(e)
