
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


  tuple = lines.map(lambda line: (line[0][0:18],line[4],line[1]))\      #crias tuples ip,1 
          .count() #remove os ips duplicados

 print(f"{number_unique_ips}")

except Exception as e:
  print(e)
