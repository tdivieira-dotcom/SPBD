#@title Weblog Analysis1
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Weblog Analysis1") \
    .getOrCreate()

sc = spark.sparkContext

try:
    #colocar cada linha a minusculas, sem acentos nem quebras de linha
  lines = sc.textFile('web.log') \
            .map(lambda line: line.split('\t')) \
            .filter( lambda line: len(line) == 6)


  ips = lines.map(lambda line: (line[1],1))\      #crias tuples ip,1 
            .reduceByKey(lambda a,b: 1)           #a cada ip com varios 1s, fica apenas com 1

  number_unique_ips= ips.count() #count devolve o nº de elementos do RDD ips

 print(f"{number_unique_ips}")

except Exception as e:
  print(e)

