#@title Weblog Analysis2
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Weblog Analysis1") \
    .getOrCreate()

sc = spark.sparkContext

try:
  lines = sc.textFile('web.log') \
            .map(lambda line: line.split('\t')) \
            .filter( lambda line: len(line) == 6)

  
  tuple = lines.map(lambda line: (line[0][0:18],float(line[5])))\      #crias tuples timestamp até caracter 18,execution time em float
          .map( lambda kv : (kv[0], (1, kv[1], kv[1], kv[1]))) \       #para cada tupla transformá-la em tuplas (timestamp; 1,exec_time,exec_time,exec_time)
          #aqui o que fazes é pegar em 2 requests diferentes do mesmo timestamp e fazes uma redução local 
          #para cada timestamp atualizas a tupla para (timestamp; 1+1, exec_time+exec_time, exec_time max, exec_time min)
          .reduceByKey( lambda a, b : (a[0] + b[0], a[1] + b[1], max(a[2],b[2]), min(a[3],b[3])) ) \ 
          #para cada tupla ficas com (timestamp, campo 0 do [1] da tupla, média de exec_time dada pela soma total/number of requests, campo 2 do [1] da tupla, campo 3 do [1] da tupla)
          .map( lambda kv : (kv[0], (kv[1][0], kv[1][1] / kv[1][0], kv[1][2], kv[1][3]))) \
          .sortByKey() #ordenar por timestamp

    for timestamp in tuple.take(10)
       print(timestamp)
  


except Exception as e:
  print(e)

