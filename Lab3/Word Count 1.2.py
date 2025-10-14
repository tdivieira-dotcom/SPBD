
#@title WordFrequecy Spark Core 1.2)
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("WordFrequency1.2") \
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

  # Top-10 por partição (pré-seleção local)
 top10_per_partition = words.mapPartitions( #isto é aplicado a grandes partições de tuplas (em vez de fazermos o sorting de 1000 tuplas diretamente, por ex.)
    lambda partition: sorted(partition, key=lambda k: k[1], reverse=True)[:10] #ordenamos as tuplas por frequencia e apenas retiramos as 10 mais frequentes([:10])
)

  top10_global = top10_per_partition.sortBy(lambda x: x[1], ascending=False)\ #false- da maior frequência para a menor, o x é o RDD com a tupla por isso x[1] ordena pela frequência
  .zipWithIndex()\                                                          #As palavras mais frequentes têm indice a começar em 0
  #O filter faz com que apenas tenhamos os 10 tuplos mais frequentes pois apenas queremos até ao indice(x[1]) menor do que 10
  #O map faz com que eliminemos a parte do indice da tupla pois só queremos o x[0] (word, count)
  .filter(lambda x: x[1] < 10)\
  .map(lambda x: x[0]) 
    
print("Top-10 Most Frequent Words:")
for word, count in top10_global.collect():
    print(f"{word}: {count}")

except Exception as e:
  print(e)

