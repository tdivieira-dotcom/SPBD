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
    #colocar cada linha a minusculas, sem acentos nem quebras de linha
  lines = sc.textFile('os_maias.txt') \
            .filter(lambda line: len(line) > 0) \
            .map(lambda line: line.strip()) \
            .map(lambda line: unicode(line).lower()) \
            .map(lambda line: line.translate(str.maketrans('','', string.punctuation+'«»')))

   #dividir cada linha em words, criar par {word,1} e agrupar por keys (fazendo a sua soma)
  words = lines.flatMap(lambda line : line.split()) \  #lambda é uma função que a cada linha divide em words
            .map(lambda word: (word,1))\     #lambda é uma função que a cada word cria um par {word,1}
            .reduceByKey(lambda a,b: a+b)   #lambda é uma função que a cada elemento a cada key soma os seus valores

  for word, count in words.take(10):
    print(f"{word}:{count}")

except Exception as e:
  print(e)

