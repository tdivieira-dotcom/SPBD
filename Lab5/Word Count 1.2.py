#@title 1.2)
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('wordfrequency1.2)').getOrCreate()
sc = spark.sparkContext

try :
  lines = sc.textFile('os_maias.txt') \
            .filter( lambda line : len(line) > 1 )

  structured_lines = lines.map( lambda line : Row( line = line, listOfWords = line.split(' ') ) )

  wordsOfLine = spark.createDataFrame( structured_lines )
  
  words = wordsOfLine.withColumn('words', explode('listOfWords')) #cria nova coluna words que "explode" a coluna listOfWords criando várias linhas para cada palavra                                                                        presente na linha
  words = words.drop('line', 'listOfWords') #elimina as colunas line e list of words, ficando apenas a coluna words com uma palavra por linha
  frequencies = words.groupBy('words').count() #juntar as keys e somar a sua frequencia
  sortedFrequencies = frequencies.orderBy('count', ascending=False) #ordenar pelas mais frequentes 
  top3Frequencies = sortedFrequencies.limit(10) #apenas as 3 mais frequentes
  top3Frequencies.show()

except Exception as err:
  print(err)
  sc.stop()
