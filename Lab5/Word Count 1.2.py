#@title 1.1c)
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

import string
from unidecode import unidecode

spark = SparkSession.builder.master('local[*]').appName('wordfrequency1.1c)').getOrCreate()
sc = spark.sparkContext

try :
  lines = sc.textFile('os_maias.txt') \
            .filter( lambda line : len(line) > 1 ) \
            .map(lambda line: unidecode(line) \
                                .translate(str.maketrans('', '', string.punctuation + '«»'))\
                                .lower()\
                                .strip())

  structured_lines = lines.map( lambda line : Row( clean = line, listOfWords = line.split(' ') ) )

  wordsOfLine = spark.createDataFrame( structured_lines )
  
  words = wordsOfLine.withColumn('words', explode('listOfWords')) #cria nova coluna words que "explode" a coluna listOfWords criando várias linhas para cada palavra                                                                        presente na linha
  words = words.drop('line', 'listOfWords') #elimina as colunas line e list of words, ficando apenas a coluna words com uma palavra por linha
  frequencies = words.groupBy('words').count() #juntar as keys e somar a sua frequencia
  sortedFrequencies = frequencies.orderBy('count', ascending=False) #ordenar pelas mais frequentes 
  top3Frequencies = sortedFrequencies.limit(10) #apenas as 10 mais frequentes
  top3Frequencies.show()

except Exception as err:
  print(err)
  sc.stop()
