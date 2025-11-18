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
  
# 1) Explode a coluna complexa (listOfWords)
words = wordsOfLine.withColumn('words', explode('listOfWords'))

# 2) Remover colunas antigas
words = words.drop('line', 'listOfWords')

# 3) Agrupar pelas palavras
frequencies = words.groupBy('words').count()

# 4) Ordenar pelas mais frequentes
sortedFrequencies = frequencies.orderBy('count', ascending=False)

# 5) Limitar às top 3
top3Frequencies = sortedFrequencies.limit(3)
top3Frequencies

except Exception as err:
  print(err)
  sc.stop()
