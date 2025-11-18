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
  
    wordsOfLine = spark.createDataFrame(structured_lines)

# Explodir a lista de palavras 
df = wordsOfLine.select(explode(col("listOfWords")).alias("word"))

# Remover palavras vazias (acontece quando há múltiplos espaços)
df = df.drop("")     # <-- uso de drop, como pedido

# Contar frequências
freq = df.groupBy("word").count()

# Ordenar por frequência decrescente
freq_sorted = freq.orderBy(col("count").desc())

# Buscar as 3 mais frequentes
top3 = freq_sorted.limit(3)

top3.show()
except Exception as err:
  print(err)
  sc.stop()
