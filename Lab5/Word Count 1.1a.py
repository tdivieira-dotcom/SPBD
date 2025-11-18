#@title 1.1a)
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import string
from unidecode import unidecode

spark = SparkSession.builder.master('local[*]').appName('wordfrequency1.1a)').getOrCreate()
sc = spark.sparkContext

def cleanup_lines( line ):
  return unidecode(line).translate(str.maketrans('', '', string.punctuation+'«»')).lower().strip()

cleanup_lines_udf = udf(cleanup_lines, StringType())

try :
  lines = sc.textFile('os_maias.txt') \
            .filter( lambda line : Row(line=1))

  structures_lines= spark.createDataFrame(lines)\
          .where( length('line') > 0 ) \
          .withColumn( "clean_lines", cleanup_lines_udf('line')) \
          .drop('line')
  
  wordsOfLine = structures_lines.withColumn('words', explode(split(structured_lines.clean_lines,' '))) \ #cria nova coluna words que "explode" a coluna listOfWords criando várias linhas para cada palavra presente na linha
                     .drop('clean_lines') #elimina as colunas line e list of words, ficando apenas a coluna words com uma palavra por linha
  
  frequencies = wordsOfLine.groupBy('words').count() #juntar as keys e somar a sua frequencia
  frequencies.show(5)

except Exception as err:
  print(err)
  sc.stop()
