from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import string
from unidecode import unidecode

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext
def cleanup_lines( line ):
  return unidecode(line).translate(str.maketrans('', '', string.punctuation+'«»')).lower().strip()

try :
  spark.udf.register("cleanupLines", cleanup_lines, StringType())
  spark.read.text('os_maias.txt') \
        .withColumnRenamed('value', 'lines') \
        .createOrReplaceTempView("OSMAIAS")

  x = spark.sql("SELECT word, count(*) AS frequency FROM \
                   (SELECT explode(split(cleanupLines(lines), ' ')) AS word FROM OSMAIAS) \
                 GROUP BY word \
                 ORDER BY word DESC")

  x.show(truncate=False)
except Exception as err:
  print(err)
  sc.stop()
