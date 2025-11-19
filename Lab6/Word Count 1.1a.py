
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try :
  lines = sc.textFile('os_maias.txt') \
            .filter( lambda line : len(line) > 1 ) \
            .map( lambda line : Row( line = line ) )

  linesDF = spark.createDataFrame( lines )
  linesDF.createOrReplaceTempView("OSMAIAS")

  x = spark.sql("SELECT count(*) as lines FROM OSMAIAS")

  x.show(5)
except Exception as err:
  print(err)
  sc.stop()
