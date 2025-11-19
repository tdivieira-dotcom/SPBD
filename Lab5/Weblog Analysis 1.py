from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').getOrCreate()
try :
  
  logRows = spark.read.csv('weblog_with_header.log', sep =' ', header=True, inferSchema=True)

  logRows.printSchema()
  countIPs = logRows.select('ipSource').distinct().count() #seleciona a coluna Ip, elimina as repetidas e conta quantas diferentes existem
  print(countIPs) 
except Exception as err:
  print(err)
