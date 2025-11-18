from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master('local[*]') \
.appName('words').getOrCreate()
sc = spark.sparkContext
try :
  lines = sc.textFile('web.log')
  logRows = lines.filter( lambda line : len(line) > 0 ) \
  .map( lambda line : line.split(' ') ) \
  .map( lambda l : Row( date = l[0], \
  ipSource = l[1], retValue = l[2], \
  op = l[3], url = l[4], time = float(l[5])))
  logRowsDF = spark.createDataFrame( logRows )
  
  
  listIpsDF = logRowsDF.select('ipSource').distinct()
  listIpsDF.show(10)
  sc.stop()
except err:
  print(err)
  sc.stop()
