from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import time
from datetime import datetime

def toInterval( date ):
  seconds = int(date.timestamp() / 10) * 10
  return datetime.fromtimestamp(seconds)

spark = SparkSession.builder.master('local[*]').getOrCreate()
try :
  
  logRows = spark.read.csv('weblog_with_header.log', sep =' ', header=True, inferSchema=True)
  toInterval_udf = udf(toInterval, TimestampType()) #criar UDF

  intervals = logRows.select(toInterval_udf('date').alias("interval"), 'time') #2 colunas- interval e time

  stats = intervals.groupBy('interval').agg( count('*').alias('count'), avg('time'), min('time'), max('time')) \ #criar colunas count, avg time, min time e max time.
        .orderBy('interval')

  stats.printSchema()
  stats.show(10)
except Exception as err:
  print(err)
