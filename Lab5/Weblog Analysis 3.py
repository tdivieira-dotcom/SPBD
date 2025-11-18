from os import truncate
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

def toInterval( date ):
  seconds = int(date.timestamp() / 10) * 10
  return datetime.fromtimestamp(seconds)

spark = SparkSession.builder.master('local[*]').getOrCreate()

try :
    logRows = spark.read.csv('weblog_with_header.log',
                             sep =' ', header=True, inferSchema=True)

    toInterval_udf = udf(toInterval, TimestampType())

    intervals = logRows.select(toInterval_udf('date').alias("interval"), 'ipSource', "url")

    stats = intervals.groupBy('interval', 'url').agg( collect_set('ipSource').alias('ips')) \
              .orderBy('interval', 'url', ascending=False)

    stats.show(10, truncate = False)

except Exception as err:
    print(err)
