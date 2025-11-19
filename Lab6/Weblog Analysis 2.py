#@title 2.2
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]') \
						.appName('weblog').getOrCreate()

try :
    spark.read.csv('weblog_with_header.log', sep =' ', header=True, inferSchema=True) \
        .createOrReplaceTempView("WebLog")

    x = spark.sql("SELECT WINDOW(date, '10 seconds').start AS interval, \
                    COUNT(*) AS numOfRequests,\
                    AVG(time) AS avgTime, \
                    MAX(time) AS maxTime, \
                    MIN(time) AS minTime FROM WebLog\
                    GROUP BY interval ORDER BY interval")
    x.show()
except Exception as err:
    print(err)
