#@title 2.3
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]') \
						.appName('weblog').getOrCreate()

try :
    spark.read.csv('weblog_with_header.log', sep =' ', header=True, inferSchema=True) \
        .createOrReplaceTempView("WebLog")

    x = spark.sql("SELECT WINDOW(date, '10 seconds').start AS interval, \ 
                    url, \
                    COLLECT_SET( ipSource) AS ips FROM WebLog\
                    GROUP BY interval, url \
                    ORDER BY interval DESC, url ASC")
    x.show()
except Exception as err:
    print(err)
