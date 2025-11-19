#@title 2.1
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]') \
						.appName('weblog').getOrCreate()

try :
    spark.read.csv('weblog_with_header.log', sep =' ', header=True, inferSchema=True) \
        .createOrReplaceTempView("WebLog")

    x = spark.sql("SELECT COUNT(DISTINCT ipSource) AS total_ips FROM WebLog")

    x.show()
except Exception as err:
    print(err)
