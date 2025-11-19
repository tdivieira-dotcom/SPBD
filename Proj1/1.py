from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]') \
						.appName('taxis').getOrCreate()

# Longitude and latitude from the upper left corner of the grid
MIN_LON = -74.916578
MAX_LAT = 41.47718278

# Longitude and latitude that correspond to a shift in 500 meters
LON_DELTA = 0.005986
LAT_DELTA = 0.004491556

def latlon_to_grid_py(lat, lon):
    if lat is None or lon is None:
        return None
    try:
      r= int((MAX_LAT - float(lat))/LAT_DELTA)
      c= int((float(lon) - MIN_LON)/LON_DELTA)
      return [r, c]
    except:
      return None

# ------------------------
# CÓDIGO PRINCIPAL
# ------------------------
try :
      # Carregar dados
      df = spark.read.csv('taxi_rides_1pc.csv.gz', sep =',', header=True, inferSchema=True) 

      # Registrar UDF
      udf_latlon = udf(latlon_to_grid_py, ArrayType(IntegerType()))  

      # Criar células pickup_cell e drop_off_cell
      df = df.withColumn('pickup_cell', udf_latlon(col('pickup_latitude'), col('pickup_longitude'))) \ 
             .withColumn('dropoff_cell', udf_latlon(col('dropoff_latitude'), col('dropoff_longitude')))

      # Filtrar apenas células válidas dentro do grid
      df_clean = df.filter(
      (col('pickup_cell').isNotNull()) & (col('dropoff_cell').isNotNull()) &
      (col('pickup_cell').getItem(0) > 0) & (col('pickup_cell').getItem(0) < 300) &
      (col('pickup_cell').getItem(1) > 0) & (col('pickup_cell').getItem(1) < 300) &
      (col('dropoff_cell').getItem(0) > 0) & (col('dropoff_cell').getItem(0) < 300) &
      (col('dropoff_cell').getItem(1) > 0) & (col('dropoff_cell').getItem(1) < 300)
                )
      print("Total linhas (após filtro de bounds):", df_clean.count())

except Exception as err:
      print(err)
