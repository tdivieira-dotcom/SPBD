# @title SPBD 2526 Project 1

# --- IMPORTAÇÕES ---
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import numpy as np
import copy

# --- Inicializar Sessão ---

spark = SparkSession.builder.master('local[*]') \
						.appName('NYC_Taxi_Analytics_Final').getOrCreate()

# --- CONSTANTES E HELPER CODE ---

# Limits of the NY area grid
MIN_LON = -74.916578
MAX_LAT = 41.47718278
LON_DELTA = 0.005986
LAT_DELTA = 0.004491556

# Function converting (lat, lon) → grid cell (r,c)
def latlon_to_grid_py(lat, lon):
    if lat is None or lon is None:
        return None
    try:
      r= int((MAX_LAT - float(lat))/LAT_DELTA)
      c= int((float(lon) - MIN_LON)/LON_DELTA)
      return [r, c]
    except:
      return None

# --- Marcos Geográficos Importantes ---

# --- Mapeados através de engenharia inversa das coordenadas GPS ---
landmarks = {
    # Aeroportos
    "JFK Airport": (186, 190),
    "LaGuardia (LGA)": (155, 174),
        
    # Manhattan
    "Midtown": (160, 155),
    "Downtown": (171, 151),
    "Meatpacking": (166, 158),
    "Battery Park": (181, 153),
    "Central Park": (154, 158),
    
    # Zonas Específicas
    "South Ozone Park": (176, 185), # O hotspot constante (08h, 16h, 22h)
    "Queens/Woodside": (158, 166),  # O hotspot das 04h (zona residencial)
    "Brooklyn": (177, 157)
}

# --- UDFs ---

# --- Vinda do Helper Code --- 
udf_latlon = udf(latlon_to_grid_py, ArrayType(IntegerType()))

# --- Recebe uma célula da grelha e retorna: nome do marco mais próximo e link do Google Maps para validação visual ---
def get_zone_info(row, col):

    # Converter Grid -> GPS Real
    lat = MAX_LAT - (row * LAT_DELTA)
    lon = MIN_LON + (col * LON_DELTA)
    
    # Encontrar marco mais próximo (Raio de ~5km / 10 células)
    closest_name = "Outros"
    min_dist = 10 
    
    # Função auxiliar para evitar colisão com o abs() do Spark
    def safe_abs(val):
        return val if val >= 0 else -val

    for name, (lr, lc) in landmarks.items():
        dist = safe_abs(row - lr) + safe_abs(col - lc)
        if dist < min_dist:
            min_dist = dist
            closest_name = name
            
    link = f"https://www.google.com/maps/search/?api=1&query={lat:.4f},{lon:.4f}"
    return f"{closest_name}|{link}"

# Registar a UDF para usar no DataFrame
zone_info_udf = udf(get_zone_info, StringType())


# --- PROCESSAMENTO DE DADOS ---

try :
      # Carregar dados
      df = spark.read.csv('taxi_rides_1pc.csv.gz', sep =',', header=True, inferSchema=True)

      # Transformação usando a UDF disponibilizada
      df_grid = df.withColumn('pickup_cell', udf_latlon(col('pickup_latitude'), col('pickup_longitude'))) \
             .withColumn('dropoff_cell', udf_latlon(col('dropoff_latitude'), col('dropoff_longitude')))

      # Separar o Array [Row, Col] em colunas individuais para poder agrupar
      df_expanded = df_grid.withColumn("p_row", col("pickup_cell")[0]) \
                         .withColumn("p_col", col("pickup_cell")[1]) \
                         .withColumn("d_row", col("dropoff_cell")[0]) \
                         .withColumn("d_col", col("dropoff_cell")[1]) \
                         .withColumn("hour", hour("pickup_datetime")) \
                         .withColumn("total_profit", col("fare_amount") + col("tip_amount"))

      # Filtrar Limites (0 a 300) e Nulos
      df_processed = df_expanded.filter(
        (col("p_row") > 0) & (col("p_row") < 300) &
        (col("p_col") > 0) & (col("p_col") < 300) &
        (col("d_row") > 0) & (col("d_row") < 300) &
        (col("d_col") > 0) & (col("d_col") < 300)
                )
      
      # Filtros de Negócio (Viagens > 60s e Lucro > 0)
      df_clean_logic = df_processed.filter(
        (col("trip_time_in_secs") > 60) & 
        (col("total_profit") > 0)
              )
      # Calcular Procura e Oferta
      demand_df = df_clean_logic.groupBy("p_row", "p_col", "hour") \
        .agg(avg("total_profit").alias("avg_revenue"), count("*").alias("num_trips"))

      supply_df = df_processed.groupBy("d_row", "d_col", "hour") \
        .agg(count("*").alias("empty_taxis"))

      # Join e Index
      profitability_df = demand_df.join(supply_df,
        (demand_df.p_row == supply_df.d_row) &
        (demand_df.p_col == supply_df.d_col) &
        (demand_df.hour == supply_df.hour),
        "inner"
        ).select(
        demand_df.hour,
        demand_df.p_row.alias("grid_row"),
        demand_df.p_col.alias("grid_col"),
        "avg_revenue",
        "empty_taxis",
        "num_trips"
        )

    # Dataset Final Robusto
      final_df_robust = profitability_df.filter(
        (col("empty_taxis") >= 3) & 
        (col("num_trips") >= 3)
       ).withColumn("profitability_index", col("avg_revenue") / col("empty_taxis")) \
      .withColumn("avg_revenue", round(col("avg_revenue"), 2)) \
      .withColumn("profitability_index", round(col("profitability_index"), 2)) \
      .withColumn("zone_data", zone_info_udf(col("grid_row"), col("grid_col")))

      final_df_robust.cache()

except Exception as err:
      print(err)

# --- CRIAÇÃO DE RELATÓRIOS (TABELAS E MAPAS) ---

def generate_hour_report(target_hour):
    print(f"\n{'='*100}")
    print(f"   RELATÓRIO ANALÍTICO: {target_hour}:00 ")
    print(f"{'='*100}\n")
    
    # Tabela
    top_5 = final_df_robust.filter(col("hour") == target_hour) \
                           .orderBy(col("profitability_index").desc()) \
                           .limit(5) \
                           .toPandas()
    
    if top_5.empty:
        print("Sem dados suficientes.")
        return

    print(f"{'ZONA APROXIMADA':<22} | {'LUCRO($)':<9} | {'VIAGENS':<7} | {'TAXIS':<5} | {'INDEX':<7} | {'LINK MAPS'}")
    print("-" * 115)

    for _, row in top_5.iterrows():
        zone_name, link = row['zone_data'].split('|')
        print(f"{zone_name:<22} | ${row['avg_revenue']:<8} | {int(row['num_trips']):<7} | {int(row['empty_taxis']):<5} | {row['profitability_index']:<7} | {link}")
    print("\n")

    # Heatmap
    data_heatmap = final_df_robust.filter(col("hour") == target_hour) \
                                  .select("grid_row", "grid_col", "profitability_index") \
                                  .toPandas()
    
    grid_matrix = np.zeros((300, 300))
    for _, row in data_heatmap.iterrows():
        r, c = int(row['grid_row']), int(row['grid_col'])
        if 0 <= r < 300 and 0 <= c < 300:
            grid_matrix[r, c] = row['profitability_index']

    masked_matrix = np.ma.masked_where(grid_matrix == 0, grid_matrix)
    my_cmap = copy.copy(plt.cm.YlOrRd)
    my_cmap.set_bad(color='white')

    plt.figure(figsize=(10, 8))
    vmax_val = np.percentile(data_heatmap['profitability_index'], 99.5)
    plt.imshow(masked_matrix, cmap=my_cmap, interpolation='nearest', aspect='auto', vmax=vmax_val)
    
    for name, (r, c) in landmarks.items():
        plt.plot(c, r, 'bo', markersize=5, markeredgecolor='white', markeredgewidth=0.8)
        plt.text(c + 0.8, r, name, color='darkblue', fontsize=7, weight='bold', 
                 ha='left', va='center',
                 bbox=dict(facecolor='white', alpha=0.6, edgecolor='none', pad=0.2))

    plt.colorbar(label='Índice de Rentabilidade ($/Taxi)')
    plt.title(f'Mapa de Rentabilidade às {target_hour}:00')
    plt.xlabel('Longitude (Grid Col)')
    plt.ylabel('Latitude (Grid Row)')
    plt.xlim(130, 210)
    plt.ylim(200, 130)
    plt.grid(visible=True, color='gray', linestyle=':', linewidth=0.5, alpha=0.3)
    plt.show()

# --- EXECUTAR ---
for h in [4, 8, 16, 22]:
    generate_hour_report(h)
