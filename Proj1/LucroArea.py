# @title
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import numpy as np
import copy

spark = SparkSession.builder.master('local[*]') \
						.appName('taxis').getOrCreate()

# Limits of the NY area grid
MIN_LON = -74.916578
MAX_LAT = 41.47718278

# Cell size (500m)
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

# ------------------------
# CÓDIGO PRINCIPAL
# ------------------------ 

try :
      # Registrar UDF
      udf_latlon = udf(latlon_to_grid_py, ArrayType(IntegerType()))

      # Carregar dados
      df = spark.read.csv('taxi_rides_1pc.csv.gz', sep =',', header=True, inferSchema=True)

      # Criar células, convertendo coordenadas em indices de grid
      df = df.withColumn('pickup_cell', udf_latlon(col('pickup_latitude'), col('pickup_longitude'))) \
             .withColumn('dropoff_cell', udf_latlon(col('dropoff_latitude'), col('dropoff_longitude')))

      # Filtrar apenas células válidas dentro do grid (sem as fora da grid e as nulas)
      df_clean = df.filter(
      (col('pickup_cell').isNotNull()) & (col('dropoff_cell').isNotNull()) &
      (col('pickup_cell').getItem(0) > 0) & (col('pickup_cell').getItem(0) < 300) &
      (col('pickup_cell').getItem(1) > 0) & (col('pickup_cell').getItem(1) < 300) &
      (col('dropoff_cell').getItem(0) > 0) & (col('dropoff_cell').getItem(0) < 300) &
      (col('dropoff_cell').getItem(1) > 0) & (col('dropoff_cell').getItem(1) < 300)
                )
# ---------------------------------------------------------
# CONTINUAÇÃO: ANÁLISE DE RENTABILIDADE
# ---------------------------------------------------------

# 1. Preparação dos Dados (Extrair Linha/Coluna e Hora)
# Como o teu UDF retorna um array [row, col], vamos extraí-los para colunas próprias
      df_processed = df_clean.withColumn("p_row", col("pickup_cell")[0]) \
                       .withColumn("p_col", col("pickup_cell")[1]) \
                       .withColumn("d_row", col("dropoff_cell")[0]) \
                       .withColumn("d_col", col("dropoff_cell")[1]) \
                       .withColumn("hour", hour("pickup_datetime")) \
                       .withColumn("total_profit", col("fare_amount") + col("tip_amount"))

# 2. Calcular a PROCURA (Receita média das viagens que COMEÇAM na área)
# Agrupamos por célula de partida (p_row, p_col) e hora
      demand_df = df_processed.groupBy("p_row", "p_col", "hour") \
      .agg(avg("total_profit").alias("avg_revenue"))

# --- MELHORIA DA ANÁLISE ---

# 1. Limpar Outliers de Preço na origem (antes de agregar)
# Vamos assumir que viagens > $200 são erros ou exceções irrelevantes para a média
      df_clean_price = df_processed.filter(col("total_profit") < 200)

# 2. Recalcular Procura e Oferta com os dados limpos
      demand_df = df_clean_price.groupBy("p_row", "p_col", "hour") \
      .agg(avg("total_profit").alias("avg_revenue"), count("*").alias("num_trips"))

      supply_df = df_processed.groupBy("d_row", "d_col", "hour") \
      .agg(count("*").alias("empty_taxis"))

# 3. Join e Filtro de Significância
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

# 4. O TRUQUE: Filtrar apenas áreas com atividade real
# Como estás a usar a amostra de 1%, números baixos são normais, mas
# vamos exigir pelo menos 3 viagens e 3 táxis para considerar "padrão".
      final_df_robust = profitability_df.filter(
      (col("empty_taxis") >= 3) & 
      (col("num_trips") >= 3)
      ).withColumn("profitability_index",
      col("avg_revenue") / col("empty_taxis") # Já não precisamos do +1 porque filtramos zeros
      )
      
      print("Top 5 Áreas CONSISTENTEMENTE Rentáveis:")
      final_df_robust.orderBy(col("profitability_index").desc()).show(5)

except Exception as err:
      print(err)
# ---------------------------------------------------------
# VISUALIZAÇÃO: MAPA DE CALOR (HEATMAP)
# ---------------------------------------------------------

try:
    def plot_heatmap(target_hour):
        # IMPORTANTE: Estamos a usar 'final_df_robust' para ver os dados limpos e significativos.
        # Se não tiveres corrido o bloco de "Melhoria", o código falhará aqui.
        target_df = final_df_robust

        # Filtrar dados para a hora desejada e converter para Pandas
        # Nota: .toPandas() é seguro aqui porque o dataset é pequeno (agregado 300x300)
        data = target_df.filter(col("hour") == target_hour) \
                        .select("grid_row", "grid_col", "profitability_index") \
                        .toPandas()

        if data.empty:
            print(f"Sem dados suficientes para a hora {target_hour} (após filtragem).")
            return

        # Criar matriz 300x300 preenchida com zeros
        grid_matrix = np.zeros((300, 300))

        # Preencher a matriz com os índices de rentabilidade
        for _, row in data.iterrows():
            r, c = int(row['grid_row']), int(row['grid_col'])
            # Verificação extra de limites
            if 0 <= r < 300 and 0 <= c < 300:
                grid_matrix[r, c] = row['profitability_index']

        # 3. O TRUQUE DO FUNDO BRANCO
        # Mascaramos os valores que são exatamente 0 (sem dados)
        masked_matrix = np.ma.masked_where(grid_matrix == 0, grid_matrix)
        
        # Escolhemos um colormap (YlOrRd é ótimo para fundo branco: Amarelo -> Vermelho)
        # Podes trocar 'YlOrRd' por 'inferno', 'plasma', 'jet', etc.
        my_cmap = copy.copy(plt.cm.YlOrRd) 
        my_cmap.set_bad(color='white') # Define que os dados mascarados (zeros) são brancos

        # Plotar
        plt.figure(figsize=(10, 8))
        
        # Usamos 'vmax' com percentil 99 para ignorar outliers extremos na coloração
        # Mudei o cmap para 'inferno' ou 'viridis' que geralmente têm melhor contraste que 'hot'
        plt.imshow(masked_matrix, cmap=my_cmap, interpolation='nearest', aspect='auto',
                   vmax=np.percentile(data['profitability_index'], 99))
        
        plt.colorbar(label='Índice de Rentabilidade ($ / taxi)')
        plt.title(f'Mapa de Rentabilidade às {target_hour}:00')
        plt.xlabel('Grid Col (Longitude)')
        plt.ylabel('Grid Row (Latitude)')
        plt.gca().invert_yaxis() # Inverter Y para alinhar com mapas geográficos
        plt.show()

    # Exemplo: Plotar a hora 16 (que foi o teu Top 1 na tabela anterior)
    print("A gerar mapa para as 16h...")
    plot_heatmap(16)

    # Exemplo: Plotar a hora 19
    print("A gerar mapa para as 19h...")
    plot_heatmap(19)

except Exception as err:
    print(f"Ocorreu um erro: {err}")
