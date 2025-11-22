# ---------------------------------------------------------
# ANÁLISE DE RENTABILIDADE COMPLETA (TABELAS + MAPAS)
# ---------------------------------------------------------

# --- 1. CONFIGURAÇÃO DE MARCOS E FUNÇÕES AUXILIARES ---

# Dicionário de Marcos (expandido com base na nossa pesquisa)
landmarks = {
    "JFK Airport": (186, 190),
    "LaGuardia (LGA)": (155, 174),
    "Midtown": (160, 155),
    "Downtown": (171, 151),
    "Meatpacking": (166, 158),
    "Battery Park": (181, 153),
    "Central Park": (154, 158),
    "Brooklyn": (177, 157)
}

# Função Python para identificar a zona mais próxima (para a tabela)
def get_zone_info(row, col):
    # Procura o marco mais próximo (distância Manhattan simples)
    closest_name = "Outros"
    min_dist = 10 # Raio de 10 células (aprox 5km)
    
    for name, (lr, lc) in landmarks.items():
        dist = abs(row - lr) + abs(col - lc)
        if dist < min_dist:
            min_dist = dist
            closest_name = name
            
    # Gera Link Google Maps
    lat = MAX_LAT - (row * LAT_DELTA)
    lon = MIN_LON + (col * LON_DELTA)
    link = f"https://www.google.com/maps/search/?api=1&query={lat:.4f},{lon:.4f}"
    
    return f"{closest_name}|{link}" # Retorna string combinada para separar depois

# Registar a UDF no Spark
zone_info_udf = udf(get_zone_info, StringType())

# --- 2. PROCESSAMENTO DE DADOS (A TUA LÓGICA) ---

try:
    # A. Filtros iniciais (Viagens válidas > 60s)
    df_clean_logic = df_processed.filter((col("trip_time_in_secs") > 60) & (col("total_profit") > 0))

    # B. Calcular Procura e Oferta
    demand_df = df_clean_logic.groupBy("p_row", "p_col", "hour") \
        .agg(avg("total_profit").alias("avg_revenue"), count("*").alias("num_trips"))

    supply_df = df_processed.groupBy("d_row", "d_col", "hour") \
        .agg(count("*").alias("empty_taxis"))

    # C. Join
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

    # D. Filtros Finais e Arredondamento
    final_df_robust = profitability_df.filter(
        (col("empty_taxis") >= 3) & 
        (col("num_trips") >= 3)
    ).withColumn("profitability_index", col("avg_revenue") / col("empty_taxis")) \
     .withColumn("avg_revenue", round(col("avg_revenue"), 2)) \
     .withColumn("profitability_index", round(col("profitability_index"), 2))

    # E. Adicionar Informação da Zona (UDF)
    # Isto adiciona uma coluna "Zone_Info" que depois vamos usar no Pandas
    final_df_enrich = final_df_robust.withColumn("zone_data", zone_info_udf(col("grid_row"), col("grid_col")))
    
    # Cache para performance
    final_df_enrich.cache()

except Exception as err:
    print(f"Erro no processamento: {err}")

# --- 3. FUNÇÃO DE VISUALIZAÇÃO E RELATÓRIO (Loop) ---

def generate_hour_report(target_hour):
    print(f"\n{'='*60}")
    print(f" RELATÓRIO DETALHADO: {target_hour}:00 ")
    print(f"{'='*60}\n")
    
    # 1. TABELA TOP 5
    # Filtramos e convertemos para Pandas para ficar bonito no print
    top_5 = final_df_enrich.filter(col("hour") == target_hour) \
                           .orderBy(col("profitability_index").desc()) \
                           .limit(5) \
                           .toPandas()
    
    if top_5.empty:
        print("Sem dados significativos para esta hora.")
        return

    # Processar a coluna combinada para apresentar melhor
    print(f"{'Zona Aproximada':<25} | {'Lucro ($)':<10} | {'Index':<8} | {'Link Google Maps'}")
    print("-" * 80)
    
    for _, row in top_5.iterrows():
        zone_name, link = row['zone_data'].split('|')
        print(f"{zone_name:<25} | ${row['avg_revenue']:<9} | {row['profitability_index']:<8} | {link}")
    
    print("\n")

    # 2. HEATMAP
    # (O mesmo código do heatmap anterior, ajustado para usar os dados filtrados)
    data_heatmap = final_df_enrich.filter(col("hour") == target_hour).select("grid_row", "grid_col", "profitability_index").toPandas()
    
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
    
    # Marcos
    for name, (r, c) in landmarks.items():
        plt.plot(c, r, 'bo', markersize=5, markeredgecolor='white', markeredgewidth=0.8)
        plt.text(c + 0.8, r, name, color='darkblue', fontsize=7, weight='bold', ha='left', va='center',
                 bbox=dict(facecolor='white', alpha=0.5, edgecolor='none', pad=0.2))

    plt.colorbar(label='Índice de Rentabilidade')
    plt.title(f'Mapa de Rentabilidade às {target_hour}:00')
    plt.xlabel('Longitude (Grid Col)')
    plt.ylabel('Latitude (Grid Row)')
    plt.xlim(130, 210)
    plt.ylim(200, 130)
    plt.grid(visible=True, color='gray', linestyle=':', linewidth=0.5, alpha=0.3)
    plt.show()

# --- 4. EXECUÇÃO DO LOOP ---
hours_to_analyze = [4, 8, 16, 22]

for h in hours_to_analyze:
    generate_hour_report(h)
