# ---------------------------------------------------------
# HeatMap com Landmarks
# ---------------------------------------------------------

# 1. Dicionário de Marcos Expandido
# Adicionámos Meatpacking (vida noturna) e Battery Park (pendulares da manhã)
landmarks = {
    "JFK Airport": (186, 190),
    "LaGuardia (LGA)": (155, 174),
    "Midtown": (160, 155),
    "Downtown": (171, 151),
    "Meatpacking": (166, 158), 
    "Battery Park": (181, 153)     
}

try:
    def plot_heatmap_with_landmarks(target_hour):
        # Obter dados
        target_df = final_df_robust
        data = target_df.filter(col("hour") == target_hour) \
                        .select("grid_row", "grid_col", "profitability_index") \
                        .toPandas()

        if data.empty:
            print(f"Sem dados suficientes para a hora {target_hour}.")
            return

        # Criar matriz e máscara
        grid_matrix = np.zeros((300, 300))
        for _, row in data.iterrows():
            r, c = int(row['grid_row']), int(row['grid_col'])
            if 0 <= r < 300 and 0 <= c < 300:
                grid_matrix[r, c] = row['profitability_index']

        masked_matrix = np.ma.masked_where(grid_matrix == 0, grid_matrix)
        my_cmap = copy.copy(plt.cm.YlOrRd)
        my_cmap.set_bad(color='white')

        # Configurar o Plot
        plt.figure(figsize=(12, 10))
        
        # Usamos um percentil alto para focar o contraste nos hotspots
        vmax_val = np.percentile(data['profitability_index'], 99.5)
        plt.imshow(masked_matrix, cmap=my_cmap, interpolation='nearest', aspect='auto',
                   vmax=vmax_val)
        
        cbar = plt.colorbar()
        cbar.set_label('Índice de Rentabilidade ($ / taxi)', fontsize=9)
        
        # --- ADICIONAR OS MARCOS (COM FONTE MENOR) ---
        print(f"A adicionar marcos ao mapa das {target_hour}h...")
        for name, (r, c) in landmarks.items():
            plt.plot(c, r, 'bo', markersize=5, markeredgecolor='white', markeredgewidth=0.8)
            plt.text(c + 0.8, r, name, color='darkblue', fontsize=7, weight='bold', ha='left',va='center')

        # Configurações finais
        plt.title(f'Mapa de Rentabilidade às {target_hour}:00', fontsize=14)
        plt.xlabel('Grid Col (Longitude)', fontsize=10)
        plt.ylabel('Grid Row (Latitude)', fontsize=10)
        # Manter o zoom na área relevante
        plt.xlim(130, 210) 
        plt.ylim(200, 130)
        
        plt.grid(visible=True, color='gray', linestyle=':', linewidth=0.5, alpha=0.3)
        plt.show()

    # --- GERAR OS 4 GRÁFICOS DO CICLO DIÁRIO ---
    plot_heatmap_with_landmarks(8)  # Manhã (Pendulares)
    plot_heatmap_with_landmarks(16) # Tarde (JFK)
    plot_heatmap_with_landmarks(22) # Noite (LGA/Teatros)
    plot_heatmap_with_landmarks(4)  # Madrugada (Vida Noturna)

except Exception as err:
    print(f"Ocorreu um erro: {err}")
