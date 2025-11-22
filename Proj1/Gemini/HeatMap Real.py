# ---------------------------------------------------------
# HEATMAP COM MARCOS GEOGRÁFICOS
# ---------------------------------------------------------

# Definir marcos importantes (Linha, Coluna) baseados na nossa "Pedra de Roseta"
landmarks = {
    "JFK Airport": (186, 190),
    "LaGuardia (LGA)": (155, 174),
    "Midtown/Times Sq": (160, 155),
    "Downtown/Wall St": (171, 151)
}

try:
    def plot_heatmap_with_landmarks(target_hour):
        # 1. Obter dados limpos
        target_df = final_df_robust
        
        data = target_df.filter(col("hour") == target_hour) \
                        .select("grid_row", "grid_col", "profitability_index") \
                        .toPandas()

        if data.empty:
            print(f"Sem dados suficientes para a hora {target_hour}.")
            return

        # 2. Criar matriz e preencher
        grid_matrix = np.zeros((300, 300))
        for _, row in data.iterrows():
            r, c = int(row['grid_row']), int(row['grid_col'])
            if 0 <= r < 300 and 0 <= c < 300:
                grid_matrix[r, c] = row['profitability_index']

        # 3. Máscara para fundo branco
        masked_matrix = np.ma.masked_where(grid_matrix == 0, grid_matrix)
        my_cmap = copy.copy(plt.cm.YlOrRd) # Podes mudar para 'inferno' ou 'jet' se preferires
        my_cmap.set_bad(color='white')

        # 4. Configurar o Plot
        plt.figure(figsize=(12, 10)) # Aumentei ligeiramente o tamanho
        
        # Plotar o Heatmap
        # Usamos percentil 99.5 para focar mesmo só nos hotspots mais fortes
        plt.imshow(masked_matrix, cmap=my_cmap, interpolation='nearest', aspect='auto',
                   vmax=np.percentile(data['profitability_index'], 99.5))
        
        plt.colorbar(label='Índice de Rentabilidade ($ / taxi)')
        
        # --- ADICIONAR OS MARCOS (LANDMARKS) ---
        print("A adicionar marcos geográficos...")
        for name, (r, c) in landmarks.items():
            # Nota: No plot(x,y), X é a Coluna, Y é a Linha
            # Desenhamos um ponto azul ('bo') com contorno branco
            plt.plot(c, r, 'bo', markersize=8, markeredgecolor='white', markeredgewidth=1.5)
            # Adicionamos o texto ligeiramente deslocado (c+3, r+3) para não tapar o ponto
            plt.text(c + 3, r + 3, name, color='darkblue', fontsize=10, weight='bold')

        # Configurações finais dos eixos
        plt.title(f'Mapa de Rentabilidade às {target_hour}:00 (com Marcos)')
        plt.xlabel('Grid Col (Longitude)')
        plt.ylabel('Grid Row (Latitude)')
        # Forçar a visualização da área principal de NYC (podes ajustar estes números se quiseres mais zoom)
        plt.xlim(130, 210) 
        plt.ylim(200, 130) # Invertido: valor maior em baixo, menor em cima
        
        plt.grid(visible=True, color='gray', linestyle='--', linewidth=0.3, alpha=0.5)
        plt.show()

    # --- GERAR OS GRÁFICOS FINAIS ---
    
    # Hora 16 (Pico da tarde - Esperamos ver Aeroportos)
    print("=== A gerar mapa para as 16h ===")
    plot_heatmap_with_landmarks(16)
    
    # Hora 22 (Noite - Esperamos ver vida noturna/centro)
    print("\n=== A gerar mapa para as 22h ===")
    plot_heatmap_with_landmarks(22)

except Exception as err:
    print(f"Ocorreu um erro: {err}")
