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
