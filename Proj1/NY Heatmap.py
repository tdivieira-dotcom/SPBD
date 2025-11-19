      # Criar matriz 300x300 inicializada a zero
      heatmap = np.zeros((300, 300))

      # Preencher com contagens
      for _, row in pickup_pd.iterrows():
          r, c = int(row['row']), int(row['col'])
          heatmap[r][c] = row['count']

      # Aplicar escala logarítmica para melhor visualização
      heatmap_log = np.log1p(heatmap)

      plt.figure(figsize=(10, 10))
      plt.imshow(heatmap_log, cmap='hot', interpolation='nearest')
      plt.gca().invert_yaxis()
      plt.title("Heatmap de Densidade de Pickups (Escala Log)")
      plt.colorbar(label="log(nº pickups)")
      plt.xlabel("Grid Column")
      plt.ylabel("Grid Row")
      plt.show()
