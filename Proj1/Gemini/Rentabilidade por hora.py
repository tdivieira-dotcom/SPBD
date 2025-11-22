# ---------------------------------------------------------
# VISÃO GERAL: RENTABILIDADE MÉDIA POR HORA DO DIA
# ---------------------------------------------------------

# Agrupar todos os dados por hora para ver a tendência geral da cidade
hourly_trend = final_df_robust.groupBy("hour") \
    .agg(avg("profitability_index").alias("avg_profitability")) \
    .orderBy("hour") \
    .toPandas()

plt.figure(figsize=(12, 6))

# Desenhar a linha
plt.plot(hourly_trend['hour'], hourly_trend['avg_profitability'], 
         marker='o', linestyle='-', color='royalblue', linewidth=2)

# Destaques visuais
plt.title('Evolução da Rentabilidade Média ao Longo do Dia', fontsize=14)
plt.xlabel('Hora do Dia (0-23)', fontsize=12)
plt.ylabel('Índice Médio de Rentabilidade', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.xticks(range(0, 24)) # Mostrar todas as horas no eixo X

# Marcar as zonas de interesse
plt.axvspan(7, 9, color='orange', alpha=0.2, label='Manhã (Rush)')
plt.axvspan(16, 19, color='green', alpha=0.2, label='Tarde (Aeroportos)')
plt.legend()

plt.show()
