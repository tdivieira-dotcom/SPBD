# ---------------------------------------------------------
# ANÁLISE DE RENTABILIDADE
# ---------------------------------------------------------

# 1. Limpar Outliers de Preço na origem (antes de agregar)
# Vamos assumir que viagens > $200 são erros ou exceções irrelevantes para a média
      df_clean_logic = df_processed.filter(
      (col("trip_time_in_secs") > 60) & 
      (col("total_profit") > 0)
	  )

# 2. Calcular a PROCURA (Receita média das viagens que COMEÇAM na área)
# Agrupamos por célula de partida (p_row, p_col) e hora
      demand_df = df_clean_logic.groupBy("p_row", "p_col", "hour") \
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

# 4. Filtrar apenas áreas com atividade real
# Mantemos a exigência de haver pelo menos 3 táxis/viagens para garantir que a área tem movimento      
      final_df_robust = profitability_df.filter(
      (col("empty_taxis") >= 3) & 
      (col("num_trips") >= 3)
      ).withColumn("profitability_index",
      col("avg_revenue") / col("empty_taxis") 
      )
      
      print("Top 5 Áreas Rentáveis:")
      final_df_robust.orderBy(col("profitability_index").desc()).show(5)

except Exception as err:
      print(err)

