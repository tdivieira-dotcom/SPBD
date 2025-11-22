      # Convert pickup cell column into r,c coordinates
      pickups_df = df_clean.select(col("pickup_cell")[0].alias("row"),
                             col("pickup_cell")[1].alias("col"))

      # Count how many trips start in each cell
      pickup_counts = pickups_df.groupBy("row", "col").count()

      # Convert to Pandas for plotting
      pickup_pd = pickup_counts.toPandas()

      plt.figure(figsize=(8,8))
      plt.scatter(pickup_pd['col'], pickup_pd['row'], s=pickup_pd['count']*0.02, alpha=0.5)
      plt.gca().invert_yaxis()  # because cell row increases downward
      plt.title("NY Pickup Locations Mapped to 500m Grid Cells")
      plt.xlabel("Grid Column")
      plt.ylabel("Grid Row")
      plt.show()
