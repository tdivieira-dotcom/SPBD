# ajuste o caminho para o arquivo no seu ambiente/Colab
csv_path = 'taxi_rides_1pc.csv.gz'  

# lê com inferSchema inicialmente para facilitar exploração
df = spark.read.csv(csv_path, header=True, inferSchema=True)

print("Total linhas (raw):", df.count())
df.printSchema()
# mostra algumas linhas
df.show(5, truncate=False)

