#bibliotecas necessarias
!pip install pyspark
!pip install pandas numpy
!pip install matplotlib seaborn plotly
!pip install pyspark pandas matplotlib
!pip install seaborn plotly
!pip install pyspark pandas matplotlib seaborn plotly


#claramente importações 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

# Criação da SparkSession
spark = SparkSession.builder.appName("NBAplayers").getOrCreate()

# dados de jogadores
data = [
    ("LeBron James", "LAL", 1698, 514, 488, 55, 2022),
    ("Stephen Curry", "GSW", 1873, 469, 354, 64, 2022),
    ("Jayson Tatum", "BOS", 2100, 300, 600, 74, 2022),
    ("Luka Doncic", "DAL", 1950, 610, 500, 65, 2022),
    ("Giannis Antetokounmpo", "MIL", 2000, 350, 720, 70, 2022),
    ("Kevin Durant", "PHX", 1600, 400, 450, 47, 2022),
    ("Joel Embiid", "PHI", 2200, 320, 610, 66, 2022),
    ("Ja Morant", "MEM", 1800, 500, 410, 60, 2022),
    ("Jimmy Butler", "MIA", 1500, 370, 420, 55, 2022),
    ("Devin Booker", "PHX", 1700, 390, 380, 58, 2022)
]
# nomes das colunas 
columns = ["jogador", "Time", "pontos", "Assist", "rebotes", "GP", "Temporada"]
# Criação do DataFrame Spark
df = spark.createDataFrame(data, columns)
#calculo de metricas
df = df.withColumn("PPG", round(col("pontos") / col("GP"), 2))
df = df.withColumn("APG", round(col("assist") / col("GP"), 2))
df = df.withColumn("RPG", round(col("rebotes") / col("GP"), 2)) 
df = df.withColumn("EFF", round((col("pontos") + col("rebotes") + col("assist")) / col("GP"), 2))

#Conversão para DataFrame Pandas para visualização com Plotly e Seaborn
df_pd = df.toPandas()

#grafico 1
fig1 = px.bar(df_pd, x="jogador", y="PPG", title="Pontos por Jogo (PPG)", color="jogador",
              labels={"PPG": "Pontos por Jogo", "jogador": "Jogador"})
fig1.show()

#grafico 2
fig2 = px.bar(df_pd, x="jogador", y="EFF", title="Eficiência (EFF)", color="jogador",
              labels={"EFF": "Eficiência", "jogador": "Jogador"})
fig2.show()
#grafico 3
fig3 = px.scatter(df_pd, x="APG", y="RPG", color="jogador", title=" Assistências vs Rebotes",
                  labels={"APG": "Assistências por Jogo (APG)", "RPG": "Rebotes por Jogo (RPG)"})
fig3.show()

#Boxplot para visualizar a distribuição das estatísticas
plt.figure(figsize=(10, 6))
sns.boxplot(data=df_pd[["PPG", "APG", "RPG", "EFF"]])
plt.title(" Distribuição das Estatísticas (PPG, APG, RPG, EFF)")
plt.ylabel("Valores")
plt.show()
