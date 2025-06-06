!pip install pyspark
!pip install pandas numpy
!pip install matplotlib seaborn plotly
!pip install pyspark pandas matplotlib
!pip install seaborn plotly
!pip install pyspark pandas matplotlib seaborn plotly



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns


spark = SparkSession.builder.appName("NBAplayers").getOrCreate()


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

columns = ["jogador", "Time", "pontos", "Assist", "rebotes", "GP", "Temporada"]

df = spark.createDataFrame(data, columns)

df = df.withColumn("PPG", round(col("pontos") / col("GP"), 2))
df = df.withColumn("APG", round(col("assist") / col("GP"), 2))
df = df.withColumn("RPG", round(col("rebotes") / col("GP"), 2)) 
df = df.withColumn("EFF", round((col("pontos") + col("rebotes") + col("assist")) / col("GP"), 2))


df_pd = df.toPandas()


fig1 = px.bar(df_pd, x="jogador", y="PPG", title="Pontos por Jogo (PPG)", color="jogador",
              labels={"PPG": "Pontos por Jogo", "jogador": "Jogador"})
fig1.show()


fig2 = px.bar(df_pd, x="jogador", y="EFF", title="Eficiência (EFF)", color="jogador",
              labels={"EFF": "Eficiência", "jogador": "Jogador"})
fig2.show()

fig3 = px.scatter(df_pd, x="APG", y="RPG", color="jogador", title=" Assistências vs Rebotes",
                  labels={"APG": "Assistências por Jogo (APG)", "RPG": "Rebotes por Jogo (RPG)"})
fig3.show()

plt.figure(figsize=(10, 6))
sns.boxplot(data=df_pd[["PPG", "APG", "RPG", "EFF"]])
plt.title(" Distribuição das Estatísticas (PPG, APG, RPG, EFF)")
plt.ylabel("Valores")
plt.show()
