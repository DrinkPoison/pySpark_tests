from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import matplotlib.pyplot as plt
import pandas as pd

# Создание сессии Spark
spark = SparkSession.builder.appName("TemperatureAnalysis").getOrCreate()

# Чтение данных из CSV файла
data = spark.read.csv("temperature_data.csv", header=True, inferSchema=True)

# Преобразование столбца с датой в формат DateType
data = data.withColumn("Date", to_date(col("date_column")))

# Агрегация данных по дате и вычисление средней температуры для каждого дня
daily_avg_temperature = data.groupBy("Date").avg("temperature").orderBy("Date")

# Сбор данных для построения графика
pandas_df = daily_avg_temperature.select("Date").toPandas()
dates = pandas_df["Date"].tolist()
print(dates)
avg_temperatures_df = daily_avg_temperature.select("avg(temperature)").toPandas()
avg_temperatures = avg_temperatures_df["avg(temperature)"].tolist()


# Построение графика
plt.figure(figsize=(10, 6))
plt.plot(dates, avg_temperatures, marker='o', linestyle='-')
plt.title("Average Daily Temperature")
plt.xlabel("Date")
plt.ylabel("Temperature")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Остановка сессии Spark
spark.stop()