"""Задача: Прочитайте данные из CSV файла (например, данные о продажах), используя PySpark.
Вычислите суммарную выручку по каждому продукту за определенный период времени.
Ожидаемый результат: Таблица с двумя столбцами - продукт и суммарная выручка."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

# Создание сессии Spark
spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

# Чтение данных из CSV файла
sales_data = spark.read.csv("sales_data_sample.csv", header=True, inferSchema=True)

# Отображение схемы данных
sales_data.printSchema()

# Вычисление суммарной выручки по каждому продукту
revenue_per_product = sales_data.groupBy(["PRODUCTLINE", "PRODUCTCODE"]).agg(sum(col("QUANTITYORDERED") * col("PRICEEACH")).alias("total_quantity")).orderBy(["PRODUCTLINE", "PRODUCTCODE"])

# Вывод результатов
revenue_per_product.show()

# Остановка сессии Spark
spark.stop()