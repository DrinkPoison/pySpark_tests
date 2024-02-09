""""Задача: Прочитайте данные из CSV файла, содержащего информацию о продажах.
Используя PySpark, отфильтруйте продажи,оспариваемые товары, по Мотоциклам и Короблям
Ожидаемый результат: Таблица с данными отфильтрованных продаж."""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("test spark Filter").getOrCreate()


spark_data = spark.read.csv('sales_data_sample.csv', header=True, inferSchema=True)


spark_data.printSchema()

filtered_df = spark_data.filter((spark_data['STATUS'] == 'Disputed') &
                                (spark_data['PRODUCTLINE'].isin(['Motorcycles', 'Ships'])))
filtered_df.show()



# Остановка сессии Spark
spark.stop()