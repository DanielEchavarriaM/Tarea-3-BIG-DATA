# tarea3.py
# Procesamiento batch del precio histórico del oro con PySpark
# Autor: Daniel Echavarría Montaña

from pyspark.sql import SparkSession, functions as F

# 1. Crear la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# 2. Ruta del dataset en HDFS
file_path = "hdfs://localhost:9000/Tarea3/monthly.csv"

# 3. Cargar el conjunto de datos
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

print("Esquema inicial del dataset")
df.printSchema()

print("### Primeras filas ###")
df.show(5)

# 4. Limpieza y transformación
# Renombrar columnas para evitar mayúsculas o caracteres especiales
df = df.withColumnRenamed("Date", "fecha") \
       .withColumnRenamed("Price", "precio_usd")

# 5. Agregar columnas auxiliares
df = df.withColumn("anio", F.year("fecha"))
df = df.withColumn("mes", F.month("fecha"))

# 6. Análisis exploratorio de datos (EDA)

print("Estadísticas generales del precio del oro")
df.describe("precio_usd").show()

print("Promedio histórico del precio del oro")
df.select(F.round(F.avg("precio_usd"), 2).alias("Promedio_USD")).show()

# 7. Análisis temporal: promedio y variación por año
promedio_anual = df.groupBy("anio") \
    .agg(F.round(F.avg("precio_usd"), 2).alias("promedio_anual_usd")) \
    .orderBy("anio")
print(promedio_anual)

# 8. Consultas útiles para un usuario
print("Año con el precio promedio más alto")
promedio_anual.orderBy(F.desc("promedio_anual_usd")).show(1)

print("Años donde el precio promedio fue superior a 1000 USD")
promedio_anual.filter(F.col("promedio_anual_usd") > 1000).show(10)
