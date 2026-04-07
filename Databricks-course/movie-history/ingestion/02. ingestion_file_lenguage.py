# Databricks notebook source
# MAGIC %md
# MAGIC ## ingestión del archivo "lenguage.csv"

# COMMAND ----------

dbutils.widgets.text("p_envionment", "")
v_environment= dbutils.widgets.get("p_envionment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-16")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### paso 1 - leer el archivo CSV usando "DatafaframeReader" de spark
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col , current_timestamp , lit

# COMMAND ----------

language_schema = StructType(fields=[
    StructField("languageId", IntegerType(), False),
    StructField("languageCode", StringType(), True),
    StructField("languageName", StringType(), True)
])

# COMMAND ----------

laguage_df = spark.read.csv(f"{bronze_folder_path}/{v_file_date}/language.csv", header =True, schema=language_schema)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Paso 2 - Seleccionaremos solo las columnas "requeridas"
# MAGIC

# COMMAND ----------

language_seleccted = laguage_df.select(col("languageId"), col("languageName"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3 - Cambiar el nombre de las columnas segun lo requerido

# COMMAND ----------

language_renamed = language_seleccted.withColumnsRenamed({"languageId" :"language_id","languageName":"language_name"})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 4 - Agregar la columna "ingestión_date" y "enviroment" al DataFrame

# COMMAND ----------

language_final_df = add_ingestion_date(language_renamed).withColumn("environment",lit(v_environment)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Paso 5 - Vamos a escribir datos en el DataLake en formato "Parquet"

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.language_id = src.language_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(language_final_df,"movie_silver","language",condicion_merge,"file_date",["language_id", "file_date"])

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


