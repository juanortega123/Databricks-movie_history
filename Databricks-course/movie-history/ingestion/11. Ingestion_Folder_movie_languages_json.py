# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestion de la carpeta "movie_languages"

# COMMAND ----------

dbutils.widgets.text("p_envionment", "develoment")
v_environment= dbutils.widgets.get("p_envionment")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-12-30")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - Leer los archivos CSV usando "DataFrameReader" de spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType

# COMMAND ----------

mlanguages_schema= StructType(fields=[
    StructField("movieId", IntegerType(),True),
    StructField("languageId", IntegerType(),True),
    StructField("languageRoleId", IntegerType(),True)
])

# COMMAND ----------

mlanguages_df= spark.read.option("multiline", "true").json(f"{bronze_folder_path}/{v_file_date}/movie_language", schema=mlanguages_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 - Renombrar las columnas ya aladir columnas de ingestion.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

mlanguages_final_df= add_ingestion_date(mlanguages_df).withColumnsRenamed({"movieId":"movie_id","languageId":"language_id"}).withColumns({"environment":lit(v_environment),"file_date":lit(v_file_date)}).drop(col("languageRoleId"))

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.movie_id = src.movie_id AND tgt.language_id = src.language_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(mlanguages_final_df,"movie_silver","movie_language",condicion_merge,"file_date",["movie_id","language_id", "file_date"])

# COMMAND ----------

dbutils.notebook.exit("Success")
