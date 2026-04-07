# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestion del archivo "country.json"

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
# MAGIC ### Paso 1 - Leer el archivo JSON usando "DataFrameReader" de spark

# COMMAND ----------

## definimos la structura del schema que usaremos para el DataFrame
countries_schema= "countryId INT, countryIsoCode STRING, countryName STRING"

# COMMAND ----------

## Almacenamos el Json en un DataFrame y tambien le damos la structura que hemos creado previamente
countries_df=spark.read.json(f"{bronze_folder_path}/{v_file_date}/country.json",schema=countries_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 - Eliminar las columnas no deseadas del DataFrame

# COMMAND ----------

## Primera forma de elimiar columnas en un DataFrame  ".drop("countryIsoCode")"
## Segunda forma de eliminar columnas de un DataFrame ".drop(countries_df.countryIsoCode)"
## Tercera forma de eliminar columnas de un DataFrame "drop(countris_df["countryIsoCode"]"
## Cuarta forma de eliminar columnas de un DataFrame * importando from pyspark.sql.funcions import col* 
# ".drop(col("countryIsoCode")"
countries_dropped_df=countries_df.drop("countryIsoCode")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3 - Cambiar el nombre de las columnas y añadir "ingestion_date" y "environment"

# COMMAND ----------

## importamos la función de pyspark.sql.funcions para optener la hora actual y el lit para añadirlo a la columna environment 
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

#con withcolumsrenamed renombramos las columnas que queremos
countries_renamed_df = countries_dropped_df.withColumnsRenamed({"countryId":"country_id", "countryName":"country_name"})
# con withcolum añadimos las columnas que queremos y con add_ingestion_date llamamos a la funcion que estandariza el llamado de la hora actual
countries_final_df = add_ingestion_date(countries_renamed_df).withColumn("environment",lit(v_environment)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Paso 4 - Escribir la salida en un formato "Parquet"

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.country_id = src.country_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(countries_final_df,"movie_silver","countries",condicion_merge,"file_date",["country_id", "file_date"])

# COMMAND ----------

dbutils.notebook.exit("Success")
