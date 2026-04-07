# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestion de la carpeta "production_country"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_envionment", "")
v_environment= dbutils.widgets.get("p_envionment")

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

pcountry_schema= StructType(fields=[
    StructField("movieId", IntegerType(),True),
    StructField("countryId", IntegerType(),True)
])

# COMMAND ----------

pcountry_df= spark.read.option("multiLine", "true").json(f"{bronze_folder_path}/{v_file_date}/production_country", schema=pcountry_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 - Renombrar las columnas ya aladir columnas de ingestion.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pcountry_final_df= add_ingestion_date(pcountry_df).withColumnsRenamed({"movieId":"movie_id","countryId":"country_id"}).withColumns({"environment":lit(v_environment),"file_date":lit(v_file_date)})

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.movie_id = src.movie_id AND tgt.country_id = src.country_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(pcountry_final_df,"movie_silver","production_country",condicion_merge,"file_date",["movie_id","country_id", "file_date"])

# COMMAND ----------

dbutils.notebook.exit("Success")
