# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestion de la carpeta "movie_company"

# COMMAND ----------

dbutils.widgets.text("p_envionment", "")
v_environment= dbutils.widgets.get("p_envionment")

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - Leer los archivos CSV usando "DataFrameReader" de spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType

# COMMAND ----------

mcompany_schema= StructType(fields=[
    StructField("movieId", IntegerType(),True),
    StructField("companyId", IntegerType(),True)
])

# COMMAND ----------

mcompany_df= spark.read.csv(f"{bronze_folder_path}/{v_file_date}/movie_company", schema=mcompany_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 - Renombrar las columnas ya aladir columnas de ingestion.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

mcompany_final_df= add_ingestion_date(mcompany_df).withColumnsRenamed({"movieId":"movie_id","companyId":"company_id"}).withColumns({"environment":lit(v_environment),"file_date":lit(v_file_date)})

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.movie_id = src.movie_id AND tgt.company_id = src.company_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(mcompany_final_df,"movie_silver","movie_company",condicion_merge,"file_date",["movie_id","company_id", "file_date"])

# COMMAND ----------

#display(spark.read.parquet(f"{silver_folder_path}/movie_company"))

# COMMAND ----------

dbutils.notebook.exit("Success")
