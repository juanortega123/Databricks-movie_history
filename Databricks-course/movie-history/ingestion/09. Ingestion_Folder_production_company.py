# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestion de la carpeta "production_company"

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

production_schema= StructType(fields=[
    StructField("CompanyId", IntegerType(),True),
    StructField("companyName", StringType(),True)
])

# COMMAND ----------

production_df= spark.read.csv(f"{bronze_folder_path}/{v_file_date}/production_company", schema=production_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 - Renombrar las columnas ya aladir columnas de ingestion.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

productions_final_df= add_ingestion_date(production_df).withColumnsRenamed({"CompanyId":"company_id","companyName":"company_name"}).withColumns({"environment":lit(v_environment),"file_date":lit(v_file_date)})

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.company_id = src.company_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(productions_final_df,"movie_silver","production_company",condicion_merge,"file_date",["company_id", "file_date"])

# COMMAND ----------

dbutils.notebook.exit("Success")
