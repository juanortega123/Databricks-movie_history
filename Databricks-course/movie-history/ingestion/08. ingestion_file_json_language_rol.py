# Databricks notebook source
# MAGIC %md
# MAGIC ## hacer la ingesta de datos de un JSON Multilinea. "language_role.json"

# COMMAND ----------

dbutils.widgets.text("p_envionment", "")
v_environment= dbutils.widgets.get("p_envionment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leer el archivo json para pasarlo a un DataFrame

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

language_schema= StructType(fields=[
    StructField("roleId", IntegerType(),False),
    StructField("languageRole", StringType(),True)
])

# COMMAND ----------

language_df= spark.read.option("multiLine", "true").json(f"{bronze_folder_path}/{v_file_date}/language_role.json", schema=language_schema)

# COMMAND ----------

language_df= language_df.withColumnsRenamed({"roleId": "role_id", "languageRole": "language_rol"})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 - añadir columnas de ingestion como "ingestion_date" y "environment"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

language_final_df= add_ingestion_date(language_df).withColumns({"environment":lit(v_environment),"file_date":lit(v_file_date)})

# COMMAND ----------

display(language_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### hacer la escritura del DataFrame en el contenedor Silver
# MAGIC

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.role_id = src.role_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(language_final_df,"movie_silver","language_rol",condicion_merge,"file_date",["role_id", "file_date"])

# COMMAND ----------

dbutils.notebook.exit("Success")
