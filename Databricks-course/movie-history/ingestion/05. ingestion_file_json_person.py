# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestion del archivo Json "person.json"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_envionment", "")
v_environment= dbutils.widgets.get("p_envionment")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Paso 1 -Leer el archivo JSON usando "DataFrameReader" de spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

name_schema = StructType([
    StructField("forename", StringType(),True),
    StructField("surname", StringType(),True)
])
person_schema = StructType([
    StructField("personId", IntegerType(),False),
    StructField("personName", name_schema)])

# COMMAND ----------

persons_df = spark.read.json(f"{bronze_folder_path}/{v_file_date}/person.json",schema = person_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 - Renombrar las columnas y añadir nuevas columnas 
# MAGIC 1. "personId"renombrar a "person_id"
# MAGIC 2. Agregar las columnas "ingestion_date" y "environment" 
# MAGIC 3. Agregar la columna "name" a partir de la concatenación de "forename"y "surename"

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

person_with_columns_df = (
    add_ingestion_date(persons_df).withColumnsRenamed({"personId":"person_id"})
    .withColumns({"environment":lit(v_environment), "file_date":lit(v_file_date)})
    .withColumn("name", concat(col("personName.forename"), lit(" "), col("personName.surname")))
)
# eliminar las columnas no requeridas
persons_final_df= person_with_columns_df.drop("personName")

#ordenamos el DataFrame
persons_final_df = persons_final_df.select("person_id", "name", "ingestion_date", "environment","file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Paso 3 - Escribir la salida en un formato "Parquet"

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.person_id = src.person_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(persons_final_df,"movie_silver","person",condicion_merge,"file_date",["person_id", "file_date"])

# COMMAND ----------

dbutils.notebook.exit("Success")
