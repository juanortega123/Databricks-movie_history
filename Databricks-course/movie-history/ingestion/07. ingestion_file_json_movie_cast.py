# Databricks notebook source
# MAGIC %md
# MAGIC ## Transformar un JSON multiline "movie_cast.json" en un DataFrame.
# MAGIC

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
# MAGIC ### Paso 1 - Leer el archivo json y darle una estructura al schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

mcast_schema= StructType(fields=[ 
StructField("movieId", IntegerType(), True),
StructField("personId", IntegerType(), True),
StructField("characterName", StringType(), True),
StructField("genderId", IntegerType(), True),
StructField("castOrder", IntegerType(), True)
])

# COMMAND ----------

mcast_df = spark.read.option("multiline", "true").json(f"{bronze_folder_path}/{v_file_date}/movie_cast.json", schema=mcast_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 - Renombrar las columnas y añadir nuevas columnas.

# COMMAND ----------

from pyspark.sql.functions import lit, col

# COMMAND ----------

mcast_renamed = add_ingestion_date(mcast_df)\
                .withColumnsRenamed({"movieId":"movie_id","personId":"person_id","characterName":"character_name"})\
                .withColumns({"environment":lit(v_environment),"file_date":lit(v_file_date)})

# COMMAND ----------

mcast_final_df =mcast_renamed.drop(col("genderId"),col("castOrder"))

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.movie_id = src.movie_id AND tgt.person_id = src.person_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(mcast_final_df,"movie_silver","movie_cast",condicion_merge,"file_date",["movie_id","person_id", "file_date"])

# COMMAND ----------

dbutils.notebook.exit("Success")
