# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestion de datos de un json "Movie_genre"

# COMMAND ----------

dbutils.widgets.text("p_envionment", "-")
v_environment= dbutils.widgets.get("p_envionment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-16")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - entablecer el schema de los datos del DataFrame

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

schema_genre= StructType(fields=[
    StructField('movieId', IntegerType(), True),
    StructField('genreId', IntegerType(),True)
])

# COMMAND ----------

movie_genre_df= spark.read.json(f"{bronze_folder_path}/{v_file_date}/movie_genre.json",schema=schema_genre)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 - Cambiar el nombre de las columnas y agregar las columnas "ingestion_date"y "environment"

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

movie_genre_final_df= add_ingestion_date(movie_genre_df).withColumnsRenamed({"movieId":"movie_id","genreId":"genre_id"}).withColumns({"envionment":lit(v_environment),"file_date": lit(v_file_date)})
movie_genre_final_df=movie_genre_final_df.select("genre_id","movie_id","ingestion_date","envionment","file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 3 - Guardar el df procesado en el contendor Silver.

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.movie_id = src.movie_id AND tgt.genre_id = src.genre_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(movie_genre_final_df,"movie_silver","movie_genre",condicion_merge,"file_date",["movie_id","genre_id", "file_date"])

# COMMAND ----------

dbutils.notebook.exit("Success")
