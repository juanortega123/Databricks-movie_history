# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingestion del archivo "movie.csv"

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
# MAGIC #### Paso 1 - Leeer el archivo CSV usando "DataframeReader" de spark

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

# COMMAND ----------

# DBTITLE 1,Untitled
movie_schema = StructType( fields= [
    StructField("movieId", IntegerType(), False),
    StructField("title",StringType(), True),
    StructField("budget",IntegerType(), True),
    StructField("homePage",StringType(), True),
    StructField("overview",StringType(), True),
    StructField("popularity",DoubleType(), True),
    StructField("yearReleaseDate",IntegerType(), True),
    StructField("releaseDate",DateType(), True),
    StructField("revenue",IntegerType(), True),
    StructField("durationTime",IntegerType(), True),
    StructField("movieStatus",StringType(), True),
    StructField("tagline",StringType(), True),
    StructField("voteAverage",DoubleType(), True),
    StructField("voteCount",IntegerType(), True)
])

# COMMAND ----------

movie_df = spark.read.csv(f"{bronze_folder_path}/{v_file_date}/movie.csv", header=True, schema=movie_schema)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Paso 2 - Seleccionar las comlumnas que vamos a requerir.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

movies_selected_df = movie_df.select(col("movieId"),col("title"),col("budget"),col("popularity"),col("yearReleaseDate"),col("releaseDate"),col("revenue"),col("durationTime"),col("voteAverage"),col("voteCount"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Paso 3 - Cambiar el nombre de las columnas según lo "requerido"

# COMMAND ----------

movies_renamed_df= movies_selected_df \
    .withColumnRenamed("movieId","movie_id") \
    .withColumnRenamed("yearReleaseDate","year_release_date")  \
    .withColumnRenamed("releaseDate","release_date",) \
    .withColumnRenamed("durationTime","duration_time") \
    .withColumnRenamed("voteAverage","vote_average")\
    .withColumnRenamed("voteCount","vote_count")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Agregar la columna "ingestion_date" al DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

movie_final_df = add_ingestion_date(movies_renamed_df) \
                    .withColumns({"environment":lit(v_environment),"file_date":lit(v_file_date)})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 5 - Escribir datos en el datalake en formato "Parquet"

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.movie_id = src.movie_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(movie_final_df,"movie_silver","movies",condicion_merge,"file_date",["movie_id", "file_date"])

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.movies
# MAGIC group by file_date;

# COMMAND ----------

dbutils.notebook.exit("Success")
