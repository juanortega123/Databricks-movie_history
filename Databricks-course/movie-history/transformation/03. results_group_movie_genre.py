# Databricks notebook source
# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Realizara agregaciones y joins con la finalidad de tener estos resultados. 
# MAGIC 1. Se requiere tener la información sobre el "Total del presupuesto" y "Total de ingresos" de las "peliculas"
# MAGIC 2. realizar ul filtrado de datos desde "year_release_date" >= 2015
# MAGIC 3. realizar agrupaciones por "Año de lanzamineto" y el "genero" al qe pertenece cada pelicula.
# MAGIC 4. Realizár un ranking ordenado de manera ASC() por el "total del presupuesto" y "total de ingresos" particiondos por "Año de la fecha de lanzamiento"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

movie_df =spark.read.table("movie_silver.movies")\
                    .filter(f"file_date = '{v_file_date}'")
movie_genre_df=spark.read.table("movie_silver.movie_genre")\
                    .filter(f"file_date = '{v_file_date}'")
genre_df=spark.read.table("movie_silver.genre")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 2 - Realizar un filtrado de datos del DataFame principal con la finalidad de trabajar solo con datos mayores o iguales a 2015
# MAGIC
# MAGIC

# COMMAND ----------

movie_filter_df = movie_df.filter("year_release_date >=2015")

# COMMAND ----------

join_genre = genre_df.join(movie_genre_df, 
                                 movie_genre_df.genre_id == genre_df.genre_id,"inner") \
                            .select(movie_genre_df.movie_id, genre_df.genre_name)

# COMMAND ----------

results_movie_genre= movie_filter_df.join(join_genre, movie_filter_df.movie_id == join_genre.movie_id,"inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ### realizar agrupaciones por "Año de lanzamineto" y el "genero"

# COMMAND ----------

from pyspark.sql.functions import sum , dense_rank, desc, asc, lit
from pyspark.sql import Window

# COMMAND ----------

results_group_df = results_movie_genre.groupBy("year_release_date", "genre_name") \
    .agg(
        sum("budget").alias("total_presupuesto"),
        sum("revenue").alias("total_recaudado")
    )

# COMMAND ----------

results_dense_rank = Window.partitionBy("year_release_date") \
.orderBy (desc("total_presupuesto"),desc("total_recaudado"))
final_df = results_group_df.withColumns({"dense_rank": dense_rank().over(results_dense_rank),"created_date": lit(v_file_date)}) 

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.year_release_date = src.year_release_date AND tgt.genre_name = src.genre_name AND tgt.created_date = src.created_date"

# 2. Ejecutamos la función
merge_delta_table(final_df,"movie_gold","results_group_movie_genre",condicion_merge,"created_date",["year_release_date","genre_name","created_date"])

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from movie_gold.results_group_movie_genre;

# COMMAND ----------

#display(spark.read.parquet(f"{gold_folder_path}/results_group_movie_genre"))
