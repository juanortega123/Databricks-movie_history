# Databricks notebook source
# MAGIC %md
# MAGIC ## Ejercicio de Join.

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - Llamar a el notebook que renombra los contenedores del DataLake

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

## leemos el archivo parquet de movie.
movie_df = spark.read.table("movie_silver.movies")\
                            .filter(f"file_date = '{v_file_date}'")  

# COMMAND ----------

## leemos los archivos parquet de languages.
language_df = spark.read.table("movie_silver.language")
movie_language_df = spark.read.table("movie_silver.movie_language")\
                            .filter(f"file_date = '{v_file_date}'")  

# COMMAND ----------

## leemos los archivos parquet de genre.
genre_df = spark.read.table("movie_silver.genre")
movie_genre_df = spark.read.table("movie_silver.movie_genre")\
                            .filter(f"file_date = '{v_file_date}'")  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora despues de llamar a los DataFrames, haremos un filtrado de datos segun nos lo indican.
# MAGIC -- optener infromación del año 2000 en adelante y ordenarlo de manera Descendente. Se debe considerar su respectivo "Genero" y e "Idioma" que se encuntra en dicha pelicula. 
# MAGIC 1. De la pelicula se requiere mostrar el "Titulo, el tiempo de duracíon, la fecha de lanzamiento y el voto promedio".
# MAGIC 2. De idioma se requiere mostrar el " Nombre del idioma"
# MAGIC 3. Del gerero mostrar el "nombre de genero". 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizar el Join de los primeros DataFrame, "Languages"

# COMMAND ----------

join_language= language_df.join(movie_language_df
                                      , movie_language_df.language_id == language_df.language_id, "inner")\
                .select(movie_language_df.movie_id,language_df.language_id, language_df.language_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizar el Join de la columna de los "Genre"
# MAGIC

# COMMAND ----------

join_genre= genre_df.join(movie_genre_df,
                          genre_df.genre_id == movie_genre_df.genre_id, "inner")\
            .select(movie_genre_df.movie_id,genre_df.genre_id, genre_df.genre_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizar el filtro y el ordenamiento del primer DataFrame

# COMMAND ----------

movie_filter_df = movie_df.filter("year_release_date >= 2000")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizar el Join completo con los 2 joins previamente realizados "join_language", "join_genre" y colocando del lado Izquierdo al DF "movie_filter_df".

# COMMAND ----------

results_movie_genre_languages = movie_filter_df.join(join_language,
                                movie_filter_df.movie_id == join_language.movie_id, "inner") \
                                                .join(join_genre,
                                movie_filter_df.movie_id == join_genre.movie_id, "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Seleccionamos los valores que nos interesan de esa combinación de DataFrames, y adicionamos una columna ("created_date")

# COMMAND ----------

result_df = results_movie_genre_languages \
    .select(movie_filter_df.movie_id,"language_id","genre_id","title","duration_time","release_date","vote_average","language_name","genre_name") \
    .withColumn("created_date", lit(v_file_date))

# COMMAND ----------

results_movie_genre_languages=result_df.orderBy(result_df.release_date.desc())

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.movie_id = src.movie_id AND tgt.language_id = src.language_id AND tgt.genre_id = src.genre_id AND tgt.created_date = src.created_date"

# 2. Ejecutamos la función
merge_delta_table(results_movie_genre_languages,"movie_gold","results_movie_genre_laguage",condicion_merge,"created_date",["movie_id","language_id","genre_id", "created_date"])

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_gold.results_movie_genre_laguage;
