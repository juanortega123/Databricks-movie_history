# Databricks notebook source
# MAGIC %md 
# MAGIC ## Acceder al DataFrame mediante SQl

# COMMAND ----------

# MAGIC %md
# MAGIC ### Local Temp View
# MAGIC 1. Crear vista(view) temporal en la Base de Datos
# MAGIC 2. Acceder a la vista(view) desde la celda "SQL"
# MAGIC 3. Acceder a la vista(view) desde la celda"python" 
# MAGIC 4. Acceder a la cista desde otro Notebook

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

results_movie_genre_language = spark.read.parquet(f"{gold_folder_path}/results_movie_genre_laguage")

# COMMAND ----------

results_movie_genre_language.createOrReplaceTempView("v_movie_genre_language")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM v_movie_genre_language
# MAGIC WHERE vote_average > 7.5
# MAGIC
# MAGIC

# COMMAND ----------

p_vote_average = 7.5

# COMMAND ----------

results_movie_genre_language_2 = spark.sql(f"SELECT * FROM v_movie_genre_language where vote_average > {p_vote_average}")

# COMMAND ----------

display(results_movie_genre_language_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Golbat Temp View
# MAGIC 1. Crear vista(view) temporal gloval del DataFrame
# MAGIC 2. Acceder a la vista(view) desde la celda "SQL"
# MAGIC 3. Acceder a la vista(view) desde la celda"python"
# MAGIC 4. Acceder a la cista desde otro "Notebook"

# COMMAND ----------

results_movie_genre_language.createOrReplaceGlobalTempView("gv_movie_genre_language")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from global_temp.gv_movie_genre_language

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_movie_genre_language").display()

# COMMAND ----------


