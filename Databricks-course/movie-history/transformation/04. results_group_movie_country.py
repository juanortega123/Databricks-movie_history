# Databricks notebook source
# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Realizar los requisitos para los Join y agegaciones para la siguiente estructura.
# MAGIC 1. realizar el llamado de los DataFrame relacionados
# MAGIC 2. Filtrado de los DataFame segun corresponda; "movie_df" >= 2015.
# MAGIC 3. realizár lo Join's.
# MAGIC 4. realizar el groupBy.
# MAGIC 5. realizar el ordenamiento con dense_rank()

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 1 - realizar el llamado de los DataFrame relacionados

# COMMAND ----------

movie_df = spark.read.table("movie_silver.movies")\
                        .filter(f"file_date = '{v_file_date}'")
production_country_df= spark.read.table("movie_silver.production_country")\
                        .filter(f"file_date = '{v_file_date}'")
country_df= spark.read.table("movie_silver.countries")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Filtrado de los DataFame segun corresponda; "movie_df" >= 2015.

# COMMAND ----------

movie_filter_df = movie_df.filter("year_release_date >= 2015")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Realizár lo Join's.

# COMMAND ----------

### Join secundario, para optener el nombre del pais
join_country_df = production_country_df.join(country_df,
                                             production_country_df.country_id == country_df.country_id,"inner") \
                                        .select(production_country_df.movie_id, country_df.country_name)

# COMMAND ----------

### Join primario, para poder optener la relación entre el DataFrame de country_df y movie_df
movie_country_df= movie_filter_df.join(join_country_df,
                                       movie_df.movie_id == join_country_df.movie_id,"inner")

# COMMAND ----------

results_group_movie_country=movie_country_df.select("budget","revenue","year_release_date","country_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - realizar el groupBy.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import sum,desc, dense_rank,lit

# COMMAND ----------

results_df=results_group_movie_country.groupBy("year_release_date", "country_name") \
    .agg(
        sum("budget").alias("total_presupuesto"),
        sum("revenue").alias("total_recaudado")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 5 - Realizar el ordenamiento con dense_rank()

# COMMAND ----------

movie_conuntry_dr= Window.partitionBy("year_release_date").orderBy(desc("total_presupuesto"),desc("total_recaudado"))
final_df=results_df.withColumns({"dense_rank": dense_rank().over(movie_conuntry_dr),"created_date": lit(v_file_date)})

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.year_release_date = src.year_release_date AND tgt.country_name = src.country_name AND tgt.created_date = src.created_date"

# 2. Ejecutamos la función
merge_delta_table(final_df,"movie_gold","results_group_movie_country",condicion_merge,"created_date",["year_release_date","country_name","created_date"])

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from movie_gold.results_group_movie_country;
