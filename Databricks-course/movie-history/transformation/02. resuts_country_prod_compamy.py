# Databricks notebook source
# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Realizár un Join entre DataFames con relación.
# MAGIC 1. leer los parquet desde el Contenedor Silver. 
# MAGIC 2. Transformar la Data (aplicar filtros, Joins, OrdeBy,etc).
# MAGIC 3. Escribir la transformación final en el Contendor Gold.

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### llamar al notebook que almacena las varaibles de los contenedore

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 1 - Llamar a los DataFrame que usaremos y se encuentran en el Contenedor Silver.
# MAGIC

# COMMAND ----------

##df principal.
movie_df = spark.read.table("movie_silver.movies")\
                        .filter(f"file_date = '{v_file_date}'")
## df's del pais donde fue hecha la pelicula.
production_country_df = spark.read.table("movie_silver.production_country")\
                                    .filter(f"file_date = '{v_file_date}'")
country_df= spark.read.table("movie_silver.countries")
#df's con relación a la productora que realizó la gravación.
movie_company_df = spark.read.table("movie_silver.movie_company")\
                                .filter(f"file_date = '{v_file_date}'")
production_company_df = spark.read.table("movie_silver.production_company")\
                                    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

movies_filter_df = movie_df.filter("year_release_date >= 2010")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Realizar los trataiento de datos respectivos antes de realizár el Join de los DF.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 3 - Realizamos los Join secundarios primero.

# COMMAND ----------

# join entre "production_country_df" y "country_df" por medio d ela comuna "country_id"
join_production_country = country_df.join(production_country_df,
                                                       production_country_df.country_id == country_df.country_id,"inner")\
                                                    .select(production_country_df.movie_id , country_df.country_id, country_df.country_name)

# COMMAND ----------

display(join_production_country)

# COMMAND ----------

# join entre "movie_company_df" y "production_company_df" por medio de la comuna "company_id"
join_movie_company= production_company_df.join(movie_company_df
                                          , movie_company_df.company_id == production_company_df.company_id,"inner")\
                                              .select(movie_company_df.movie_id,movie_company_df.company_id,production_company_df.company_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Realizár el Join Principal a partir de los 2 joins secundarios y el movie_df

# COMMAND ----------

results_movie_country_company= movies_filter_df.join(join_movie_company, 
                                movies_filter_df.movie_id == join_movie_company.movie_id,"inner") \
                                                .join(join_production_country,
                                movies_filter_df.movie_id == join_production_country.movie_id, "inner")

# COMMAND ----------

resuts_country_prod_compamy=results_movie_country_company \
    .select(movies_filter_df.movie_id,"company_id","country_id","title","budget","revenue","duration_time","release_date","country_name","company_name") \
    .withColumn("created_date", lit(v_file_date)) \
    .orderBy(results_movie_country_company.title.asc())

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.movie_id = src.movie_id AND tgt.company_id = src.company_id AND tgt.country_id = src.country_id AND tgt.created_date = src.created_date"

# 2. Ejecutamos la función
merge_delta_table(resuts_country_prod_compamy,"movie_gold","resuts_country_prod_compamy",condicion_merge,"created_date",["movie_id","company_id","country_id", "created_date"])

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_gold.resuts_country_prod_compamy;

# COMMAND ----------

#display(spark.read.parquet(f"{gold_folder_path}/resuts_country_prod_compamy"))
