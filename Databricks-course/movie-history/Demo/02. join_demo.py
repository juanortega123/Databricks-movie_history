# Databricks notebook source
# MAGIC %md
# MAGIC ## Spark join transfortation

# COMMAND ----------

# MAGIC %md
# MAGIC ### INNER JOIN
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

movie_df= spark.read.parquet(f"{silver_folder_path}/movies").filter("year_release_date = 2007")

# COMMAND ----------

production_country = spark.read.parquet(f"{silver_folder_path}/production_country")

# COMMAND ----------

countries_df= spark.read.parquet(f"{silver_folder_path}/countries")

# COMMAND ----------

display(movie_df)

# COMMAND ----------

display(production_country)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

movie_production_country=movie_df.join(production_country,
                                        movie_df.movie_id == production_country.movie_id,
                                        "inner") \
                                        .select(movie_df.title, movie_df.budget, production_country.country_id)

# COMMAND ----------

movie_country_df = movie_production_country.join(countries_df,
                                                 movie_production_country.country_id == countries_df.country_id,"inner")\
                    .select(movie_production_country.title, movie_production_country.budget,countries_df.country_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outer Join

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left outer join

# COMMAND ----------

movie_production_country=movie_df.join(production_country,
                                        movie_df.movie_id == production_country.movie_id,
                                        "left") \
                                        .select(movie_df.title, movie_df.budget, production_country.country_id)

# COMMAND ----------

movie_country_df = movie_production_country.join(countries_df,
                                                 movie_production_country.country_id == countries_df.country_id,"left")\
                    .select(movie_production_country.title, movie_production_country.budget,countries_df.country_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Right Outer Join

# COMMAND ----------

movie_production_country=movie_df.join(production_country,
                                        movie_df.movie_id == production_country.movie_id,
                                        "right") \
                                        .select(movie_df.title, movie_df.budget, production_country.country_id)

# COMMAND ----------

movie_country_df = movie_production_country.join(countries_df,
                                                 movie_production_country.country_id == countries_df.country_id,"right")\
                    .select(movie_production_country.title, movie_production_country.budget,countries_df.country_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Outer Join

# COMMAND ----------

movie_production_country=movie_df.join(production_country,
                                        movie_df.movie_id == production_country.movie_id,
                                        "full") \
                                        .select(movie_df.title, movie_df.budget, production_country.country_id)

# COMMAND ----------

movie_country_df = movie_production_country.join(countries_df,
                                                 movie_production_country.country_id == countries_df.country_id,"full")\
                    .select(movie_production_country.title, movie_production_country.budget,countries_df.country_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi join

# COMMAND ----------

movie_production_country=movie_df.join(production_country,
                                        movie_df.movie_id == production_country.movie_id,
                                        "left") \
                                        .select(movie_df.title, movie_df.budget, production_country.country_id)

# COMMAND ----------

movie_country_df = movie_production_country.join(countries_df,
                                                 movie_production_country.country_id == countries_df.country_id,"semi")\
                    .select(movie_production_country.title, movie_production_country.budget)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti Join

# COMMAND ----------

movie_production_country=movie_df.join(production_country,
                                        movie_df.movie_id == production_country.movie_id,
                                        "left") \
                                        .select(movie_df.title, movie_df.budget, production_country.country_id)

# COMMAND ----------

movie_country_df = movie_production_country.join(countries_df,
                                                 movie_production_country.country_id == countries_df.country_id,"anti")\
                    .select(movie_production_country.title, movie_production_country.budget)

# COMMAND ----------

# MAGIC %md
# MAGIC ## cross join

# COMMAND ----------

movie_country_df = movie_df.crossJoin(countries_df)
display(movie_country_df.count())

# COMMAND ----------

display(movie_country_df)

# COMMAND ----------


