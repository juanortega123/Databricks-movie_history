# Databricks notebook source
# MAGIC %md
# MAGIC ## Spark Filetr Transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

movies_df = spark.read.parquet(f"{silver_folder_path}/movies")

# COMMAND ----------

movies_filtered_df = movies_df.filter((movies_df.year_release_date == 2007)& (movies_df.vote_average > 7))

# COMMAND ----------

display(movies_filtered_df)

# COMMAND ----------


