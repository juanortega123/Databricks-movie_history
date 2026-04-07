# Databricks notebook source
# MAGIC %md
# MAGIC ##Saprk Aggergation Funtions

# COMMAND ----------

# MAGIC %md
# MAGIC ### funciones simples de agregación.

# COMMAND ----------

from pyspark.sql.functions import sum, count, countDistinct, avg,max,min

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

movie_df = spark.read.parquet(f"{silver_folder_path}/movies")

# COMMAND ----------

movie_df.filter("year_release_date = 2016") \
        .select(sum("budget"),count("movie_id")) \
        .withColumnsRenamed({"sum(budget)": "total_budget", "count(movie_id)": "total_movies"}) \
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group By

# COMMAND ----------

movie_df \
.groupBy("year_release_date") \
    .agg(
        sum("budget").alias("total_budget"),
        avg("budget").alias("avg_budget"),
        max("budget").alias("max_budget"),
        min("budget").alias("min_budget"),
        count("movie_id").alias("total_movies")
    ) \
    .orderBy(movie_df.year_release_date.asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window funcions

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import desc, rank, dense_rank

# COMMAND ----------

movie_rank = Window.partitionBy("year_release_date").orderBy(desc("budget"))
movie_dense_rank = Window.partitionBy("year_release_date").orderBy(desc("budget"))
movie_df.select("title","budget","year_release_date") \
    .filter("year_release_date is not null") \
    .withColumn("rank", rank().over(movie_rank)) \
    .withColumn("dense_rank", dense_rank().over(movie_dense_rank)) \
    .display()

# COMMAND ----------


