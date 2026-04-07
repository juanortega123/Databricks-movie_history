# Databricks notebook source
date_test="2024-12-16"

# COMMAND ----------

status = dbutils.notebook.run("01. ingestion_file_movie", 0, {"p_envionment": "developer", "p_file_date": f"{date_test}"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("02. ingestion_file_lenguage",0, {"p_envionment": "developer", "p_file_date": "2024-12-16"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("03. ingestion_file_genre",0, {"p_envionment": "developer", "p_file_date": "2024-12-16"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("04. ingestion_file_json_country",0, {"p_envionment": "developer", "p_file_date": "2024-12-16"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("05. ingestion_file_json_person",0, {"p_envionment": "developer", "p_file_date": f"{date_test}"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("06. ingestion_file_json_movie_genre",0,{"p_envionment": "developer", "p_file_date": f"{date_test}"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("07. ingestion_file_json_movie_cast",0, {"p_envionment": "developer", "p_file_date": f"{date_test}"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("08. ingestion_file_json_language_rol",0, {"p_envionment": "developer", "p_file_date": "2024-12-16"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("09. Ingestion_Folder_production_company",0, {"p_envionment": "developer", "p_file_date": f"{date_test}"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("10. Ingestion_Folder_movie_company",0, {"p_envionment": "developer", "p_file_date": f"{date_test}"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("11. Ingestion_Folder_movie_languages_json",0, {"p_environment": "developer", "p_file_date": f"{date_test}"})
print(status)

# COMMAND ----------

status = dbutils.notebook.run("12. Ingestion_Folder_production_country",0, {"p_envionment": "developer", "p_file_date": f"{date_test}"})
print(status)

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.movies
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.language
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.genre
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.countries
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.person
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.movie_genre
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.movie_cast
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.language_rol
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.production_company
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.movie_company
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.movie_language
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1)
# MAGIC from movie_silver.production_country
# MAGIC group by file_date;
