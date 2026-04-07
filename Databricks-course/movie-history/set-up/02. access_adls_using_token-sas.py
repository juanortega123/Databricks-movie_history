# Databricks notebook source
# MAGIC %md
# MAGIC ### Acceder a Azure Data Lake Storage mediante Access Key
# MAGIC 1. Establecer la configuración de spark "SAS Token"
# MAGIC 2. Lstar archivos del contenedor "demo-contenedor"
# MAGIC 3. Leer datos del archivo "movie.csv" 

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.moviehistorytesting.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.moviehistorytesting.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.moviehistorytesting.dfs.core.windows.net", 
               "sp=rl&st=2026-03-18T20:41:56Z&se=2026-03-19T04:56:56Z&spr=https&sv=2024-11-04&sr=c&sig=4YjXuDWGphiK4UZA6%2FijxuMxQtJWU67FuIzdtU1J0zU%3D")

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistorytesteo.dfs.core.windows.net/movie.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### optención de datos del DATA LAKE con un secret scope y el SAS token sin exponer las claves.

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("movie-hisory-scope")

# COMMAND ----------

key_sas =dbutils.secrets.get( scope="movie-hisory-scope",key="movie-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.moviehistorytesting.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.moviehistorytesting.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.moviehistorytesting.dfs.core.windows.net", 
               key_sas)

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistorytesteo.dfs.core.windows.net/movie.csv"))

