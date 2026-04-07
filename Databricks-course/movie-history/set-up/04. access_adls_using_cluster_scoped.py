# Databricks notebook source
# MAGIC %md
# MAGIC ### Acceder a Azure Data Lake Storage mediante cluster Scoped
# MAGIC 1. Establecer la configuración de spark "fs.azure.account.key" en el cluster
# MAGIC 2. Lstar archivos del contenedor "demo-contenerdor"
# MAGIC 3. Leer datos del archivo "movie.csv" 

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo-contenedor@historialdevideo.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo-contenedor@historialdevideo.dfs.core.windows.net/movie.csv"))

# COMMAND ----------


