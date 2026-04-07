# Databricks notebook source
# MAGIC %md
# MAGIC ### Acceder a Azure Data Lake Storage mediante Access Key
# MAGIC 1. Establecer la configuración de spark "fs.azure.account.key"
# MAGIC 2. Lstar archivos del contenedor "demo"
# MAGIC 3. Leer datos del archivo "movie.csv" 

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.moviehistorytesteo.dfs.core.windows.net",
    ""
)

# COMMAND ----------

# DBTITLE 1,Untitled
display(dbutils.fs.ls("abfss://demo@moviehistorytesteo.dfs.core.windows.net"))

# COMMAND ----------

# DBTITLE 1,Untitled
display(spark.read.csv("abfss://demo@moviehistorytesteo.dfs.core.windows.net/movie.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### optención de datos del DATA LAKE con un secret scope y de la clave sin exponer las claves.

# COMMAND ----------

## identificamos el nombre de nuestro Scope
dbutils.secrets.listScopes()

# COMMAND ----------

##y optendremos el nombre de nuestro access key 
dbutils.secrets.list(scope ="movie-hisory-scope")

# COMMAND ----------

## concatenamos stos datos y los almacenamos en una variable en este caso movie_accese_key
movie_accese_key= dbutils.secrets.get(scope="movie-hisory-scope",key="movie-access-key")

# COMMAND ----------

##ahora remplazamos los valores de la celda 2 por la variable

spark.conf.set(
    "fs.azure.account.key.moviehistorytesteo.dfs.core.windows.net",
    movie_accese_key)

## ejecutamos el display para ver que se cargue el contenedor de manera adecuada
display(dbutils.fs.ls("abfss://demo@moviehistorytesteo.dfs.core.windows.net"))

# COMMAND ----------

## visualizamos la bd que esta dentro del contenedor 

display(spark.read.csv("abfss://demo@moviehistorytesteo.dfs.core.windows.net/movie.csv"))

# COMMAND ----------


