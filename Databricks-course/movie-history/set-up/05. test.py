# Databricks notebook source
# MAGIC %md
# MAGIC #### Prueba de lo aprendido el dia de hoy.
# MAGIC 1. configurar un Storage Account
# MAGIC 2. configurar un contenedor 
# MAGIC 3. colocar un archivo dentro de una carpeta para mayor dificultad.
# MAGIC 4. crear un Services Principal para poder lograr conectarlo con Azure Data Lake Storage.
# MAGIC 5. Mostrar las conecciones.

# COMMAND ----------

client_id = ""
tenant_id = ""
client_secret= ""

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.moviehistorial.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.moviehistorial.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.moviehistorial.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.moviehistorial.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.moviehistorial.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(spark.read.csv("abfss://demolicion@moviehistorial.dfs.core.windows.net/files-demo/movie.csv"))
