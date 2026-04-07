# Databricks notebook source
# MAGIC %md
# MAGIC ### Acceder a Azure Data Lake Storage mediante Service Principal
# MAGIC 1. "Registrar la Aplicación" en Azure Entra ID / Service Principal.
# MAGIC 2. Generar un secreto (Contraseña) para la aplicación
# MAGIC 3. Configurar Spark con APP / Client id,Directory / Tenand id & Secret
# MAGIC 4. Asignar el Role "Storage Blob Data Contributor" al Data Lake

# COMMAND ----------

client_id=""
tenant_id=""
client_secret=""

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.moviehistorytesteo.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.moviehistorytesteo.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.moviehistorytesteo.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.moviehistorytesteo.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.moviehistorytesteo.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@moviehistorytesteo.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistorytesteo.dfs.core.windows.net/movie.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### optención de datos del DATA LAKE con un secret scope y Service Principal sin exponer las claves.

# COMMAND ----------

client_id_sc= dbutils.secrets.get(scope="movie-hisory-scope", key= "movie-token-SP-client")
tenant_id_sc= dbutils.secrets.get(scope="movie-hisory-scope", key="movie-token-tenant")
client_secret_sc= dbutils.secrets.get(scope="movie-hisory-scope", key="movie-token-certificate")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.moviehistorytesting.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.moviehistorytesting.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.moviehistorytesting.dfs.core.windows.net", client_id_sc)
spark.conf.set("fs.azure.account.oauth2.client.secret.moviehistorytesting.dfs.core.windows.net", client_secret_sc)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.moviehistorytesting.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{tenant_id_sc}/oauth2/token")

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistorytesteo.dfs.core.windows.net/movie.csv"))
