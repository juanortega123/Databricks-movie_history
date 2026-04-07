# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount (montar ) Azure Data Lake mediante sercive Princial
# MAGIC 1. Obtener el valor client_id, tenant_id y client_secret del key Vault
# MAGIC 2. Configurar Spark con APP/Client id, Directory/Tenant Id & Secret
# MAGIC 3. Utilizar el método "mount" de "utility" para montar el almacenamiento
# MAGIC 4. Explorar otras utilidades del sistema de archivos relacionado con el montaje (list all mounts,unmounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### optención de datos del DATA LAKE con un secret scope y Service Principal sin exponer las claves.

# COMMAND ----------

client_id_sc= dbutils.secrets.get(scope="movie-hisory-scope", key= "movie-token-SP-client")
tenant_id_sc= dbutils.secrets.get(scope="movie-hisory-scope", key="movie-token-tenant")
client_secret_sc= dbutils.secrets.get(scope="movie-hisory-scope", key="movie-token-certificate")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id_sc,
          "fs.azure.account.oauth2.client.secret": client_secret_sc,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id_sc}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@moviehistorytesting.dfs.core.windows.net/",
  mount_point = "/mnt/moviehistorytesting/demo",
  extra_configs = configs)

# COMMAND ----------

display(spark.read.csv("abfss://demo@moviehistorytesting.dfs.core.windows.net/movie.csv"))
