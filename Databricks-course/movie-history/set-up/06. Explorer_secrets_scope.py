# Databricks notebook source
# MAGIC %md
# MAGIC ### Exlorar las capacidades de la utilidad "dbutulis.secrets"
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope ="movie-hisory-scope")

# COMMAND ----------

dbutils.secrets.get(scope="movie-hisory-scope",key="movie-access-key")

# COMMAND ----------


