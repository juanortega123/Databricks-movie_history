# Databricks notebook source
# MAGIC %md
# MAGIC ### Trabajar con Archivos (estilo URI y POSIX)
# MAGIC 1. Mostrar los directorios de directorio raíz DBFS
# MAGIC 2. mostrar el contenido de un rectorio dentro del directorio raíz DBFS
# MAGIC 3. Mostrar el contenido del Sistema de Archivos Local.
# MAGIC 4. Interactuar con el "Explorador de Archivos de DBFS"
# MAGIC 5. Leer Datos de ADLS mediante estilo URI

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls file:/

# COMMAND ----------

dbutils.fs.ls("/FileStore")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@moviehistorytesteo.dfs.core.windows.net/")

# COMMAND ----------


