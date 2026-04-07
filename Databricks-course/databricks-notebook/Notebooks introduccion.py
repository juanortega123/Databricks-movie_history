# Databricks notebook source
# MAGIC %md
# MAGIC ### Introduccion a Notebooks
# MAGIC #### Interfáz Gráficas
# MAGIC #### Comandos Mágicos
# MAGIC   -%python
# MAGIC   -%sql
# MAGIC   -%scala
# MAGIC   -%r

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comando Mágico %python

# COMMAND ----------

messaje = "Hello World"

# COMMAND ----------

print(messaje)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comando Mágico %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hola"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comando Mágico %scala
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %scala
# MAGIC var msg = "Hola Mundo"
# MAGIC print(msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comando Mágico %r

# COMMAND ----------

# MAGIC %r
# MAGIC print("Hola Mundo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comando Mágico %fs

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comando Mágico %sh

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comando Mágico %lsmagic

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

# MAGIC %env?
# MAGIC

# COMMAND ----------


