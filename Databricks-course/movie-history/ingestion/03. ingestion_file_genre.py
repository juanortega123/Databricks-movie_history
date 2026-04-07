# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingestión de datos del archivo "genre"

# COMMAND ----------

dbutils.widgets.text("p_envionment", "")
v_environment= dbutils.widgets.get("p_envionment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date= dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/commom_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso 1 - Extraer el archivo en un Dataframe con "DataFrameReader"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit , current_timestamp

# COMMAND ----------

genre_schema=StructType(fields=[
    StructField("genreId", IntegerType(), False),
    StructField("genreName", StringType(), True)
])

# COMMAND ----------

genre_df = spark.read.csv(f"{bronze_folder_path}/{v_file_date}/genre.csv", header = True,schema= genre_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ## paso 2 - cambiar de nombre con la función rename y agregar las nuevas columnas de "ingestion_date" y "environmet"

# COMMAND ----------

# cambiar el nombre de las columnas actuales.

genre_renamed = genre_df.withColumnsRenamed({"genreId":"genre_id", "genreName": "genre_name"})

## agregar las columnas "ingestion_date" y "environmet"

genre_final_df= add_ingestion_date(genre_renamed).withColumn("environment",lit(v_environment)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### paso 3 - Sobreescribir los datos en formato parquet en la capa silver

# COMMAND ----------

# 1. Definimos la condición de cruce para el motor de Delta
condicion_merge = "tgt.genre_id = src.genre_id AND tgt.file_date = src.file_date"

# 2. Ejecutamos la función
merge_delta_table(genre_final_df,"movie_silver","genre",condicion_merge,"file_date",["genre_id", "file_date"])

# COMMAND ----------

dbutils.notebook.exit("Success")
