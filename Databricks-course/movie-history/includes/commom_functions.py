# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def merge_delta_table(input_df, db_name, table_name, join_condition, partition_col, pk_cols):
    """
    Función universal para carga Silver/Gold en Delta Lake.
    
    Args:
        input_df: DataFrame con los nuevos datos.
        db_name: Nombre de la base de datos (esquema).
        table_name: Nombre de la tabla destino.
        join_condition: String con la condición del MERGE (ej: "tgt.id = src.id").
        partition_col: Columna para la partición física en DISCO (carpetas).
        pk_cols: Lista de columnas que definen la unicidad en MEMORIA (para row_number).
    """
    from delta.tables import DeltaTable
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, col

    # --- PASO 1: DEDUPLICACIÓN EN MEMORIA (RAM) ---
    # Esto agrupa por ID y Fecha en RAM para numerar duplicados.
    # NO crea carpetas físicas, solo organiza el conteo.
    window_spec = Window.partitionBy(pk_cols).orderBy(col(partition_col).desc())
    
    df_unique = input_df.withColumn("_sn", row_number().over(window_spec)) \
                        .filter("_sn == 1") \
                        .drop("_sn")

    full_table_name = f"{db_name}.{table_name}"

    # --- PASO 2: ESCRITURA O ACTUALIZACIÓN ---
    if spark.catalog.tableExists(full_table_name):
        # Si la tabla ya existe, hacemos MERGE
        print(f"-> Realizando MERGE en {full_table_name}...")
        deltaTable = DeltaTable.forName(spark, full_table_name)
        
        deltaTable.alias('tgt') \
            .merge(
                df_unique.alias('src'),
                join_condition
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        # Si la tabla es nueva, la creamos particionada en DISCO
        # Aquí partitionBy(partition_col) creará una carpeta por cada fecha.
        print(f"-> Creando tabla nueva {full_table_name}...")
        df_unique.write.mode("overwrite") \
            .partitionBy(partition_col) \
            .format("delta") \
            .saveAsTable(full_table_name)
