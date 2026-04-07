-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Configurar Acceso Azure Data Lake Storage Gen2
-- MAGIC 1. Crear el objaeto "External Location"llamado "external_location_bronze_mov_hist_test"
-- MAGIC 2. Crear el objaeto "External Location"llamado "external_location_silver_mov_hist_test"
-- MAGIC 3. Crear el objaeto "External Location"llamado "external_location_gold_mov_hist_test"
-- MAGIC 4. Validar el acceso a los objetos "Externals Locations" creados

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS external_location_bronze_mov_hist_test
    URL 'abfss://bronze@moviehistorytesteo.dfs.core.windows.net'
    WITH (STORAGE CREDENTIAL storage_credential_mov_his);
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA `external_location_bronze_mov_hist_test` TO `e2cb736f-77e2-4118-869e-49859af097e1`;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS external_location_silver_mov_hist_test
    URL 'abfss://silver@moviehistorytesteo.dfs.core.windows.net'
    WITH (STORAGE CREDENTIAL storage_credential_mov_his)
    COMMENT 'External Location For the silver Data Lakehouse.';
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA `databricks_course_workspace`.`movie_silver` TO `e2cb736f-77e2-4118-869e-49859af097e1`; 

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS external_location_gold_mov_hist_test
    URL 'abfss://gold@moviehistorytesteo.dfs.core.windows.net'
    WITH (STORAGE CREDENTIAL storage_credential_mov_his)
    COMMENT 'External Location For the Gold Data Lakehouse.';
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA `databricks_course_workspace`.`movie_gold` TO `e2cb736f-77e2-4118-869e-49859af097e1`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('abfss://bronze@moviehistorytesteo.dfs.core.windows.net/')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('abfss://silver@moviehistorytesting.dfs.core.windows.net/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('abfss://gold@moviehistorytesting.dfs.core.windows.net/')

-- COMMAND ----------

DROP EXTERNAL LOCATION external_location_demo_mov_his;

-- Ahora ya puedes ejecutar tu comando original
CREATE EXTERNAL LOCATION IF NOT EXISTS external_location_demo_mov_hist_test
    URL 'abfss://demo@moviehistorytesteo.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL storage_credential_mov_his)
    COMMENT 'External Location For the Demo Data Lakehouse.';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('abfss://demo@moviehistorytesteo.dfs.core.windows.net/')
