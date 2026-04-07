-- Databricks notebook source
create schema if not exists movie_silver
MANAGED LOCATION "abfss://silver@moviehistorytesteo.dfs.core.windows.net/";


-- COMMAND ----------

desc database movie_silver;

-- COMMAND ----------


