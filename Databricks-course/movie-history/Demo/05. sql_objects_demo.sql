-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Base de Datos (DataBase)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Objetivos
-- MAGIC 1. Documentación sobre SPARK
-- MAGIC 2. Crear la Base de Datos "demoW
-- MAGIC 3. Aceder al "Catalog" en la "interfaz de Usuario"
-- MAGIC 4. Comando "SHOW"
-- MAGIC 5. Comando "DESCRIBE(DESC)"
-- MAGIC 6. Mostrar la Bas de Datos Actual.

-- COMMAND ----------

CREATE SCHEMA if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database demo;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## tamblas administradas ( managed Tables)
-- MAGIC
-- MAGIC #### Objetivos :
-- MAGIC 1. Crear una **"Tabla Administrada (Managed Table)"** con python.
-- MAGIC 2. Crear una **"Tabla Administrada (Managed Table)"** con SQL.
-- MAGIC 3. Efecto de elmiminar una Tabla Administrada
-- MAGIC 4. Describir(Describe) la Tabla

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_movie_genre_language = spark.read.parquet(f"{gold_folder_path}/results_movie_genre_laguage")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_movie_genre_language.write.format("delta").saveAsTable("demo.results_movie_genre_language_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

describe extended results_movie_genre_language_python

-- COMMAND ----------

create table demo.results_movie_genre_language_sql
as
select * 
from results_movie_genre_language_python
where genre_name ="Adventure";

-- COMMAND ----------

describe extended results_movie_genre_language_sql;

-- COMMAND ----------

drop table if exists demo.results_movie_genre_language_sql;

-- COMMAND ----------

use demo;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##tablas Externas (External Tables)
-- MAGIC ####Objetivos :
-- MAGIC 1. Crear una **"Tabla Externa (External Table)"** con python.
-- MAGIC 2. Crear una **"Tabla Externa (External Table)"** con SQL.
-- MAGIC 3. Efecto de elmiminar una **"Tabla Externa(External Table)"**
-- MAGIC 4. Describir(Describe) la Tabla

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_movie_genre_language.write.format("delta").option("path", f"{gold_folder_path}/results_movie_genre_language_py").saveAsTable("demo.results_movie_genre_language_py")

-- COMMAND ----------

desc extended demo.results_movie_genre_language_py

-- COMMAND ----------

create table demo.results_movie_genre_language_sql(
  title string,
  duration_time int,
  release_date date,
  vote_average float,
  language_name string,
  genre_name string,
  created_date timestamp
)
using delta
location "abfss://gold@moviehistorytesteo.dfs.core.windows.net/results_movie_genre_language_ext_sql"


-- COMMAND ----------

insert into demo.results_movie_genre_language_sql
select * from demo.results_movie_genre_language_py
where genre_name = "Adventure"

-- COMMAND ----------

select count(11)
from demo.results_movie_genre_language_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## solo se eliminara la tabla en databricks ya que al estar solo la metadata en databricks, este solo tiene acceso a la data almacenada en databricks mas no a la admistrada en azure.

-- COMMAND ----------

drop table demo.results_movie_genre_language_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## vistas
-- MAGIC #### Objetivos
-- MAGIC 1. Crear Vistas Temporales
-- MAGIC 2. Crear Vistas Temporales Glovales
-- MAGIC 3. Crear Vistas Permanentes.

-- COMMAND ----------

select current_database()

-- COMMAND ----------

create or replace temp view v_results_movie_genre_language_py
as
select * 
from demo.results_movie_genre_language_py
where genre_name = "Adventure";

-- COMMAND ----------

select * from v_results_movie_genre_language_py;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Vista Temporal Global

-- COMMAND ----------

create or replace global temp view gv_results_movie_genre_language_py
as
select * 
from demo.results_movie_genre_language_py
where genre_name = "Drama";

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

select * from global_temp.gv_results_movie_genre_language_py

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Vista Permanente

-- COMMAND ----------

create or replace view pv_results_movie_genre_language_py
as
select * 
from demo.results_movie_genre_language_py
where genre_name = "Comedy";

-- COMMAND ----------

show tables;


-- COMMAND ----------

select * from pv_results_movie_genre_language_py

-- COMMAND ----------


