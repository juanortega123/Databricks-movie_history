-- Databricks notebook source
create schema if not exists movie_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Crear tablas para archivos CSV

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Crear la tabla "movies"

-- COMMAND ----------

drop table if exists movie_bronze.movies;
create table if not exists movie_bronze.movies(
movieId int,
title string,
budget double,
homePage string,
overview string,
popularity double,
yearReleaseDate int,
releaseDate date,
revenue double,
durationTime int,
movieStatus string, 
tagline string,
voteAverage double,
voteCount int
)
using csv
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/movie.csv", header true)

-- COMMAND ----------

create table if not exists movie_bronze.languages(
  languageId int,
  languageCode string,
  languageName string
)
using csv 
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/language.csv", header true)

-- COMMAND ----------

create table if not exists movie_bronze.genre(
  genreId int,
  genreName string
)
using csv
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/genre.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Crear tablas para archivos JSON

-- COMMAND ----------

drop table if exists movie_bronze.countries;
create table if not exists movie_bronze.countries(
  countryId int,
  countryIsoCode string,
  countryName string
)
using json
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/counties.json", header true)

-- COMMAND ----------

drop table if exists movie_bronze.persons;
create table if not exists movie_bronze.persons(
  personId int,
  personName struct<forename: string , surname: string>
)
using json
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/person.json", header true)

-- COMMAND ----------

drop table if exists movie_bronze.movie_genre;
create table if not exists movie_bronze.movie_genre(
  movieId int,
  genreId string
)
using json
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/movie_genre.json", header true)

-- COMMAND ----------

drop table if exists movie_bronze.movie_cast;
create table if not exists movie_bronze.movie_cast(
  movieId int,
  personId int,
  characterName string,
  genderId int,
  castOrder int
)
using json
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/movie_cast.json", multiLine true, header true)

-- COMMAND ----------

drop table if exists movie_bronze.language_role;
create table if not exists movie_bronze.language_role(
  roleId int, 
  languageRole string
)
using json
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/language_role.json",multiLine true, header  true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Crear tablas para folders de archivos.

-- COMMAND ----------

drop table if exists movie_bronze.productions_companies;
create table if not exists movie_bronze.productions_companies(
  companyId int,
  companyName string
)
using csv
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/production_company",headed true)

-- COMMAND ----------

drop table if exists movie_bronze.movie_company;
create table if not exists movie_bronze.movie_company(
  movieId int,
  companyId int
)
using csv
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/movie_company", header true)

-- COMMAND ----------

drop table if exists movie_bronze.movie_language;
create table if not exists movie_bronze.movie_language(
  movieId int,
  languageId int, 
  languageRoleId int
)
using json
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/movie_language", multiLine true, header true)

-- COMMAND ----------

drop table if exists movie_bronze.production_country;
create table if not exists movie_bronze.production_country(
  movieId int,
  countryId int
)
using json
options(path "abfss://bronze@moviehistorytesteo.dfs.core.windows.net/production_country",multiLine true, header true)

-- COMMAND ----------

select * from movie_bronze.production_country

-- COMMAND ----------


