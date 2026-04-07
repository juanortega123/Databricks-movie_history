# Databricks notebook source
# Obtenemos el parámetro del padre
parametro = dbutils.widgets.get("p_ex")

if parametro == "0":
    # FORZAMOS UN ERROR: División por cero
    print("El parámetro es 0, voy a fallar...")
    1 / 0 
else:
    # Si no es cero, todo sale bien
    resultado = int(parametro) * 10
    dbutils.notebook.exit(str(resultado))
