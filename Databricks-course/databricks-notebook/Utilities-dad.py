# Databricks notebook source
try:
    print("Llamando al hijo con valor 0 para forzar error...")
    # Enviamos "0" para que el hijo falle
    v_ex = dbutils.notebook.run("./Utilities-child", 60, {"p_ex": "20"})
    print(f"Éxito: El hijo devolvió {v_ex}")

except Exception as e:
    print("\n--- ¡ALERTA! El notebook hijo ha fallado ---")
    print(f"Tipo de error: {type(e).__name__}")
    print(f"Mensaje del error: {e}")
    
    # Aquí el padre puede tomar una decisión alternativa
    v_ex = "VALOR_RESCATE"
    print(f"Continuando ejecución con valor de rescate: {v_ex}")

# COMMAND ----------

print(v_ex)

# COMMAND ----------


