# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente | 01 — Valida conexão e banco de origem
# MAGIC Usa `%run ./00_config`. Executa somente SELECT no Aiven; não chama procedures.
# MAGIC `refresh_datamart=true` é recusado nesta cópia para preservar o banco.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

if _widget_value("refresh_datamart", "false").strip().lower() != "false":
    raise RuntimeError("Esta versão é somente leitura. Use refresh_datamart=false.")

row = _aiven_retry(lambda: read_postgres_query(
    "SELECT current_database() AS banco, 1 AS teste"
).collect()[0], "validação inicial")
if row["banco"] != "ecociente" or row["teste"] != 1:
    raise RuntimeError("A conexão deve apontar para o banco ecociente. Confira a URL da conexão.")
print(f"Conexão {JDBC_CONNECTION} OK: banco ecociente; SELECT 1 confirmado.")
print("Aiven somente leitura. Nenhuma procedure de carga foi executada.")