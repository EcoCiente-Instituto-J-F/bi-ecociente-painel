# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente — 02 — Auditoria do contrato e Bronze
# MAGIC
# MAGIC Audita o contrato de origem contra o PostgreSQL e ingere as 22 tabelas na Bronze.
# MAGIC
# MAGIC Usa `%run ./00_config` (conexão, helpers, contrato e logs).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02b — Auditoria essencial contra o PostgreSQL
# MAGIC
# MAGIC Confere apenas o que o projeto precisa: a tabela existe, as colunas essenciais existem,
# MAGIC o tipo básico bate. Não audita CHECK/default/identity nem objetos lógicos (functions,
# MAGIC views, triggers) — fora de escopo para um pipeline acadêmico que só lê tabelas.

# COMMAND ----------

def read_jdbc_query(query: str):
    return read_postgres_query(query)

# Tipos Postgres agrupados por família, para comparação "tipo básico bate" (sem exigir
# precisão/escala/tamanho idênticos ao contrato).
_PG_TYPE_FAMILY = {
    "integer": "numeric", "bigint": "numeric", "smallint": "numeric", "numeric": "numeric",
    "boolean": "boolean",
    "character varying": "text", "character": "text", "text": "text",
    "date": "temporal", "timestamp with time zone": "temporal", "timestamp without time zone": "temporal",
}

def audit_source_contract() -> None:
    pg_columns = _aiven_retry(lambda: read_jdbc_query(f"""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{SOURCE_SCHEMA}'
    """).collect(), "auditoria do contrato")

    existing = {}
    for row in pg_columns:
        existing.setdefault(row["table_name"], {})[row["column_name"]] = row["data_type"]

    problems = []
    for table in SOURCE_TABLES:
        if table not in existing:
            problems.append(f"{table}: tabela não encontrada no schema '{SOURCE_SCHEMA}'.")
            continue
        for column, (expected_type, _nullable) in DDL_CONTRACT[table]["columns"].items():
            if column not in existing[table]:
                problems.append(f"{table}.{column}: coluna não encontrada.")
                continue
            actual_type = existing[table][column]
            expected_family = _PG_TYPE_FAMILY.get(expected_type)
            actual_family = _PG_TYPE_FAMILY.get(actual_type)
            if expected_family and actual_family and expected_family != actual_family:
                problems.append(
                    f"{table}.{column}: tipo divergente. esperado~{expected_type} "
                    f"({expected_family}), encontrado={actual_type} ({actual_family})."
                )

    if problems:
        raise RuntimeError(
            "Auditoria do contrato de origem falhou:\n" + "\n".join(f"- {p}" for p in problems)
        )
    print(f"Auditoria OK: {len(SOURCE_TABLES)} tabelas, "
          f"{sum(len(DDL_CONTRACT[t]['columns']) for t in SOURCE_TABLES)} colunas conferidas.")

audit_source_contract()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 — Bronze
# MAGIC
# MAGIC Sem staging separada: cada tabela é lida do PostgreSQL via JDBC (Python, célula 01d) e
# MAGIC gravada direto na Bronze por `overwrite` (SQL), com metadados de ingestão. Sem staging
# MAGIC em duas etapas, sem WAL LSN/`txid_current_snapshot()` — a limitação de snapshot entre
# MAGIC tabelas independentes do Spark JDBC continua existindo, mas não precisa de instrumentação
# MAGIC dedicada para um pipeline acadêmico com volume pequeno e execução rápida.
# MAGIC
# MAGIC Python orquestra o loop entre as 22 tabelas; a limpeza/seleção de colunas é SQL.

# COMMAND ----------

def _ingest_bronze_once(table: str) -> int:
    columns = list(DDL_CONTRACT[table]["columns"])
    df_source = read_postgres_table(table)

    missing = sorted(set(columns).difference(df_source.columns))
    if missing:
        raise RuntimeError(f"{table}: colunas ausentes na leitura JDBC: {missing}")

    df_source.createOrReplaceTempView(f"stg_{table}")
    columns_sql = ", ".join(columns)

    run_sql(f"""
        CREATE OR REPLACE TABLE {bronze_table(table)} AS
        SELECT
            {columns_sql},
            current_timestamp() AS _ingestion_timestamp,
            '{RUN_ID}' AS _pipeline_run_id,
            '{table}' AS _source_table
        FROM stg_{table}
    """)

    rows = scalar(f"SELECT COUNT(*) FROM {bronze_table(table)}")
    log_execucao("BRONZE", table, rows)
    return rows

def ingest_bronze(table: str) -> int:
    return _aiven_retry(lambda: _ingest_bronze_once(table), f"Bronze: {table}")

BRONZE_COUNTS = {table: ingest_bronze(table) for table in SOURCE_TABLES}
print("Bronze OK:", BRONZE_COUNTS)