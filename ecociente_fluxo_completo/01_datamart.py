# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente | 01 — Valida conexão e disponibilidade do Data Mart
# MAGIC
# MAGIC Tarefa somente leitura. Confirma o banco `ecociente`, a existência das nove tabelas
# MAGIC esperadas no schema `public` e a disponibilidade de registros. Não executa procedure,
# MAGIC não altera o PostgreSQL e não presume a existência de `carregado_em`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

EXPECTED_TABLES = (
    "dim_tempo",
    "dim_usuario",
    "dim_condominio",
    "dim_cooperativa",
    "dim_categoria_residuo",
    "dim_status_postagem",
    "dim_status_agendamento",
    "fato_postagens",
    "fato_coletas",
)

if _widget_value("refresh_datamart", "false").strip().lower() != "false":
    raise RuntimeError(
        "A tarefa 01 é somente leitura. Configure refresh_datamart=false; "
        "a carga do Data Mart deve ser executada separadamente e com autorização."
    )

# Teste de conexão e confirmação do banco de destino.
row = _aiven_retry(
    lambda: read_postgres_query(
        "SELECT current_database() AS banco, current_schema() AS schema_atual"
    ).collect()[0],
    "validação de conexão"
)
if row["banco"] != "ecociente":
    raise RuntimeError(
        f"Conexão apontou para o banco {row['banco']!r}; esperado 'ecociente'."
    )
print(f"Conexão {JDBC_CONNECTION} OK: banco {row['banco']}.")

# Descobre as tabelas físicas do schema público sem depender de colunas de auditoria.
rows = _aiven_retry(
    lambda: read_postgres_query("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
    """).collect(),
    "listagem de tabelas do Data Mart"
)
available = {str(r["table_name"]) for r in rows}
missing = sorted(set(EXPECTED_TABLES) - available)
if missing:
    raise RuntimeError(
        "Data Mart incompleto no schema public. Tabelas ausentes: "
        + ", ".join(missing)
    )

# Confirma leitura e contagem em cada tabela. Identificadores são nomes constantes
# da lista EXPECTED_TABLES, não valores recebidos de widgets ou de usuários.
counts = []
for table in EXPECTED_TABLES:
    result = _aiven_retry(
        lambda table=table: read_postgres_query(
            f'SELECT COUNT(*) AS total FROM public."{table}"'
        ).collect()[0],
        f"contagem de public.{table}"
    )
    total = int(result["total"] or 0)
    counts.append((table, total))
    print(f"public.{table}: {total:,} registros")

empty = [table for table, total in counts if total == 0]
if empty:
    raise RuntimeError(
        "Uma ou mais tabelas obrigatórias estão vazias: " + ", ".join(empty)
    )

print(
    f"Validação concluída: {len(counts)} tabelas obrigatórias presentes e não vazias. "
    "Nenhuma procedure de carga foi executada."
)

# COMMAND ----------

# Resultado tabular para evidência da execução no Databricks.
display(spark.createDataFrame(counts, ["tabela", "total_registros"]))
