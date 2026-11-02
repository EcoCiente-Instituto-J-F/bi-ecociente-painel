# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente — 03 — Data Quality e Silver
# MAGIC
# MAGIC Aplica as regras de Data Quality (grava `invalid_keys`) e constrói a Silver.
# MAGIC
# MAGIC Usa `%run ./00_config` (conexão, helpers, contrato e logs).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Bronze já foi gravada pelo notebook 02: relê as contagens da própria tabela.
BRONZE_COUNTS = contar_tabelas(bronze_table, SOURCE_TABLES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04 — Data Quality
# MAGIC
# MAGIC Regras mantidas: grão único (PK sem NULL e sem duplicidade), NOT NULL do DDL, FK sem
# MAGIC referência órfã, e duas regras semânticas do domínio (torre pertence ao mesmo condomínio da
# MAGIC entidade; `concluido_em >= iniciado_em` em tentativas de quiz).
# MAGIC
# MAGIC Cada violação é gravada em `quality.invalid_keys` (`run_id`, `tabela`, `chave`, `regra`), então
# MAGIC dá para consultar **qual regra** excluiu cada linha. As chaves inválidas ficam de fora da
# MAGIC Silver (via a temp view `invalid_keys_<tabela>`, montada a partir dessa tabela); o restante
# MAGIC segue normalmente. Não há bloqueio do pipeline por violação isolada. O `data_quality_log`
# MAGIC guarda as contagens por tabela.

# COMMAND ----------

INVALID_KEYS_TABLE = f"{TARGET_CATALOG}.{QUALITY_SCHEMA}.invalid_keys"

run_sql(f"""
CREATE TABLE IF NOT EXISTS {INVALID_KEYS_TABLE} (
    run_id STRING,
    tabela STRING,
    chave BIGINT,
    regra STRING,
    detectado_em TIMESTAMP
) USING DELTA
""")

def _invalid_keys_sql(table: str) -> str:
    contract = DDL_CONTRACT[table]
    pk = contract["pk"]
    src = bronze_table(table)
    parts = []

    for column, (_type, nullable) in contract["columns"].items():
        if not nullable:
            parts.append(
                f"SELECT CAST({pk} AS BIGINT) AS chave, 'NOT_NULL:{column}' AS regra "
                f"FROM {src} WHERE {column} IS NULL"
            )

    parts.append(
        f"SELECT CAST({pk} AS BIGINT) AS chave, 'PK_DUPLICADA' AS regra "
        f"FROM {src} GROUP BY {pk} HAVING COUNT(*) > 1"
    )

    for fk_col, ref_table, ref_col in contract["fks"]:
        ref = bronze_table(ref_table)
        parts.append(f"""
            SELECT CAST(b.{pk} AS BIGINT) AS chave, 'FK_ORFA:{fk_col}->{ref_table}' AS regra
            FROM {src} b
            WHERE b.{fk_col} IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM {ref} r WHERE r.{ref_col} = b.{fk_col})
        """)

    if table in ("tb_postagens", "tb_tentativas_quiz"):
        parts.append(f"""
            SELECT CAST(e.{pk} AS BIGINT) AS chave, 'TORRE_DE_OUTRO_CONDOMINIO' AS regra
            FROM {src} e
            JOIN {bronze_table('tb_torres')} tw ON tw.id_torre = e.torre_id
            WHERE e.torre_id IS NOT NULL AND tw.condominio_id != e.condominio_id
        """)

    if table == "tb_tentativas_quiz":
        parts.append(f"""
            SELECT CAST({pk} AS BIGINT) AS chave, 'CONCLUIDO_ANTES_DE_INICIADO' AS regra
            FROM {src}
            WHERE concluido_em IS NOT NULL AND concluido_em < iniciado_em
        """)

    return " UNION ALL ".join(parts)

def validate_bronze(table: str) -> int:
    # DELETE antes do INSERT deixa a execução repetível com o mesmo run_id.
    run_sql(f"DELETE FROM {INVALID_KEYS_TABLE} WHERE run_id = '{RUN_ID}' AND tabela = '{table}'")
    run_sql(f"""
        INSERT INTO {INVALID_KEYS_TABLE}
        SELECT '{RUN_ID}', '{table}', chave, regra, current_timestamp()
        FROM ({_invalid_keys_sql(table)}) AS q
    """)
    # A Silver continua lendo invalid_keys_<tabela>; agora ela vem da tabela persistida.
    run_sql(f"""
        CREATE OR REPLACE TEMP VIEW invalid_keys_{table} AS
        SELECT DISTINCT chave FROM {INVALID_KEYS_TABLE}
        WHERE run_id = '{RUN_ID}' AND tabela = '{table}'
    """)
    invalid = scalar(f"SELECT COUNT(*) FROM invalid_keys_{table}")
    total = BRONZE_COUNTS[table]
    log_qualidade(table, total, invalid)
    return invalid

QUARANTINE_COUNTS = {table: validate_bronze(table) for table in SOURCE_TABLES}

_critical = {t: c for t, c in QUARANTINE_COUNTS.items() if c > 0}
if _critical:
    print(f"AVISO: linhas inválidas encontradas e excluídas da Silver: {_critical}")
print("Data Quality OK — detalhe em", INVALID_KEYS_TABLE, "e", DQ_LOG_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05 — Silver
# MAGIC
# MAGIC Tipagem alinhada ao contrato (`DDL_CONTRACT`), `TRIM` só nas colunas de texto que
# MAGIC justificam, remoção de `senha_hash`, e exclusão das linhas marcadas como inválidas na
# MAGIC célula 04. `NUMERIC` do Postgres vira `DOUBLE` — o contrato essencial não carrega
# MAGIC precisão/escala por coluna (simplificação deliberada da célula 02a), e nenhum valor deste
# MAGIC projeto (notas, coordenadas, saldo de confiança) precisa de precisão decimal exata.

# COMMAND ----------

_SPARK_TYPE_BY_PG = {
    "integer": "INT", "bigint": "BIGINT", "smallint": "SMALLINT",
    "boolean": "BOOLEAN",
    "character varying": "STRING", "character": "STRING", "text": "STRING",
    "date": "DATE",
    "timestamp with time zone": "TIMESTAMP", "timestamp without time zone": "TIMESTAMP",
    "numeric": "DOUBLE",
}

TEXT_COLUMNS_TO_TRIM = {
    "tb_lkp_tipos_usuarios": ["nome_tipo", "descricao"],
    "tb_usuarios": ["nome_usuario", "email_usuario", "cpf"],
    "tb_enderecos": ["cep", "estado", "cidade", "logradouro", "numero", "complemento"],
    "tb_lkp_tipos_condominios": ["nome_tipo", "descricao"],
    "tb_condominios": ["nome_condominio", "cnpj", "codigo_acesso"],
    "tb_torres": ["nome_torre"],
    "tb_cooperativas": ["cnpj_cooperativa", "nome_cooperativa", "email_cooperativa", "telefone_cooperativa"],
    "tb_lkp_categorias_residuos": ["nome_categoria", "descricao_material", "cor_identificacao"],
    "tb_lkp_status_validacoes_postagens": ["nome_status", "descricao"],
    "tb_lkp_status_agendamentos": ["nome_status"],
    "tb_visitas_coletas": ["observacao"],
    "tb_avaliacoes_visitas_coletas": ["comentario"],
    "tb_cursos": ["titulo_curso", "descricao_curso"],
    "tb_aulas": ["titulo_aula", "conteudo_aula"],
    "tb_quizzes": ["titulo_quiz"],
    "tb_perguntas_quiz": ["enunciado"],
    "tb_alternativas_quiz": ["texto_alternativa"],
}

def silver_select_sql(table: str) -> str:
    contract = DDL_CONTRACT[table]
    trim_cols = set(TEXT_COLUMNS_TO_TRIM.get(table, []))
    exprs = []
    for column, (pg_type, _nullable) in contract["columns"].items():
        if table == "tb_usuarios" and column == "senha_hash":
            continue
        spark_type = _SPARK_TYPE_BY_PG[pg_type]
        expr = f"CAST({column} AS {spark_type})"
        if column in trim_cols:
            expr = f"TRIM({expr})"
        exprs.append(f"{expr} AS {column}")

    pk = contract["pk"]
    return f"""
        SELECT {', '.join(exprs)}, current_timestamp() AS _silver_processed_at
        FROM {bronze_table(table)} b
        WHERE NOT EXISTS (SELECT 1 FROM invalid_keys_{table} ik WHERE ik.chave = b.{pk})
    """

def build_silver(table: str) -> int:
    run_sql(f"CREATE OR REPLACE TABLE {silver_table(table)} AS {silver_select_sql(table)}")

    rows = scalar(f"SELECT COUNT(*) FROM {silver_table(table)}")
    expected = BRONZE_COUNTS[table] - QUARANTINE_COUNTS[table]
    if rows != expected:
        raise RuntimeError(
            f"{table}: contagem Silver diverge do esperado. "
            f"bronze={BRONZE_COUNTS[table]}, invalidas={QUARANTINE_COUNTS[table]}, "
            f"esperado={expected}, obtido={rows}"
        )
    if table == "tb_usuarios":
        cols = [r["col_name"] for r in run_sql(f"DESCRIBE {silver_table(table)}").collect()]
        if "senha_hash" in cols:
            raise RuntimeError("tb_usuarios: senha_hash não pode ser persistido na Silver.")

    log_execucao("SILVER", table, rows)
    return rows

SILVER_COUNTS = {table: build_silver(table) for table in SOURCE_TABLES}
print("Silver OK:", SILVER_COUNTS)