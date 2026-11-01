# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente — 05 — Testes da Gold antes da publicação
# MAGIC
# MAGIC Roda a suíte de testes da Gold. Falha aqui impede os KPIs de serem calculados.
# MAGIC
# MAGIC Usa `%run ./00_config` (conexão, helpers, contrato e logs).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 07 — Testes da Gold antes da publicação
# MAGIC
# MAGIC Roda **antes** de qualquer KPI ser calculado sobre a Gold: grão único, FK sem órfã contra
# MAGIC as dimensões, medidas fora do domínio esperado, e ausência de colunas sensíveis/PII. Cada
# MAGIC checagem é uma contagem SQL; Python só decide se ela passou.

# COMMAND ----------

GRAIN_TESTS = {
    "dim_usuario": "usuario_key",
    "dim_condominio": "condominio_key",
    "dim_cooperativa": "cooperativa_key",
    "dim_categoria_residuo": "categoria_residuo_key",
    "dim_status_postagem": "status_postagem_key",
    "dim_status_agendamento": "status_agendamento_key",
    "dim_tipo_evento_pontuacao": "tipo_evento_pontuacao_key",
    "dim_curso": "curso_key",
    "dim_tempo": "data_key",
    "fato_postagens": "postagem_key",
    "fato_coletas": "visita_coleta_key",
    "fato_pontuacao": "evento_referencia_key",
    "fato_tentativas_quiz": "tentativa_key",
    "fato_respostas_quiz": "resposta_key",
}

GOLD_SENSITIVE_COLUMNS = {
    "senha_hash", "nome_usuario", "email_usuario", "cpf", "url_foto", "hash_foto",
    "codigo_acesso", "cnpj", "cnpj_cooperativa", "email_cooperativa", "telefone_cooperativa",
    "endereco_id", "logradouro", "cep", "numero", "complemento", "latitude", "longitude",
}

FK_TESTS = [
    # (fato, coluna_fk, dimensao, coluna_dim, obrigatoria)
    ("fato_postagens", "usuario_key", "dim_usuario", "usuario_key", True),
    ("fato_postagens", "condominio_key", "dim_condominio", "condominio_key", True),
    ("fato_postagens", "categoria_residuo_key", "dim_categoria_residuo", "categoria_residuo_key", True),
    ("fato_postagens", "status_postagem_key", "dim_status_postagem", "status_postagem_key", True),
    ("fato_postagens", "data_key", "dim_tempo", "data_key", True),
    ("fato_coletas", "condominio_key", "dim_condominio", "condominio_key", True),
    ("fato_coletas", "cooperativa_key", "dim_cooperativa", "cooperativa_key", True),
    ("fato_coletas", "status_agendamento_key", "dim_status_agendamento", "status_agendamento_key", True),
    ("fato_coletas", "data_key", "dim_tempo", "data_key", True),
    ("fato_pontuacao", "usuario_key", "dim_usuario", "usuario_key", True),
    ("fato_pontuacao", "condominio_key", "dim_condominio", "condominio_key", False),
    ("fato_pontuacao", "categoria_residuo_key", "dim_categoria_residuo", "categoria_residuo_key", False),
    ("fato_pontuacao", "tipo_evento_pontuacao_key", "dim_tipo_evento_pontuacao", "tipo_evento_pontuacao_key", True),
    ("fato_pontuacao", "data_key", "dim_tempo", "data_key", True),
    ("fato_tentativas_quiz", "usuario_key", "dim_usuario", "usuario_key", True),
    ("fato_tentativas_quiz", "data_key", "dim_tempo", "data_key", True),
    ("fato_respostas_quiz", "usuario_key", "dim_usuario", "usuario_key", True),
    ("fato_respostas_quiz", "curso_key", "dim_curso", "curso_key", True),
    ("fato_respostas_quiz", "data_key", "dim_tempo", "data_key", True),
]

def assert_grain(table: str, key: str) -> int:
    full = gold_table(table)
    dup = scalar(f"""
        SELECT COUNT(*) FROM (
            SELECT {key} FROM {full} WHERE {key} IS NULL
            UNION ALL
            SELECT {key} FROM {full} GROUP BY {key} HAVING COUNT(*) > 1
        )
    """)
    if dup > 0:
        raise RuntimeError(f"{table}: grão violado em {key} ({dup} ocorrências).")
    return scalar(f"SELECT COUNT(*) FROM {full}")

def assert_no_sensitive_columns(table: str) -> None:
    cols = {r["col_name"] for r in run_sql(f"DESCRIBE {gold_table(table)}").collect()}
    forbidden = sorted(GOLD_SENSITIVE_COLUMNS & cols)
    if forbidden:
        raise RuntimeError(f"{table}: Gold expõe colunas sensíveis: {forbidden}")

def assert_fk(fato: str, fk_col: str, dim: str, dim_col: str, obrigatoria: bool) -> None:
    fato_full, dim_full = gold_table(fato), gold_table(dim)
    null_filter = "" if obrigatoria else f"AND f.{fk_col} IS NOT NULL"
    if obrigatoria:
        nulls = scalar(f"SELECT COUNT(*) FROM {fato_full} WHERE {fk_col} IS NULL")
        if nulls > 0:
            raise RuntimeError(f"{fato}.{fk_col}: FK obrigatória contém {nulls} NULLs.")
    orphans = scalar(f"""
        SELECT COUNT(*) FROM {fato_full} f
        LEFT JOIN {dim_full} d ON f.{fk_col} = d.{dim_col}
        WHERE d.{dim_col} IS NULL {null_filter}
    """)
    if orphans > 0:
        raise RuntimeError(f"{fato}.{fk_col}: {orphans} linhas órfãs contra {dim}.{dim_col}.")

def validate_gold() -> dict:
    counts = {}
    for table, key in GRAIN_TESTS.items():
        counts[table] = assert_grain(table, key)
        assert_no_sensitive_columns(table)

    for fato, fk_col, dim, dim_col, obrigatoria in FK_TESTS:
        assert_fk(fato, fk_col, dim, dim_col, obrigatoria)

    # Medidas que não devem ser negativas/inconsistentes.
    _bad_pontos = scalar(f"""
        SELECT COUNT(*) FROM {gold_table('fato_postagens')}
        WHERE pontos_teoricos_configuracao_atual < 0
    """)
    _bad_credito = scalar(f"""
        SELECT COUNT(*) FROM {gold_table('fato_pontuacao')} WHERE credito_confirmado != FALSE
    """)
    for label, invalid in (
        ("fato_postagens.pontos_teoricos_configuracao_atual", _bad_pontos),
        ("fato_pontuacao.credito_confirmado", _bad_credito),
    ):
        if invalid > 0:
            raise RuntimeError(f"Medida inválida em {label}: {invalid} linhas.")

    return counts

GOLD_COUNTS = validate_gold()
print("Gold validada:", GOLD_COUNTS)