# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente — 06 — KPIs mensais e encerramento do run
# MAGIC
# MAGIC Calcula os KPIs mensais sobre a Gold já testada e encerra o run.
# MAGIC
# MAGIC Usa `%run ./00_config` (conexão, helpers, contrato e logs).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Camadas já gravadas pelos notebooks 02, 03 e 04: relê as contagens das tabelas.
BRONZE_COUNTS = contar_tabelas(bronze_table, SOURCE_TABLES)
SILVER_COUNTS = contar_tabelas(silver_table, SOURCE_TABLES)
GOLD_COUNTS = contar_schema(GOLD_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 08 — KPIs mensais

# COMMAND ----------

# MAGIC %md
# MAGIC ### kpi_condominio_mensal
# MAGIC
# MAGIC Grão = 1 linha por `condominio_key` × `ano_mes_key` — universo completo (`CROSS JOIN`
# MAGIC entre todos os condomínios e todos os meses de `dim_tempo`), com zeros explícitos onde não
# MAGIC houve atividade. Alimenta o mini-gráfico de tendência por condomínio na Aba 2.

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('kpi_condominio_mensal')} AS
WITH meses AS (
    SELECT DISTINCT ano * 100 + mes AS ano_mes_key, ano, mes FROM {gold_table('dim_tempo')}
),
condominios_meses AS (
    SELECT dc.condominio_key, dc.nome_condominio, m.ano_mes_key, m.ano, m.mes
    FROM {gold_table('dim_condominio')} dc
    CROSS JOIN meses m
),
postagens_status AS (
    SELECT f.*, d.status_postagem AS status_txt, t.ano * 100 + t.mes AS ano_mes_key
    FROM {gold_table('fato_postagens')} f
    JOIN {gold_table('dim_status_postagem')} d ON d.status_postagem_key = f.status_postagem_key
    JOIN {gold_table('dim_tempo')} t ON t.data_key = f.data_key
),
postagens AS (
    SELECT
        condominio_key, ano_mes_key,
        COUNT(*) AS total_postagens,
        COUNT(CASE WHEN lower(trim(status_txt)) = 'aprovada' THEN 1 END) AS postagens_aprovadas,
        COUNT(DISTINCT usuario_key) AS usuarios_com_postagem
    FROM postagens_status
    GROUP BY condominio_key, ano_mes_key
),
coletas AS (
    SELECT
        c.condominio_key, t.ano * 100 + t.mes AS ano_mes_key,
        COUNT(*) AS total_coletas,
        COUNT(CASE WHEN c.foi_realizada THEN 1 END) AS coletas_realizadas
    FROM {gold_table('fato_coletas')} c
    JOIN {gold_table('dim_tempo')} t ON t.data_key = c.data_key
    GROUP BY c.condominio_key, t.ano * 100 + t.mes
),
quiz AS (
    SELECT
        p.condominio_key, t.ano * 100 + t.mes AS ano_mes_key,
        COUNT(*) AS total_quiz_aprovados
    FROM {gold_table('fato_pontuacao')} p
    JOIN {gold_table('dim_tempo')} t ON t.data_key = p.data_key
    WHERE p.tipo_evento_referencia = 'quiz_aprovado' AND p.condominio_key IS NOT NULL
    GROUP BY p.condominio_key, t.ano * 100 + t.mes
),
eventos_engajamento AS (
    SELECT condominio_key, usuario_key, data_key FROM {gold_table('fato_postagens')}
    WHERE usuario_key IS NOT NULL AND condominio_key IS NOT NULL
    UNION ALL
    SELECT condominio_key, usuario_key, data_key FROM {gold_table('fato_pontuacao')}
    WHERE tipo_evento_referencia = 'quiz_aprovado' AND usuario_key IS NOT NULL AND condominio_key IS NOT NULL
),
engajados_mes AS (
    SELECT DISTINCT e.condominio_key, t.ano * 100 + t.mes AS ano_mes_key, e.usuario_key
    FROM eventos_engajamento e
    JOIN {gold_table('dim_tempo')} t ON t.data_key = e.data_key
),
engajamento AS (
    SELECT condominio_key, ano_mes_key, COUNT(*) AS usuarios_engajados
    FROM engajados_mes GROUP BY condominio_key, ano_mes_key
)
SELECT
    cm.condominio_key,
    cm.nome_condominio,
    cm.ano_mes_key,
    format_string('%04d-%02d', cm.ano, cm.mes) AS ano_mes,
    COALESCE(postagens.total_postagens, 0) AS total_postagens,
    COALESCE(postagens.postagens_aprovadas, 0) AS postagens_aprovadas,
    ROUND(100.0 * COALESCE(postagens.postagens_aprovadas, 0) / NULLIF(postagens.total_postagens, 0), 2)
        AS taxa_aprovacao_postagens,
    COALESCE(coletas.total_coletas, 0) AS total_coletas,
    COALESCE(coletas.coletas_realizadas, 0) AS coletas_realizadas,
    ROUND(100.0 * COALESCE(coletas.coletas_realizadas, 0) / NULLIF(coletas.total_coletas, 0), 2)
        AS taxa_realizacao_coletas,
    COALESCE(quiz.total_quiz_aprovados, 0) AS total_quiz_aprovados,
    COALESCE(engajamento.usuarios_engajados, 0) AS usuarios_engajados
FROM condominios_meses cm
LEFT JOIN postagens  ON postagens.condominio_key = cm.condominio_key AND postagens.ano_mes_key = cm.ano_mes_key
LEFT JOIN coletas    ON coletas.condominio_key = cm.condominio_key AND coletas.ano_mes_key = cm.ano_mes_key
LEFT JOIN quiz       ON quiz.condominio_key = cm.condominio_key AND quiz.ano_mes_key = cm.ano_mes_key
LEFT JOIN engajamento ON engajamento.condominio_key = cm.condominio_key AND engajamento.ano_mes_key = cm.ano_mes_key
ORDER BY cm.condominio_key, cm.ano_mes_key
""")

_kpi_cm_rows = scalar(f"SELECT COUNT(*) FROM {gold_table('kpi_condominio_mensal')}")
_expected_cm_rows = scalar(f"SELECT COUNT(*) FROM {gold_table('dim_condominio')}") * \
                     scalar(f"SELECT COUNT(DISTINCT ano * 100 + mes) FROM {gold_table('dim_tempo')}")
if _kpi_cm_rows != _expected_cm_rows:
    raise RuntimeError(
        f"kpi_condominio_mensal: esperado condominios x meses = {_expected_cm_rows}, "
        f"obtido {_kpi_cm_rows}."
    )
log_execucao("GOLD", "kpi_condominio_mensal", _kpi_cm_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### kpi_categoria_mensal
# MAGIC
# MAGIC Grão = 1 linha por `categoria_residuo_key` × `ano_mes_key`, mesmo raciocínio de
# MAGIC `kpi_condominio_mensal`. Alimenta o gráfico de área empilhada de categorias ao longo do
# MAGIC tempo na Aba 3.

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('kpi_categoria_mensal')} AS
WITH meses AS (
    SELECT DISTINCT ano * 100 + mes AS ano_mes_key, ano, mes FROM {gold_table('dim_tempo')}
),
categorias_meses AS (
    SELECT dcr.categoria_residuo_key, dcr.nome_categoria, dcr.cor_identificacao,
           m.ano_mes_key, m.ano, m.mes
    FROM {gold_table('dim_categoria_residuo')} dcr
    CROSS JOIN meses m
),
postagens_status AS (
    SELECT f.*, d.status_postagem AS status_txt, t.ano * 100 + t.mes AS ano_mes_key
    FROM {gold_table('fato_postagens')} f
    JOIN {gold_table('dim_status_postagem')} d ON d.status_postagem_key = f.status_postagem_key
    JOIN {gold_table('dim_tempo')} t ON t.data_key = f.data_key
    WHERE f.categoria_residuo_key IS NOT NULL
),
postagens AS (
    SELECT
        categoria_residuo_key, ano_mes_key,
        COUNT(*) AS total_postagens,
        COUNT(CASE WHEN lower(trim(status_txt)) = 'aprovada' THEN 1 END) AS total_postagens_aprovadas
    FROM postagens_status
    GROUP BY categoria_residuo_key, ano_mes_key
)
SELECT
    cm.categoria_residuo_key,
    cm.nome_categoria,
    cm.cor_identificacao,
    cm.ano_mes_key,
    format_string('%04d-%02d', cm.ano, cm.mes) AS ano_mes,
    COALESCE(p.total_postagens, 0) AS total_postagens,
    COALESCE(p.total_postagens_aprovadas, 0) AS total_postagens_aprovadas
FROM categorias_meses cm
LEFT JOIN postagens p
    ON p.categoria_residuo_key = cm.categoria_residuo_key AND p.ano_mes_key = cm.ano_mes_key
ORDER BY cm.categoria_residuo_key, cm.ano_mes_key
""")

_kpi_catm_rows = scalar(f"SELECT COUNT(*) FROM {gold_table('kpi_categoria_mensal')}")
_expected_catm_rows = scalar(f"SELECT COUNT(*) FROM {gold_table('dim_categoria_residuo')}") * \
                       scalar(f"SELECT COUNT(DISTINCT ano * 100 + mes) FROM {gold_table('dim_tempo')}")
if _kpi_catm_rows != _expected_catm_rows:
    raise RuntimeError(
        f"kpi_categoria_mensal: esperado categorias x meses = {_expected_catm_rows}, "
        f"obtido {_kpi_catm_rows}."
    )
log_execucao("GOLD", "kpi_categoria_mensal", _kpi_catm_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### kpi_mensal
# MAGIC
# MAGIC Grão = 1 linha por mês (`ano_mes_key`): postagens, coletas, quiz, pontuação, engajamento, adoção por
# MAGIC condomínios e acumulado de usuários engajados.

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('kpi_mensal')} AS
WITH meses AS (
    SELECT DISTINCT ano * 100 + mes AS ano_mes_key, ano, mes
    FROM {gold_table('dim_tempo')}
),
postagens_status AS (
    SELECT f.*, d.status_postagem AS status_txt, t.ano * 100 + t.mes AS ano_mes_key
    FROM {gold_table('fato_postagens')} f
    JOIN {gold_table('dim_status_postagem')} d ON d.status_postagem_key = f.status_postagem_key
    JOIN {gold_table('dim_tempo')} t ON t.data_key = f.data_key
),
postagens AS (
    SELECT
        ano_mes_key,
        COUNT(*) AS total_postagens,
        COUNT(CASE WHEN lower(trim(status_txt)) = 'aprovada' THEN 1 END) AS postagens_aprovadas,
        COUNT(CASE WHEN lower(trim(status_txt)) = 'reprovada' THEN 1 END) AS postagens_reprovadas,
        COUNT(CASE WHEN lower(trim(status_txt)) = 'em_analise' THEN 1 END) AS postagens_em_analise,
        COUNT(DISTINCT usuario_key) AS usuarios_com_postagem
    FROM postagens_status GROUP BY ano_mes_key
),
coletas AS (
    SELECT
        t.ano * 100 + t.mes AS ano_mes_key,
        COUNT(*) AS total_coletas,
        COUNT(CASE WHEN c.foi_realizada THEN 1 END) AS coletas_realizadas,
        COUNT(CASE WHEN NOT c.foi_realizada THEN 1 END) AS coletas_nao_realizadas
    FROM {gold_table('fato_coletas')} c
    JOIN {gold_table('dim_tempo')} t ON t.data_key = c.data_key
    GROUP BY t.ano * 100 + t.mes
),
quiz AS (
    SELECT
        t.ano * 100 + t.mes AS ano_mes_key,
        COUNT(*) AS total_quiz_aprovados,
        COUNT(DISTINCT p.usuario_key) AS usuarios_com_quiz
    FROM {gold_table('fato_pontuacao')} p
    JOIN {gold_table('dim_tempo')} t ON t.data_key = p.data_key
    WHERE p.tipo_evento_referencia = 'quiz_aprovado'
    GROUP BY t.ano * 100 + t.mes
),
pontuacao AS (
    SELECT
        t.ano * 100 + t.mes AS ano_mes_key,
        COUNT(*) AS total_eventos_pontuacao_referencia,
        COALESCE(SUM(p.pontos_teoricos_configuracao_atual), 0) AS pontos_referencia,
        COALESCE(SUM(p.pontos_disponiveis_referencia_atual), 0) AS pontos_disponiveis_referencia_atual,
        COALESCE(SUM(p.pontos_recompensa_configuracao_atual), 0) AS pontos_recompensa_configuracao_atual
    FROM {gold_table('fato_pontuacao')} p
    JOIN {gold_table('dim_tempo')} t ON t.data_key = p.data_key
    GROUP BY t.ano * 100 + t.mes
),
eventos_engajamento AS (
    SELECT usuario_key, data_key FROM {gold_table('fato_postagens')} WHERE usuario_key IS NOT NULL
    UNION ALL
    SELECT usuario_key, data_key FROM {gold_table('fato_pontuacao')}
    WHERE tipo_evento_referencia = 'quiz_aprovado' AND usuario_key IS NOT NULL
),
engajados_mes AS (
    SELECT DISTINCT t.ano * 100 + t.mes AS ano_mes_key, e.usuario_key
    FROM eventos_engajamento e
    JOIN {gold_table('dim_tempo')} t ON t.data_key = e.data_key
),
engajamento AS (
    SELECT
        em.ano_mes_key,
        COUNT(*) AS usuarios_engajados,
        COUNT(CASE WHEN u.ativo THEN 1 END) AS usuarios_ativos
    FROM engajados_mes em
    JOIN {gold_table('dim_usuario')} u ON u.usuario_key = em.usuario_key
    GROUP BY em.ano_mes_key
),
condominios_engajados_mes AS (
    SELECT DISTINCT t.ano * 100 + t.mes AS ano_mes_key, f.condominio_key
    FROM {gold_table('fato_postagens')} f
    JOIN {gold_table('dim_tempo')} t ON t.data_key = f.data_key
    WHERE f.condominio_key IS NOT NULL
),
adocao_condominios_mes AS (
    SELECT ano_mes_key, COUNT(DISTINCT condominio_key) AS condominios_com_engajamento
    FROM condominios_engajados_mes GROUP BY ano_mes_key
),
primeiro_engajamento AS (
    SELECT e.usuario_key, MIN(t.ano * 100 + t.mes) AS mes_primeiro_engajamento
    FROM eventos_engajamento e
    JOIN {gold_table('dim_tempo')} t ON t.data_key = e.data_key
    GROUP BY e.usuario_key
),
novos_por_mes AS (
    SELECT mes_primeiro_engajamento AS ano_mes_key, COUNT(*) AS usuarios_engajados_novos
    FROM primeiro_engajamento GROUP BY mes_primeiro_engajamento
),
_total_condominios AS (
    SELECT COUNT(*) AS n FROM {gold_table('dim_condominio')}
)
SELECT
    meses.ano_mes_key,
    meses.ano,
    meses.mes,
    format_string('%04d-%02d', meses.ano, meses.mes) AS ano_mes,
    COALESCE(postagens.total_postagens, 0) AS total_postagens,
    COALESCE(postagens.postagens_aprovadas, 0) AS postagens_aprovadas,
    COALESCE(postagens.postagens_reprovadas, 0) AS postagens_reprovadas,
    COALESCE(postagens.postagens_em_analise, 0) AS postagens_em_analise,
    COALESCE(postagens.usuarios_com_postagem, 0) AS usuarios_com_postagem,
    ROUND(100.0 * COALESCE(postagens.postagens_aprovadas, 0) / NULLIF(postagens.total_postagens, 0), 2)
        AS taxa_aprovacao_postagens,
    ROUND(COALESCE(postagens.total_postagens, 0) / NULLIF(postagens.usuarios_com_postagem, 0), 2)
        AS media_postagens_por_usuario,
    COALESCE(coletas.total_coletas, 0) AS total_coletas,
    COALESCE(coletas.coletas_realizadas, 0) AS coletas_realizadas,
    COALESCE(coletas.coletas_nao_realizadas, 0) AS coletas_nao_realizadas,
    ROUND(100.0 * COALESCE(coletas.coletas_realizadas, 0) / NULLIF(coletas.total_coletas, 0), 2)
        AS taxa_realizacao_coletas,
    COALESCE(quiz.total_quiz_aprovados, 0) AS total_quiz_aprovados,
    COALESCE(quiz.usuarios_com_quiz, 0) AS usuarios_com_quiz,
    COALESCE(pontuacao.total_eventos_pontuacao_referencia, 0) AS total_eventos_pontuacao_referencia,
    COALESCE(pontuacao.pontos_referencia, 0) AS pontos_referencia,
    COALESCE(pontuacao.pontos_disponiveis_referencia_atual, 0) AS pontos_disponiveis_referencia_atual,
    COALESCE(pontuacao.pontos_recompensa_configuracao_atual, 0) AS pontos_recompensa_configuracao_atual,
    COALESCE(engajamento.usuarios_engajados, 0) AS usuarios_engajados,
    COALESCE(engajamento.usuarios_ativos, 0) AS usuarios_ativos,
    ROUND(100.0 * COALESCE(adocao_condominios_mes.condominios_com_engajamento, 0) / NULLIF((SELECT n FROM _total_condominios), 0), 2)
        AS pct_condominios_com_engajamento,
    COALESCE(novos_por_mes.usuarios_engajados_novos, 0) AS usuarios_engajados_novos,
    SUM(COALESCE(novos_por_mes.usuarios_engajados_novos, 0))
        OVER (ORDER BY meses.ano_mes_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS usuarios_engajados_acumulado
FROM meses
LEFT JOIN postagens ON postagens.ano_mes_key = meses.ano_mes_key
LEFT JOIN coletas ON coletas.ano_mes_key = meses.ano_mes_key
LEFT JOIN quiz ON quiz.ano_mes_key = meses.ano_mes_key
LEFT JOIN pontuacao ON pontuacao.ano_mes_key = meses.ano_mes_key
LEFT JOIN engajamento ON engajamento.ano_mes_key = meses.ano_mes_key
LEFT JOIN adocao_condominios_mes ON adocao_condominios_mes.ano_mes_key = meses.ano_mes_key
LEFT JOIN novos_por_mes ON novos_por_mes.ano_mes_key = meses.ano_mes_key
ORDER BY meses.ano_mes_key
""")

_kpi_mensal_rows = scalar(f"SELECT COUNT(*) FROM {gold_table('kpi_mensal')}")
_expected_months = scalar(f"SELECT COUNT(DISTINCT ano * 100 + mes) FROM {gold_table('dim_tempo')}")
if _kpi_mensal_rows != _expected_months:
    raise RuntimeError(
        f"kpi_mensal: quantidade de meses divergente da dim_tempo. "
        f"esperado={_expected_months}, obtido={_kpi_mensal_rows}."
    )
log_execucao("GOLD", "kpi_mensal", _kpi_mensal_rows)
print("KPIs OK: kpi_condominio_mensal, kpi_categoria_mensal, kpi_mensal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 09 — Encerramento do run

# COMMAND ----------

_duracao_s = (datetime.now(timezone.utc) - RUN_STARTED_AT).total_seconds()
log_execucao("PIPELINE", PIPELINE_NAME, 0, status="SUCCEEDED")
print(f"Duração do run: {_duracao_s:.0f}s")
print(f"Pipeline concluído. run_id={RUN_ID}")
print(f"Bronze: {BRONZE_COUNTS}")
print(f"Silver: {SILVER_COUNTS}")
print(f"Gold: {GOLD_COUNTS}")
print(f"Log de execução: {RUN_LOG_TABLE}")
print(f"Log de qualidade: {DQ_LOG_TABLE}")