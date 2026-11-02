# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente | 04 — Gold compatível com o datamart Neon auditado
# MAGIC Usa `%run ./00_config`. Mantém a conexão JDBC já configurada e as cópias locais por run_id.
# MAGIC Não executa a procedure do Neon: seu código foi utilizado somente como referência de mapeamento.
# MAGIC
# MAGIC - `registro_em`: usuários da Silver, por `id_usuario`.
# MAGIC - `ativo` da cooperativa: usuário associado, mantendo desconhecidos como NULL.
# MAGIC - Descrição e cor da categoria: lookup da Silver, por `id_categoria`.
# MAGIC - `limite_pontos_diario`: alias de `limite_pontos_ciclo`, cuja origem foi comprovada pela procedure.
# MAGIC - Nome do status: `codigo`, preenchido com `nome_status` pela procedure.
# MAGIC - Pontos teóricos da postagem: `pontos_base_categoria`, inclusive em postagens não aprovadas.
# MAGIC   Esse campo não comprova crédito. A lógica separada de `fato_pontuacao` permanece intacta.
# MAGIC
# MAGIC Confere as colunas das nove cópias do datamart e das 22 tabelas Silver antes das escritas Gold.
# MAGIC Valida a unicidade das chaves da Silver usadas para complementar dimensões.
# MAGIC JSON, testes, KPIs e envio GitHub dos notebooks seguintes não foram editados.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Bronze já foi gravada pelo notebook 02: relê as contagens da própria tabela.
BRONZE_COUNTS = contar_tabelas(bronze_table, SOURCE_TABLES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06 — Gold | modelo dimensional
# MAGIC
# MAGIC **Fonte de verdade do modelo dimensional: o Data Mart PostgreSQL** (`ecociente_datamart.sql`),
# MAGIC não este notebook. `sp_carga_datamart()` calcula `dim_tempo`, `dim_usuario`,
# MAGIC `dim_condominio`, `dim_cooperativa`, `dim_categoria_residuo`, `dim_status_postagem`,
# MAGIC `dim_status_agendamento`, `fato_postagens` e `fato_coletas` direto do OLTP. Este notebook
# MAGIC **lê** essas 9 tabelas prontas (mesma conexão JDBC da célula 01d — elas vivem no schema
# MAGIC `public`, igual as tabelas OLTP) e resolve as surrogate keys (`sk_*`) de volta pra chave
# MAGIC natural (`usuario_key = id_usuario` etc.), pra manter a mesma convenção usada no resto do
# MAGIC notebook — incluindo `fato_pontuacao`/`dim_curso`/`fato_tentativas_quiz`, que **continuam
# MAGIC calculados aqui**, porque o Postgres não modela quiz.
# MAGIC
# MAGIC **Pré-requisito:** rodar `ecociente_datamart.sql` (que já chama `sp_carga_datamart()` no
# MAGIC final) antes deste notebook — a célula seguinte falha com erro explícito se as tabelas do
# MAGIC Data Mart estiverem vazias.
# MAGIC
# MAGIC **Colunas perdidas** por vir do Postgres em vez de ser calculado aqui: `fato_postagens`
# MAGIC fica sem `torre_key`. Nada em `07`/`08` depende dela.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura do Data Mart Postgres

# COMMAND ----------

from pyspark.sql import functions as F

def read_datamart_table(table: str) -> int:
    destination = bronze_table("_datamart_" + table)
    reusable = False
    if spark.catalog.tableExists(destination):
        local = spark.table(destination)
        if "_pipeline_run_id" in local.columns:
            ids = local.select("_pipeline_run_id").distinct().limit(2).collect()
            reusable = len(ids) == 1 and ids[0]["_pipeline_run_id"] == RUN_ID
    if not reusable:
        def copy_once():
            (read_postgres_table(table)
             .withColumn("_pipeline_run_id", F.lit(RUN_ID))
             .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destination))
        _aiven_retry(copy_once, f"cópia do datamart: {table}")
    local = spark.table(destination)
    local.createOrReplaceTempView(f"pg_{table}")
    count = local.count()
    print(f"{'Reutilizado do Databricks' if reusable else 'Cópia do datamart'}: {table} = {count} linhas")
    return count

DATAMART_SOURCE_TABLES = [
    "dim_tempo", "dim_usuario", "dim_condominio", "dim_cooperativa",
    "dim_categoria_residuo", "dim_status_postagem", "dim_status_agendamento",
    "fato_postagens", "fato_coletas",
]


_datamart_counts = {t: read_datamart_table(t) for t in DATAMART_SOURCE_TABLES}
_empty_datamart_tables = [t for t, c in _datamart_counts.items() if c == 0]
if _empty_datamart_tables:
    raise RuntimeError(
        "Tabelas vazias no datamart de origem: " + ", ".join(_empty_datamart_tables) +
        ". Solicite a verificação à equipe responsável pelo banco; esta versão não altera o Neon."
    )
print("Datamart disponível no Databricks:", _datamart_counts)


# Verifica todas as dependências antes de substituir qualquer tabela Gold.
COLUNAS_DATAMART_04 = {'dim_tempo': ['sk_tempo', 'data_completa', 'ano', 'trimestre', 'mes', 'nome_mes', 'dia', 'dia_semana_numero', 'nome_dia_semana', 'is_fim_de_semana'], 'dim_usuario': ['sk_usuario', 'id_usuario', 'tipo_usuario', 'ativo'], 'dim_condominio': ['sk_condominio', 'id_condominio', 'nome_condominio', 'tipo_condominio', 'ativo', 'cidade', 'estado'], 'dim_cooperativa': ['sk_cooperativa', 'id_cooperativa', 'nome_cooperativa', 'cidade', 'estado'], 'dim_categoria_residuo': ['sk_categoria', 'id_categoria', 'nome_categoria', 'permite_reciclagem', 'pontos_base', 'limite_pontos_ciclo'], 'dim_status_postagem': ['sk_status_postagem', 'id_status_validacao', 'codigo'], 'dim_status_agendamento': ['sk_status_agendamento', 'id_status', 'nome_status'], 'fato_postagens': ['id_postagem', 'sk_usuario', 'sk_condominio', 'sk_categoria', 'sk_status_postagem', 'sk_tempo', 'qtd_postagens', 'pontos_base_categoria'], 'fato_coletas': ['id_visita_coleta', 'sk_condominio', 'sk_cooperativa', 'sk_status_agendamento', 'sk_tempo', 'foi_realizada', 'houve_confirmacao', 'qtd_visitas']}
COLUNAS_SILVER_04 = {'tb_lkp_tipos_usuarios': ['descricao', 'id_tipo_usuario', 'nome_tipo'], 'tb_usuarios': ['ativo', 'cpf', 'data_nascimento', 'email_usuario', 'id_usuario', 'nome_usuario', 'registro_em', 'tipo_usuario_id', 'url_avatar'], 'tb_enderecos': ['cep', 'cidade', 'complemento', 'estado', 'id_endereco', 'logradouro', 'numero'], 'tb_sindicos': ['id_sindico', 'usuario_id'], 'tb_lkp_tipos_condominios': ['descricao', 'id_tipo_condominio', 'nome_tipo'], 'tb_condominios': ['ativo', 'cnpj', 'codigo_acesso', 'endereco_id', 'id_condominio', 'nome_condominio', 'sindico_id', 'tipo_condominio_id'], 'tb_torres': ['condominio_id', 'id_torre', 'nome_torre'], 'tb_cooperativas': ['cnpj_cooperativa', 'data_cadastro', 'email_cooperativa', 'endereco_id', 'id_cooperativa', 'nome_cooperativa', 'telefone_cooperativa', 'usuario_id'], 'tb_lkp_categorias_residuos': ['cor_identificacao', 'descricao_material', 'id_categoria', 'limite_pontos_diario', 'nome_categoria', 'permite_reciclagem', 'pontos_base'], 'tb_lkp_status_validacoes_postagens': ['descricao', 'id_status_validacao', 'nome_status'], 'tb_postagens': ['capturada_em', 'categoria_id', 'condominio_id', 'data_limite_analise', 'data_postagem', 'hash_foto', 'id_postagem', 'resolvido_em', 'saldo_confianca', 'status_validacao_id', 'torre_id', 'triagem_automatica_aprovada', 'triagem_automatica_confianca', 'url_foto', 'usuario_id'], 'tb_lkp_status_agendamentos': ['id_status', 'nome_status'], 'tb_agendamentos_coletas': ['condominio_id', 'cooperativa_id', 'data_fim', 'data_inicio', 'id_agendamento_coleta', 'possui_recorrencia', 'status_agendamento_id'], 'tb_visitas_coletas': ['agendamento_coleta_id', 'confirmado_em', 'data_visita', 'foi_realizada', 'houve_confirmacao', 'id_visita_coleta', 'observacao'], 'tb_avaliacoes_visitas_coletas': ['avaliado_em', 'comentario', 'id_avaliacao', 'nota', 'usuario_avaliador_id', 'visita_coleta_id'], 'tb_cursos': ['descricao_curso', 'esta_ativo', 'id_curso', 'titulo_curso'], 'tb_aulas': ['conteudo_aula', 'curso_id', 'id_aula', 'ordem', 'titulo_aula'], 'tb_quizzes': ['ativo', 'aula_id', 'curso_id', 'criado_em', 'id_quiz', 'nota_minima_aprovacao', 'pontos_recompensa', 'titulo_quiz'], 'tb_tentativas_quiz': ['aprovado', 'concluido_em', 'condominio_id', 'id_tentativa', 'iniciado_em', 'nota', 'quiz_id', 'torre_id', 'usuario_id'], 'tb_perguntas_quiz': ['enunciado', 'id_pergunta', 'ordem', 'quiz_id'], 'tb_alternativas_quiz': ['correta', 'id_alternativa', 'pergunta_id', 'texto_alternativa'], 'tb_rel_respostas_tentativas_quiz': ['alternativa_escolhida_id', 'correta', 'id_resposta', 'pergunta_id', 'tentativa_id']}

def validar_contrato_04():
    issues = []
    checks = [("pg_" + table, cols) for table, cols in COLUNAS_DATAMART_04.items()]
    checks += [(silver_table(table), cols) for table, cols in COLUNAS_SILVER_04.items()]
    for full, expected in checks:
        if not spark.catalog.tableExists(full):
            issues.append(full + ": tabela/view indisponível")
            continue
        present = set(spark.table(full).columns)
        missing = sorted(set(expected) - present)
        if missing:
            issues.append(full + ": faltam " + ", ".join(missing))
    if issues:
        raise RuntimeError("Contrato do 04 incompatível antes da gravação Gold:\n" + "\n".join(issues))

    # As complementações por LEFT JOIN devem preservar o grão do datamart.
    for table, key in (
        ("tb_usuarios", "id_usuario"),
        ("tb_cooperativas", "id_cooperativa"),
        ("tb_lkp_categorias_residuos", "id_categoria"),
    ):
        duplicates = scalar(f"""
            SELECT COUNT(*) FROM (
                SELECT {key} FROM {silver_table(table)}
                GROUP BY {key} HAVING COUNT(*) > 1
            ) AS chaves_duplicadas
        """)
        if duplicates:
            raise RuntimeError(f"{table}.{key}: duplicidades impedem complementar a Gold sem multiplicar linhas.")
    print("Contrato do 04 conferido: 9 tabelas do datamart e 22 da Silver.")

validar_contrato_04()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconciliação Data Mart × Bronze
# MAGIC
# MAGIC `fato_postagens` e `fato_coletas` do Data Mart devem ter o mesmo número de linhas que
# MAGIC `tb_postagens` e `tb_visitas_coletas` na Bronze (grão: 1 linha por postagem / por visita). Diferença
# MAGIC não interrompe o pipeline: é registrada no log com status `DIVERGENTE` e impressa. Causa comum:
# MAGIC linhas novas no OLTP entre a carga do Data Mart e a ingestão do Bronze. A diferença Bronze × Silver
# MAGIC é a quarentena do Data Quality.

# COMMAND ----------

_RECON = {
    "fato_postagens": ("tb_postagens", _datamart_counts["fato_postagens"], BRONZE_COUNTS["tb_postagens"]),
    "fato_coletas": ("tb_visitas_coletas", _datamart_counts["fato_coletas"], BRONZE_COUNTS["tb_visitas_coletas"]),
}
for _fato, (_origem, _dm, _bronze) in _RECON.items():
    _diff = abs(_dm - _bronze)
    log_execucao("RECONCILIACAO", f"{_fato}_vs_bronze_{_origem}", _diff,
                 status="OK" if _diff == 0 else "DIVERGENTE")
    print(f"{_fato}: data_mart={_dm} | bronze.{_origem}={_bronze} | "
          f"{'OK' if _diff == 0 else f'DIVERGENTE (diferença de {_diff})'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_tempo, dim_usuario, dim_condominio, dim_cooperativa
# MAGIC
# MAGIC `dia_semana_numero` do Postgres usa 0=domingo…6=sábado; convertido para 1=domingo…7=sábado
# MAGIC (convenção Spark, `dayofweek()`) já usada no resto do notebook.

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('dim_tempo')} AS
SELECT
    sk_tempo AS data_key,
    data_completa AS data,
    ano, trimestre, mes, nome_mes, dia,
    dia_semana_numero + 1 AS dia_semana,
    nome_dia_semana,
    is_fim_de_semana AS eh_fim_semana
FROM pg_dim_tempo
""")

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('dim_usuario')} AS
SELECT
    d.id_usuario AS usuario_key,
    d.tipo_usuario,
    d.ativo,
    u.registro_em
FROM pg_dim_usuario d
LEFT JOIN {silver_table('tb_usuarios')} u ON u.id_usuario = d.id_usuario
""")

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('dim_condominio')} AS
SELECT id_condominio AS condominio_key, nome_condominio, tipo_condominio, ativo, cidade, estado
FROM pg_dim_condominio
""")

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('dim_cooperativa')} AS
SELECT
    d.id_cooperativa AS cooperativa_key,
    d.nome_cooperativa,
    d.cidade,
    d.estado,
    u.ativo
FROM pg_dim_cooperativa d
LEFT JOIN {silver_table('tb_cooperativas')} c
    ON c.id_cooperativa = d.id_cooperativa
LEFT JOIN {silver_table('tb_usuarios')} u
    ON u.id_usuario = c.usuario_id
""")

for _t in ("dim_tempo", "dim_usuario", "dim_condominio", "dim_cooperativa"):
    log_execucao("GOLD", _t, scalar(f"SELECT COUNT(*) FROM {gold_table(_t)}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_categoria_residuo, dim_status_postagem, dim_status_agendamento

# COMMAND ----------

# Mapeamentos confirmados na procedure fornecida, sem executá-la:
# limite_pontos_ciclo <- tb_lkp_categorias_residuos.limite_pontos_diario;
# dim_status_postagem.codigo <- tb_lkp_status_validacoes_postagens.nome_status.
run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('dim_categoria_residuo')} AS
SELECT
    d.id_categoria AS categoria_residuo_key,
    d.nome_categoria,
    c.descricao_material,
    d.permite_reciclagem,
    c.cor_identificacao,
    d.pontos_base,
    d.limite_pontos_ciclo AS limite_pontos_diario
FROM pg_dim_categoria_residuo d
LEFT JOIN {silver_table('tb_lkp_categorias_residuos')} c
    ON c.id_categoria = d.id_categoria
""")

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('dim_status_postagem')} AS
SELECT id_status_validacao AS status_postagem_key, codigo AS status_postagem
FROM pg_dim_status_postagem
""")

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('dim_status_agendamento')} AS
SELECT id_status AS status_agendamento_key, nome_status AS status_agendamento
FROM pg_dim_status_agendamento
""")

for _t in ("dim_categoria_residuo", "dim_status_postagem", "dim_status_agendamento"):
    log_execucao("GOLD", _t, scalar(f"SELECT COUNT(*) FROM {gold_table(_t)}"))

# IDs distintos podem compartilhar o nome 'aprovada'. Os joins usam a chave,
# e os KPIs já agregam por nome normalizado. Não exclui nem unifica IDs.
_approved_status_count = scalar(f"""
    SELECT COUNT(*) FROM {gold_table('dim_status_postagem')}
    WHERE lower(trim(status_postagem)) = 'aprovada'
""")
if _approved_status_count == 0:
    raise RuntimeError("dim_status_postagem: nenhum status 'aprovada' encontrado.")
if _approved_status_count > 1:
    print(f"Status aprovada: {_approved_status_count} IDs preservados; os KPIs agregam todos por nome.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fato_postagens
# MAGIC
# MAGIC `pontos_gerados` do Postgres já é a regra "pontos_base da categoria quando aprovada, 0
# MAGIC caso contrário" aplicada — renomeado para `pontos_teoricos_configuracao_atual` só para
# MAGIC manter o nome que `fato_pontuacao`/KPIs (célula 08) já esperam.

# COMMAND ----------

# A procedure grava COALESCE(dcat.pontos_base, 0) para cada postagem,
# inclusive não aprovada. Este campo é referência teórica, não crédito confirmado;
# não multiplica por foi_aprovada, não aplica limites nem infere pontos concedidos.
run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('fato_postagens')} AS
SELECT
    f.id_postagem AS postagem_key,
    du.id_usuario AS usuario_key,
    dc.id_condominio AS condominio_key,
    dcat.id_categoria AS categoria_residuo_key,
    dsp.id_status_validacao AS status_postagem_key,
    f.sk_tempo AS data_key,
    f.qtd_postagens,
    f.pontos_base_categoria AS pontos_teoricos_configuracao_atual
FROM pg_fato_postagens f
JOIN pg_dim_usuario du                  ON du.sk_usuario = f.sk_usuario
LEFT JOIN pg_dim_condominio dc          ON dc.sk_condominio = f.sk_condominio
LEFT JOIN pg_dim_categoria_residuo dcat ON dcat.sk_categoria = f.sk_categoria
JOIN pg_dim_status_postagem dsp         ON dsp.sk_status_postagem = f.sk_status_postagem
""")

_fato_postagens_rows = scalar(f"SELECT COUNT(*) FROM {gold_table('fato_postagens')}")
if _fato_postagens_rows != _datamart_counts["fato_postagens"]:
    raise RuntimeError(
        f"fato_postagens: a resolução de sk_*→chave natural alterou o grão. "
        f"postgres={_datamart_counts['fato_postagens']}, gold={_fato_postagens_rows}"
    )
log_execucao("GOLD", "fato_postagens", _fato_postagens_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### fato_coletas

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('fato_coletas')} AS
SELECT
    f.id_visita_coleta AS visita_coleta_key,
    dc.id_condominio AS condominio_key,
    dcoop.id_cooperativa AS cooperativa_key,
    dsa.id_status AS status_agendamento_key,
    f.sk_tempo AS data_key,
    f.foi_realizada,
    f.houve_confirmacao,
    f.qtd_visitas
FROM pg_fato_coletas f
LEFT JOIN pg_dim_condominio dc          ON dc.sk_condominio = f.sk_condominio
LEFT JOIN pg_dim_cooperativa dcoop      ON dcoop.sk_cooperativa = f.sk_cooperativa
LEFT JOIN pg_dim_status_agendamento dsa ON dsa.sk_status_agendamento = f.sk_status_agendamento
""")

_fato_coletas_rows = scalar(f"SELECT COUNT(*) FROM {gold_table('fato_coletas')}")
if _fato_coletas_rows != _datamart_counts["fato_coletas"]:
    raise RuntimeError(
        f"fato_coletas: a resolução de sk_*→chave natural alterou o grão. "
        f"postgres={_datamart_counts['fato_coletas']}, gold={_fato_coletas_rows}"
    )
log_execucao("GOLD", "fato_coletas", _fato_coletas_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ### fato_tentativas_quiz
# MAGIC
# MAGIC Grão: 1 linha por tentativa, aprovada ou não, concluída ou não. Precisa existir antes de
# MAGIC `fato_pontuacao` e `fato_respostas_quiz`, que leem dela.

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('fato_tentativas_quiz')} AS
SELECT
    t.id_tentativa   AS tentativa_key,
    t.usuario_id     AS usuario_key,
    t.quiz_id        AS quiz_key,
    t.condominio_id  AS condominio_key,
    t.torre_id       AS torre_key,
    t.iniciado_em,
    t.concluido_em,
    t.nota,
    t.aprovado,
    CAST(date_format(COALESCE(t.concluido_em, t.iniciado_em), 'yyyyMMdd') AS INT) AS data_key
FROM {silver_table('tb_tentativas_quiz')} t
""")

log_execucao("GOLD", "fato_tentativas_quiz",
             scalar(f"SELECT COUNT(*) FROM {gold_table('fato_tentativas_quiz')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### fato_pontuacao
# MAGIC
# MAGIC `pontos_teoricos_configuracao_atual` e `pontos_disponiveis_referencia_atual` usam a
# MAGIC configuração atual de `pontos_base`/`limite_pontos_ciclo`, não o valor vigente quando o
# MAGIC evento aconteceu — a origem não guarda esse histórico. `credito_confirmado` é sempre
# MAGIC `FALSE`: referência analítica, não ledger de pontos efetivamente creditados.
# MAGIC
# MAGIC `pontos_quiz` lê de `fato_tentativas_quiz` já materializada, filtrando aprovado+concluído,
# MAGIC em vez de reler `tb_tentativas_quiz` — depende da célula anterior já ter rodado.

# COMMAND ----------

if not spark.catalog.tableExists(gold_table('fato_tentativas_quiz')):
    raise RuntimeError(
        "fato_tentativas_quiz precisa existir antes desta célula — rode a célula anterior primeiro."
    )

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('fato_pontuacao')} AS
WITH postagens_aprovadas AS (
    SELECT
        p.id_postagem, p.usuario_id, p.condominio_id, p.torre_id, p.categoria_id,
        p.data_postagem,
        c.pontos_base AS pontos_base_categoria,
        c.limite_pontos_diario AS limite_pontos_ciclo_categoria,
        trunc(p.data_postagem, 'MM') AS mes_referencia
    FROM {silver_table('tb_postagens')} p
    JOIN {silver_table('tb_lkp_status_validacoes_postagens')} s
        ON s.id_status_validacao = p.status_validacao_id
    JOIN {silver_table('tb_lkp_categorias_residuos')} c ON c.id_categoria = p.categoria_id
    WHERE lower(trim(s.nome_status)) = 'aprovada'
),
postagens_com_acumulado AS (
    SELECT *,
        COALESCE(SUM(pontos_base_categoria) OVER (
            PARTITION BY usuario_id, categoria_id, mes_referencia
            ORDER BY data_postagem, id_postagem
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS pontos_base_anteriores
    FROM postagens_aprovadas
),
pontos_postagens AS (
    SELECT
        concat('postagem:', id_postagem) AS evento_referencia_key,
        'postagem_aprovada' AS tipo_evento_referencia,
        usuario_id AS usuario_key,
        condominio_id AS condominio_key,
        torre_id AS torre_key,
        categoria_id AS categoria_residuo_key,
        CAST(NULL AS INT) AS quiz_key,
        data_postagem AS data_evento,
        CAST(date_format(data_postagem, 'yyyyMMdd') AS INT) AS data_key,
        pontos_base_categoria AS pontos_teoricos_configuracao_atual,
        CASE
            WHEN limite_pontos_ciclo_categoria IS NULL THEN pontos_base_categoria
            ELSE GREATEST(0, LEAST(
                pontos_base_categoria,
                limite_pontos_ciclo_categoria - pontos_base_anteriores
            ))
        END AS pontos_disponiveis_referencia_atual,
        CAST(NULL AS INT) AS pontos_recompensa_configuracao_atual
    FROM postagens_com_acumulado
),
-- O DDL registra incremento no Redis quando o quiz é aprovado, mas o PostgreSQL não
-- comprova o crédito. Sem concluido_em não há timestamp confiável do evento.
pontos_quiz AS (
    SELECT
        concat('quiz:', ft.tentativa_key) AS evento_referencia_key,
        'quiz_aprovado' AS tipo_evento_referencia,
        ft.usuario_key,
        ft.condominio_key,
        ft.torre_key,
        CAST(NULL AS INT) AS categoria_residuo_key,
        ft.quiz_key,
        ft.concluido_em AS data_evento,
        ft.data_key,
        q.pontos_recompensa AS pontos_teoricos_configuracao_atual,
        CAST(NULL AS INT) AS pontos_disponiveis_referencia_atual,
        q.pontos_recompensa AS pontos_recompensa_configuracao_atual
    FROM {gold_table('fato_tentativas_quiz')} ft
    JOIN {silver_table('tb_quizzes')} q ON q.id_quiz = ft.quiz_key
    WHERE ft.aprovado = TRUE AND ft.concluido_em IS NOT NULL
)
SELECT *, FALSE AS credito_confirmado FROM pontos_postagens
UNION ALL
SELECT *, FALSE AS credito_confirmado FROM pontos_quiz
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_tipo_evento_pontuacao
# MAGIC
# MAGIC Extraída dos valores realmente presentes em `fato_pontuacao` — não inventa linhas para
# MAGIC tipos de evento inexistentes. Se a origem passar a emitir um tipo novo, a descrição cai no
# MAGIC próprio código (`SEM_DESCRICAO_CURADA:...`) em vez de presumir um texto.

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('dim_tipo_evento_pontuacao')} AS
SELECT
    ROW_NUMBER() OVER (ORDER BY tipo_evento_referencia) AS tipo_evento_pontuacao_key,
    tipo_evento_referencia,
    CASE tipo_evento_referencia
        WHEN 'postagem_aprovada' THEN 'Evento de pontuação por postagem de resíduo aprovada.'
        WHEN 'quiz_aprovado' THEN 'Evento de pontuação por tentativa de quiz aprovada e concluída.'
        ELSE concat('SEM_DESCRICAO_CURADA:', tipo_evento_referencia)
    END AS descricao_tipo_evento
FROM (SELECT DISTINCT tipo_evento_referencia FROM {gold_table('fato_pontuacao')})
""")

# FK tipo_evento_pontuacao_key em fato_pontuacao. A coluna textual tipo_evento_referencia
# é mantida porque os testes (célula 07) e os KPIs (célula 08) filtram por ela diretamente.
run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('fato_pontuacao')} AS
SELECT f.*, d.tipo_evento_pontuacao_key
FROM {gold_table('fato_pontuacao')} f
JOIN {gold_table('dim_tipo_evento_pontuacao')} d ON d.tipo_evento_referencia = f.tipo_evento_referencia
""")

_fato_pontuacao_rows = scalar(f"SELECT COUNT(*) FROM {gold_table('fato_pontuacao')}")
log_execucao("GOLD", "fato_pontuacao", _fato_pontuacao_rows)
log_execucao("GOLD", "dim_tipo_evento_pontuacao",
             scalar(f"SELECT COUNT(*) FROM {gold_table('dim_tipo_evento_pontuacao')}"))
print(f"fato_pontuacao: {_fato_pontuacao_rows} eventos de referência")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_curso, fato_respostas_quiz
# MAGIC
# MAGIC Grão de `fato_respostas_quiz`: 1 linha por resposta. Deriva de `fato_tentativas_quiz`
# MAGIC (usuario_key/condominio_key/torre_key/data_key), sem reler `tb_tentativas_quiz`.
# MAGIC `curso_key` resolve via `tb_quizzes.curso_id`, porque `fato_tentativas_quiz` só guarda
# MAGIC `quiz_key`.

# COMMAND ----------

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('dim_curso')} AS
SELECT
    id_curso     AS curso_key,
    titulo_curso,
    esta_ativo   AS ativo
FROM {silver_table('tb_cursos')}
""")

run_sql(f"""
CREATE OR REPLACE TABLE {gold_table('fato_respostas_quiz')} AS
SELECT
    r.id_resposta               AS resposta_key,
    ft.tentativa_key,
    ft.usuario_key,
    ft.quiz_key,
    q.curso_id                  AS curso_key,
    ft.condominio_key,
    ft.torre_key,
    r.pergunta_id                AS pergunta_key,
    r.alternativa_escolhida_id   AS alternativa_key,
    r.correta,
    ft.data_key
FROM {silver_table('tb_rel_respostas_tentativas_quiz')} r
JOIN {gold_table('fato_tentativas_quiz')} ft ON ft.tentativa_key = r.tentativa_id
JOIN {silver_table('tb_quizzes')} q ON q.id_quiz = ft.quiz_key
""")

log_execucao("GOLD", "dim_curso", scalar(f"SELECT COUNT(*) FROM {gold_table('dim_curso')}"))
log_execucao("GOLD", "fato_respostas_quiz",
             scalar(f"SELECT COUNT(*) FROM {gold_table('fato_respostas_quiz')}"))