# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente — 00 — Configuração compartilhada

# COMMAND ----------

# MAGIC %md
# MAGIC # EcoCiente — Pipeline de Engenharia de Dados
# MAGIC
# MAGIC **Arquitetura:** PostgreSQL operacional (OLTP puro) → Bronze → Silver → Gold → KPIs.
# MAGIC
# MAGIC **Modelo dimensional (dois caminhos):** `fato_postagens`, `fato_coletas` e as dimensões
# MAGIC correspondentes vêm de cópias do Data Mart PostgreSQL já existente (sem executar procedures); quiz, pontuação e
# MAGIC `dim_curso` são calculados aqui a partir da Silver. Detalhes na seção 06.
# MAGIC
# MAGIC O DDL PostgreSQL oficial é a fonte da verdade para tabelas e colunas do OLTP. O contrato de
# MAGIC origem usado aqui cobre as 22 tabelas efetivamente consumidas pelo Bronze/Silver deste
# MAGIC notebook (`SOURCE_TABLES`), não as 46 tabelas do banco operacional completo.
# MAGIC
# MAGIC **Estilo de código:** a lógica de negócio (limpeza, tipagem, dimensional modeling, KPIs) é
# MAGIC escrita em SQL puro (`spark.sql(...)`). Python cuida apenas de configuração, conexão JDBC,
# MAGIC orquestração entre tabelas e chamadas de validação/log.
# MAGIC
# MAGIC **Gravação da Gold:** direto por `CREATE OR REPLACE TABLE ... AS SELECT`, sem staging
# MAGIC nem ponteiro de promoção. A suíte de testes (seção 07) roda **antes** de qualquer KPI ser
# MAGIC calculado sobre a Gold.
# MAGIC
# MAGIC #

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01a — Imports e metadados do run
# MAGIC
# MAGIC Python aqui serve só para orquestração: identificar o run, importar bibliotecas e criar
# MAGIC `spark`/`dbutils` de forma defensiva quando o notebook roda fora do runtime interativo padrão.

# COMMAND ----------

from datetime import datetime, timezone
import uuid

try:
    spark  # type: ignore[used-before-def]  # noqa: F821
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

try:
    dbutils  # type: ignore[used-before-def]  # noqa: F821
except NameError:
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except Exception as _dbutils_exc:  # pragma: no cover
        raise RuntimeError(
            "dbutils indisponível: anexe o notebook a um cluster Databricks."
        ) from _dbutils_exc

PIPELINE_NAME = "ecociente_pipeline_engenharia_dados"
RUN_STARTED_AT = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01b — Configuração de schemas e widgets

# COMMAND ----------

def _widget_value(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)

try:
    DEFAULT_CATALOG = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]
except Exception:
    DEFAULT_CATALOG = "hive_metastore"

SOURCE_SCHEMA = _widget_value("source_schema", "public")
TARGET_CATALOG = _widget_value("target_catalog", "workspace")
BRONZE_SCHEMA = _widget_value("bronze_schema", "ecociente_bronze")
SILVER_SCHEMA = _widget_value("silver_schema", "ecociente_silver")
GOLD_SCHEMA = _widget_value("gold_schema", "ecociente_gold")
QUALITY_SCHEMA = _widget_value("quality_schema", "ecociente_quality")
TIMEZONE = _widget_value("timezone", "America/Sao_Paulo")

spark.conf.set("spark.sql.session.timeZone", TIMEZONE)

import re
for _identifier in (SOURCE_SCHEMA, TARGET_CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, QUALITY_SCHEMA):
    if not re.fullmatch(r"[A-Za-z0-9_]+", _identifier):
        raise ValueError("Catálogo/schema inválido; use letras, números e underscore.")

for schema in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, QUALITY_SCHEMA):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{schema}")

def bronze_table(table: str) -> str:
    return f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.{table}"

def silver_table(table: str) -> str:
    return f"{TARGET_CATALOG}.{SILVER_SCHEMA}.{table}"

def gold_table(table: str) -> str:
    return f"{TARGET_CATALOG}.{GOLD_SCHEMA}.{table}"

# Identificador do run. No Job, passe o parâmetro run_id = {{job.run_id}}: o valor casa com o
# histórico de execuções do job e com o run_id gravado nos logs. Sem parâmetro (execução
# manual), gera um uuid novo.
RUN_ID = _widget_value("run_id", "").strip() or str(uuid.uuid4())
if not re.fullmatch(r"[A-Za-z0-9_-]+", RUN_ID):
    raise ValueError("run_id inválido.")
print(f"run_id={RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conexão de leitura
# MAGIC Usa a conexão Unity Catalog `aiven_ecociente_jdbc` do workspace pessoal.
# MAGIC Ela deve apontar para o banco **ecociente**, com as credenciais e o JAR já configurados.
# MAGIC Não depende dos widgets `jdbc_password` nem de um Secret Scope do Aiven.

# COMMAND ----------

import re
JDBC_CONNECTION = _widget_value("jdbc_connection", "aiven_ecociente_jdbc").strip()
if not re.fullmatch(r"[A-Za-z0-9_-]+", JDBC_CONNECTION):
    raise ValueError("jdbc_connection inválido: use o nome da conexão do workspace pessoal.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01d — Leitura JDBC e helpers de execução
# MAGIC `read_postgres_query` e `read_postgres_table` usam a conexão Unity Catalog configurada.
# MAGIC As escritas e transformações ficam no Databricks. A Gold lê as cópias locais do datamart.

# COMMAND ----------

import time

def _aiven_retry(action, operation: str):
    # Só repete falhas por falta temporária de vagas, sem encerrar sessões de terceiros.
    for attempt in range(3):
        try:
            return action()
        except Exception as exc:
            message = str(exc).lower()
            busy = any(text in message for text in (
                "remaining connection slots", "too many clients", "too many connections",
            ))
            if not busy:
                raise
            if attempt == 2:
                raise RuntimeError(
                    f"Aiven sem vagas de conexão durante {operation}. "
                    "As cópias concluídas no Databricks foram preservadas. "
                    "Feche seus clientes de teste e repita a tarefa com o mesmo run_id."
                ) from None
            time.sleep(5 * (attempt + 1))

def read_postgres_query(query: str):
    return (spark.read.format("jdbc")
            .option("databricks.connection", JDBC_CONNECTION)
            .option("dbtable", f"({query.strip().rstrip(';')}) AS ecociente_src")
            .option("numPartitions", "1")
            .load())

def read_postgres_table(table: str):
    if not re.fullmatch(r"[A-Za-z0-9_]+", table):
        raise ValueError("Nome de tabela de origem inválido.")
    return read_postgres_query(f'SELECT * FROM "{SOURCE_SCHEMA}"."{table}"')

def run_sql(sql: str):
    return spark.sql(sql)

def scalar(sql: str):
    return run_sql(sql).first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01e — Log de execução do pipeline
# MAGIC
# MAGIC Substitui o lineage criptográfico por linha (hash composto, hashes de lookup) por um
# MAGIC controle leve: `run_id`, contagem de linhas e timestamp de execução por camada/tabela.
# MAGIC Suficiente para responder "quando" e "quantas linhas" cada tabela recebeu, sem carregar
# MAGIC o modelo com hashing determinístico que ninguém neste projeto consulta.

# COMMAND ----------

RUN_LOG_TABLE = f"{TARGET_CATALOG}.{QUALITY_SCHEMA}.pipeline_execution_log"

run_sql(f"""
CREATE TABLE IF NOT EXISTS {RUN_LOG_TABLE} (
    run_id STRING,
    pipeline_name STRING,
    camada STRING,
    tabela STRING,
    linhas BIGINT,
    status STRING,
    executado_em TIMESTAMP
) USING DELTA
""")

def log_execucao(camada: str, tabela: str, linhas: int, status: str = "OK") -> None:
    run_sql(f"""
        INSERT INTO {RUN_LOG_TABLE}
        SELECT
            '{RUN_ID}' AS run_id,
            '{PIPELINE_NAME}' AS pipeline_name,
            '{camada}' AS camada,
            '{tabela}' AS tabela,
            {int(linhas)} AS linhas,
            '{status}' AS status,
            current_timestamp() AS executado_em
    """)

DQ_LOG_TABLE = f"{TARGET_CATALOG}.{QUALITY_SCHEMA}.data_quality_log"

run_sql(f"""
CREATE TABLE IF NOT EXISTS {DQ_LOG_TABLE} (
    run_id STRING,
    tabela STRING,
    total_linhas BIGINT,
    linhas_invalidas BIGINT,
    percentual_invalidas DOUBLE,
    executado_em TIMESTAMP
) USING DELTA
""")

def log_qualidade(tabela: str, total: int, invalidas: int) -> None:
    pct = 0.0 if total == 0 else round(100.0 * invalidas / total, 4)
    run_sql(f"""
        INSERT INTO {DQ_LOG_TABLE}
        SELECT
            '{RUN_ID}' AS run_id,
            '{tabela}' AS tabela,
            {int(total)} AS total_linhas,
            {int(invalidas)} AS linhas_invalidas,
            {pct} AS percentual_invalidas,
            current_timestamp() AS executado_em
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02a — Contrato de origem (DDL essencial)
# MAGIC
# MAGIC O manifesto completo do DDL (46 tabelas, checks, defaults, identity, UNIQUE) tinha ~3900
# MAGIC linhas e não era usado em sua maior parte — o pipeline só consome 22 tabelas. O contrato
# MAGIC abaixo cobre exatamente essas 22: tipo de coluna, nulabilidade, PK e FKs. É o suficiente
# MAGIC para a auditoria estrutural (célula 02b) e para as regras de qualidade mantidas (célula 04).

# COMMAND ----------

SOURCE_TABLES = [
    "tb_lkp_tipos_usuarios",
    "tb_usuarios",
    "tb_enderecos",
    "tb_sindicos",
    "tb_lkp_tipos_condominios",
    "tb_condominios",
    "tb_torres",
    "tb_cooperativas",
    "tb_lkp_categorias_residuos",
    "tb_lkp_status_validacoes_postagens",
    "tb_postagens",
    "tb_lkp_status_agendamentos",
    "tb_agendamentos_coletas",
    "tb_visitas_coletas",
    "tb_avaliacoes_visitas_coletas",
    "tb_cursos",
    "tb_aulas",
    "tb_quizzes",
    "tb_tentativas_quiz",
    "tb_perguntas_quiz",
    "tb_alternativas_quiz",
    "tb_rel_respostas_tentativas_quiz",
]

# Contrato essencial de origem: por tabela, colunas esperadas (tipo Postgres + nulabilidade),
# chave primária (sempre 1 coluna neste subconjunto) e FKs. Sem checks/defaults/identity/unique
# detalhados — não são necessários para a auditoria estrutural nem para as validações de DQ
# mantidas (grão único, FK sem órfã, NOT NULL). Ver ddl_manifest completo no repositório do
# projeto se for preciso auditar as 46 tabelas do banco operacional.
DDL_CONTRACT = {
    "tb_lkp_tipos_usuarios": {
        "pk": "id_tipo_usuario",
        "columns": {"descricao": ("character varying", True), "id_tipo_usuario": ("integer", False), "nome_tipo": ("character varying", False)},
        "fks": [],
    },
    "tb_usuarios": {
        "pk": "id_usuario",
        "columns": {"ativo": ("boolean", False), "cpf": ("character varying", True), "data_nascimento": ("date", True), "email_usuario": ("character varying", False), "id_usuario": ("integer", False), "nome_usuario": ("character varying", False), "registro_em": ("timestamp with time zone", False), "senha_hash": ("character varying", False), "tipo_usuario_id": ("integer", False), "url_avatar": ("character varying", True)},
        "fks": [("tipo_usuario_id", "tb_lkp_tipos_usuarios", "id_tipo_usuario")],
    },
    "tb_enderecos": {
        "pk": "id_endereco",
        "columns": {"cep": ("character varying", True), "cidade": ("character varying", True), "complemento": ("character varying", True), "estado": ("character", True), "id_endereco": ("integer", False), "logradouro": ("character varying", True), "numero": ("character varying", True)},
        "fks": [],
    },
    "tb_sindicos": {
        "pk": "id_sindico",
        "columns": {"id_sindico": ("integer", False), "usuario_id": ("integer", False)},
        "fks": [("usuario_id", "tb_usuarios", "id_usuario")],
    },
    "tb_lkp_tipos_condominios": {
        "pk": "id_tipo_condominio",
        "columns": {"descricao": ("character varying", True), "id_tipo_condominio": ("integer", False), "nome_tipo": ("character varying", False)},
        "fks": [],
    },
    "tb_condominios": {
        "pk": "id_condominio",
        "columns": {"ativo": ("boolean", False), "cnpj": ("character varying", True), "codigo_acesso": ("character varying", True), "endereco_id": ("integer", True), "id_condominio": ("integer", False), "nome_condominio": ("character varying", False), "sindico_id": ("integer", True), "tipo_condominio_id": ("integer", False)},
        "fks": [("tipo_condominio_id", "tb_lkp_tipos_condominios", "id_tipo_condominio"), ("sindico_id", "tb_sindicos", "id_sindico"), ("endereco_id", "tb_enderecos", "id_endereco")],
    },
    "tb_torres": {
        "pk": "id_torre",
        "columns": {"condominio_id": ("integer", False), "id_torre": ("integer", False), "nome_torre": ("character varying", False)},
        "fks": [("condominio_id", "tb_condominios", "id_condominio")],
    },
    "tb_cooperativas": {
        "pk": "id_cooperativa",
        "columns": {"cnpj_cooperativa": ("character varying", False), "data_cadastro": ("timestamp with time zone", False), "email_cooperativa": ("character varying", True), "endereco_id": ("integer", True), "id_cooperativa": ("integer", False), "nome_cooperativa": ("character varying", False), "telefone_cooperativa": ("character varying", True), "usuario_id": ("integer", True)},
        "fks": [("usuario_id", "tb_usuarios", "id_usuario"), ("endereco_id", "tb_enderecos", "id_endereco")],
    },
    "tb_lkp_categorias_residuos": {
        "pk": "id_categoria",
        "columns": {"cor_identificacao": ("character varying", True), "descricao_material": ("character varying", True), "id_categoria": ("integer", False), "limite_pontos_diario": ("integer", True), "nome_categoria": ("character varying", False), "permite_reciclagem": ("boolean", False), "pontos_base": ("integer", False)},
        "fks": [],
    },
    "tb_lkp_status_validacoes_postagens": {
        "pk": "id_status_validacao",
        "columns": {"descricao": ("character varying", True), "id_status_validacao": ("integer", False), "nome_status": ("character varying", False)},
        "fks": [],
    },
    "tb_postagens": {
        "pk": "id_postagem",
        "columns": {"capturada_em": ("timestamp with time zone", False), "categoria_id": ("integer", False), "condominio_id": ("integer", False), "data_limite_analise": ("timestamp with time zone", False), "data_postagem": ("timestamp with time zone", False), "hash_foto": ("character varying", False), "id_postagem": ("integer", False), "resolvido_em": ("timestamp with time zone", True), "saldo_confianca": ("integer", False), "status_validacao_id": ("integer", False), "torre_id": ("integer", True), "triagem_automatica_aprovada": ("boolean", True), "triagem_automatica_confianca": ("numeric", True), "url_foto": ("character varying", False), "usuario_id": ("integer", False)},
        "fks": [("usuario_id", "tb_usuarios", "id_usuario"), ("condominio_id", "tb_condominios", "id_condominio"), ("torre_id", "tb_torres", "id_torre"), ("categoria_id", "tb_lkp_categorias_residuos", "id_categoria"), ("status_validacao_id", "tb_lkp_status_validacoes_postagens", "id_status_validacao")],
    },
    "tb_lkp_status_agendamentos": {
        "pk": "id_status",
        "columns": {"id_status": ("integer", False), "nome_status": ("character varying", False)},
        "fks": [],
    },
    "tb_agendamentos_coletas": {
        "pk": "id_agendamento_coleta",
        "columns": {"condominio_id": ("integer", False), "cooperativa_id": ("integer", False), "data_fim": ("timestamp with time zone", True), "data_inicio": ("timestamp with time zone", False), "id_agendamento_coleta": ("integer", False), "possui_recorrencia": ("boolean", False), "status_agendamento_id": ("integer", False)},
        "fks": [("condominio_id", "tb_condominios", "id_condominio"), ("cooperativa_id", "tb_cooperativas", "id_cooperativa"), ("status_agendamento_id", "tb_lkp_status_agendamentos", "id_status")],
    },
    "tb_visitas_coletas": {
        "pk": "id_visita_coleta",
        "columns": {"agendamento_coleta_id": ("integer", False), "confirmado_em": ("timestamp with time zone", True), "data_visita": ("timestamp with time zone", False), "foi_realizada": ("boolean", False), "houve_confirmacao": ("boolean", False), "id_visita_coleta": ("integer", False), "observacao": ("text", True)},
        "fks": [("agendamento_coleta_id", "tb_agendamentos_coletas", "id_agendamento_coleta")],
    },
    "tb_avaliacoes_visitas_coletas": {
        "pk": "id_avaliacao",
        "columns": {"avaliado_em": ("timestamp with time zone", False), "comentario": ("character varying", True), "id_avaliacao": ("integer", False), "nota": ("smallint", False), "usuario_avaliador_id": ("integer", False), "visita_coleta_id": ("integer", False)},
        "fks": [("visita_coleta_id", "tb_visitas_coletas", "id_visita_coleta"), ("usuario_avaliador_id", "tb_usuarios", "id_usuario")],
    },
    "tb_cursos": {
        "pk": "id_curso",
        "columns": {"descricao_curso": ("text", True), "esta_ativo": ("boolean", False), "id_curso": ("integer", False), "titulo_curso": ("character varying", False)},
        "fks": [],
    },
    "tb_aulas": {
        "pk": "id_aula",
        "columns": {"conteudo_aula": ("text", True), "curso_id": ("integer", False), "id_aula": ("integer", False), "ordem": ("integer", False), "titulo_aula": ("character varying", False)},
        "fks": [("curso_id", "tb_cursos", "id_curso")],
    },
   "tb_quizzes": {
    "pk": "id_quiz",
    "columns": {
        "ativo": ("boolean", False),
        "aula_id": ("integer", False),
        "curso_id": ("integer", False),
        "criado_em": ("timestamp with time zone", False),
        "id_quiz": ("integer", False),
        "nota_minima_aprovacao": ("numeric", False),
        "pontos_recompensa": ("integer", False),
        "titulo_quiz": ("character varying", False),
    },
    "fks": [("aula_id", "tb_aulas", "id_aula"), ("curso_id", "tb_cursos", "id_curso")],
},
    "tb_tentativas_quiz": {
        "pk": "id_tentativa",
        "columns": {"aprovado": ("boolean", False), "concluido_em": ("timestamp with time zone", True), "condominio_id": ("integer", True), "id_tentativa": ("integer", False), "iniciado_em": ("timestamp with time zone", False), "nota": ("numeric", True), "quiz_id": ("integer", False), "torre_id": ("integer", True), "usuario_id": ("integer", False)},
        "fks": [("usuario_id", "tb_usuarios", "id_usuario"), ("quiz_id", "tb_quizzes", "id_quiz"), ("condominio_id", "tb_condominios", "id_condominio"), ("torre_id", "tb_torres", "id_torre")],
    },
    "tb_perguntas_quiz": {
        "pk": "id_pergunta",
        "columns": {"enunciado": ("text", False), "id_pergunta": ("integer", False), "ordem": ("integer", False), "quiz_id": ("integer", False)},
        "fks": [("quiz_id", "tb_quizzes", "id_quiz")],
    },
    "tb_alternativas_quiz": {
        "pk": "id_alternativa",
        "columns": {"correta": ("boolean", False), "id_alternativa": ("integer", False), "pergunta_id": ("integer", False), "texto_alternativa": ("character varying", False)},
        "fks": [("pergunta_id", "tb_perguntas_quiz", "id_pergunta")],
    },
    "tb_rel_respostas_tentativas_quiz": {
        "pk": "id_resposta",
        "columns": {"alternativa_escolhida_id": ("integer", False), "correta": ("boolean", False), "id_resposta": ("integer", False), "pergunta_id": ("integer", False), "tentativa_id": ("integer", False)},
        "fks": [("tentativa_id", "tb_tentativas_quiz", "id_tentativa"), ("pergunta_id", "tb_perguntas_quiz", "id_pergunta"), ("alternativa_escolhida_id", "tb_alternativas_quiz", "id_alternativa")],
    },
}

_missing_contract_sources = sorted(set(SOURCE_TABLES).difference(DDL_CONTRACT))
if _missing_contract_sources:
    raise RuntimeError(f"Fonte declarada sem contrato: {_missing_contract_sources}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagens por camada (entre notebooks)
# MAGIC
# MAGIC Cada notebook roda como uma tarefa separada do Job e **não herda variáveis Python** dos
# MAGIC anteriores. Onde a versão em um notebook só usava `BRONZE_COUNTS`, `SILVER_COUNTS` ou
# MAGIC `GOLD_COUNTS`, agora cada notebook relê as contagens direto das tabelas Delta.

# COMMAND ----------

def contar_tabelas(nome_fn, tabelas) -> dict:
    return {t: scalar(f"SELECT COUNT(*) FROM {nome_fn(t)}") for t in tabelas}

def contar_schema(schema: str) -> dict:
    nomes = [r["tableName"] for r in run_sql(f"SHOW TABLES IN {TARGET_CATALOG}.{schema}").collect()]
    return {n: scalar(f"SELECT COUNT(*) FROM {TARGET_CATALOG}.{schema}.{n}") for n in sorted(nomes)}