# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente | 07 — Exporta JSON e publica no GitHub
# MAGIC Lê a camada Gold e preserva os nove blocos originais, os sete campos em `meta` e os 15 KPIs originais; acrescenta distribuições, comparativos e agregados para filtros.
# MAGIC Salva no Volume `/Volumes/workspace/ecociente_gold/exports` e mantém a publicação por API
# MAGIC no repositório `EcoCiente-Instituto-J-F/bi-ecociente-painel`, branch `main`, arquivo `data.json`.
# MAGIC
# MAGIC No Job, `github_required=true`: ausência de token gera falha, evitando sucesso sem publicação.
# MAGIC O token é lido do Secret Scope `ecociente-github`, chave `GITHUB_TOKEN`.
# MAGIC O widget `github_token` continua disponível para testes manuais; nunca envie o token ao GitHub.
# MAGIC O catálogo do exportador usa o parâmetro `catalog`; as demais tarefas usam `target_catalog`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 — Parâmetros

# COMMAND ----------

import json
import re
from datetime import date, datetime, timedelta

def _widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        value = dbutils.widgets.get(name)
    return (value or "").strip() or default

CATALOG      = _widget("catalog", "workspace")
GOLD_SCHEMA  = _widget("gold_schema", "ecociente_gold")
DATA_INICIO  = _widget("data_inicio", "")
DATA_FIM     = _widget("data_fim", "")
VOLUME_PATH  = _widget("volume_path", "/Volumes/workspace/ecociente_gold/exports").rstrip("/")
LAKEVIEW_URL = _widget("lakeview_url", "")

for _name, _value in (("catalog", CATALOG), ("gold_schema", GOLD_SCHEMA)):
    if not re.fullmatch(r"[A-Za-z0-9_]+", _value):
        raise ValueError(f"widget {_name} inválido: {_value!r}")

def g(table: str) -> str:
    return f"{CATALOG}.{GOLD_SCHEMA}.{table}"

def q(sql: str):
    return spark.sql(sql).collect()

def q1(sql: str):
    rows = q(sql)
    return rows[0].asDict() if rows else {}

def to_int(v) -> int:
    return int(v) if v is not None else 0

def to_float(v) -> float:
    return float(v) if v is not None else 0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 02 — Janela de datas
# MAGIC
# MAGIC Vazio = automático: da primeira à última data com postagem na Gold. Datas vão por
# MAGIC `date.fromisoformat`, então só entra data válida nas consultas.

# COMMAND ----------

def _iso(value: str, nome: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{nome} deve ser AAAA-MM-DD, recebi {value!r}") from exc

if DATA_INICIO and DATA_FIM:
    d_ini, d_fim = _iso(DATA_INICIO, "data_inicio"), _iso(DATA_FIM, "data_fim")
else:
    r = q1(f"""
        SELECT MIN(t.data) AS ini, MAX(t.data) AS fim
        FROM {g('fato_postagens')} f
        JOIN {g('dim_tempo')} t ON t.data_key = f.data_key
    """)
    if r.get("ini") is None:
        raise RuntimeError("fato_postagens está vazia: nada para exportar. Rode Bronze/Silver/Gold antes.")
    d_ini = _iso(DATA_INICIO, "data_inicio") if DATA_INICIO else r["ini"]
    d_fim = _iso(DATA_FIM, "data_fim") if DATA_FIM else r["fim"]

if d_ini > d_fim:
    raise ValueError(f"janela invertida: {d_ini} > {d_fim}")

INI, FIM = d_ini.isoformat(), d_fim.isoformat()
PERIODO_SQL = f"""(SELECT data_key FROM {g('dim_tempo')} WHERE data BETWEEN DATE'{INI}' AND DATE'{FIM}')"""
print(f"Janela: {INI} → {FIM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 — KPIs

# COMMAND ----------

geral = q1(f"""
WITH usuarios AS (SELECT COUNT(*) AS total FROM {g('dim_usuario')}),
comuns AS (SELECT COUNT(*) AS total FROM {g('dim_usuario')} WHERE tipo_usuario = 'Usuário Comum' AND ativo),
coops AS (SELECT COUNT(*) AS total FROM {g('dim_cooperativa')} WHERE ativo),
conds AS (
    SELECT COUNT(*) AS total,
           COUNT(CASE WHEN ativo THEN 1 END) AS ativos,
           COUNT(CASE WHEN NOT ativo THEN 1 END) AS inativos
    FROM {g('dim_condominio')}
)
SELECT usuarios.total AS usuarios_gerais, comuns.total AS usuarios_comuns_ativos,
       coops.total AS cooperativas_ativas, conds.total AS condominios,
       conds.ativos AS condominios_ativos, conds.inativos AS condominios_inativos
FROM usuarios CROSS JOIN comuns CROSS JOIN coops CROSS JOIN conds
""")

post = q1(f"""
SELECT
    COUNT(*) AS total,
    COUNT(CASE WHEN lower(trim(d.status_postagem)) = 'aprovada'   THEN 1 END) AS aprovadas,
    COUNT(CASE WHEN lower(trim(d.status_postagem)) = 'reprovada'  THEN 1 END) AS reprovadas,
    COUNT(CASE WHEN lower(trim(d.status_postagem)) = 'em_analise' THEN 1 END) AS em_analise
FROM {g('fato_postagens')} f
JOIN {g('dim_status_postagem')} d ON d.status_postagem_key = f.status_postagem_key
WHERE f.data_key IN {PERIODO_SQL}
""")

quiz = q1(f"""
SELECT
    COUNT(*) AS respondidos,
    COUNT(CASE WHEN aprovado THEN 1 END) AS aprovados,
    COUNT(CASE WHEN NOT aprovado THEN 1 END) AS reprovados,
    COUNT(nota) AS tentativas_com_nota,
    COUNT(*) - COUNT(nota) AS tentativas_sem_nota,
    ROUND(AVG(nota), 2) AS nota_media,
    ROUND(STDDEV_SAMP(nota), 2) AS nota_desvio_padrao_amostral,
    ROUND(percentile_approx(nota, 0.5, 10000), 2) AS nota_mediana
FROM {g('fato_tentativas_quiz')}
WHERE concluido_em IS NOT NULL AND data_key IN {PERIODO_SQL}
""")

KPIS = {
    "usuarios_gerais":        to_int(geral.get("usuarios_gerais")),
    "usuarios_comuns_ativos": to_int(geral.get("usuarios_comuns_ativos")),
    "cooperativas_ativas":    to_int(geral.get("cooperativas_ativas")),
    "condominios":            to_int(geral.get("condominios")),
    "condominios_ativos":     to_int(geral.get("condominios_ativos")),
    "condominios_inativos":   to_int(geral.get("condominios_inativos")),
    "postagens_total":        to_int(post.get("total")),
    "postagens_aprovadas":    to_int(post.get("aprovadas")),
    "postagens_reprovadas":   to_int(post.get("reprovadas")),
    "postagens_em_analise":   to_int(post.get("em_analise")),
    "quiz_respondidos":       to_int(quiz.get("respondidos")),
    "quiz_aprovados":         to_int(quiz.get("aprovados")),
    "quiz_reprovados":        to_int(quiz.get("reprovados")),
    "nota_media":             to_float(quiz.get("nota_media")),
    "tentativas_com_nota":    to_int(quiz.get("tentativas_com_nota")),
    "tentativas_sem_nota":    to_int(quiz.get("tentativas_sem_nota")),
    "nota_desvio_padrao_amostral": (to_float(quiz.get("nota_desvio_padrao_amostral"))
                                     if quiz.get("nota_desvio_padrao_amostral") is not None else None),
    "nota_mediana":           (to_float(quiz.get("nota_mediana"))
                                if quiz.get("nota_mediana") is not None else None),
}
# usuários distintos que postaram no período (participação real, sem depender de cadastro "ativo")
_postaram = q1(f"""
    SELECT COUNT(DISTINCT usuario_key) AS total
    FROM {g('fato_postagens')}
    WHERE data_key IN {PERIODO_SQL} AND usuario_key IS NOT NULL
""")
KPIS["usuarios_que_postaram"] = to_int(_postaram.get("total"))
print(KPIS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04 — Séries, rankings, mapa e cursos

# COMMAND ----------

MESES = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]

def rotulo(d: date) -> str:
    return f"{d.day} {MESES[d.month - 1]}"

def dias(ini: date, fim: date):
    n = (fim - ini).days
    return [ini + timedelta(days=i) for i in range(n + 1)]

def amostrar(seq, limite=60):
    """Reduz séries longas mantendo o primeiro e o último ponto."""
    if len(seq) <= limite:
        return list(range(len(seq)))
    passo = -(-len(seq) // limite)
    idx = list(range(0, len(seq), passo))
    if idx[-1] != len(seq) - 1:
        idx.append(len(seq) - 1)
    return idx

# --- evolução de usuários acumulados por tipo de conta (mesma regra do ds_aba1_evolucao_usuarios)
GRUPOS = ["Morador Residencial", "Usuário Comum", "Síndico", "Usuário Comercial", "Cooperativa"]
evo_rows = q(f"""
WITH usuarios_classificados AS (
    SELECT CAST(registro_em AS DATE) AS dia,
           CASE
               WHEN tipo_usuario = 'Cooperativa' THEN 'Cooperativa'
               WHEN tipo_usuario IN ('Síndico Residencial', 'Síndico Comercial') THEN 'Síndico'
               WHEN tipo_usuario = 'Morador Residencial' THEN 'Morador Residencial'
               WHEN tipo_usuario = 'Usuário Comercial' THEN 'Usuário Comercial'
               ELSE 'Usuário Comum'
           END AS grupo
    FROM {g('dim_usuario')}
    WHERE registro_em IS NOT NULL
)
SELECT dia, grupo, COUNT(*) AS novos FROM usuarios_classificados GROUP BY dia, grupo
""")
novos = {}
for r in evo_rows:
    novos[(r["grupo"], r["dia"])] = to_int(r["novos"])

calendario = dias(d_ini, d_fim)
acum_inicial = {gr: sum(n for (g2, dia), n in novos.items() if g2 == gr and dia < d_ini) for gr in GRUPOS}
series_full = {}
for gr in GRUPOS:
    total, vals = acum_inicial[gr], []
    for dia in calendario:
        total += novos.get((gr, dia), 0)
        vals.append(total)
    series_full[gr] = vals
idx = amostrar(calendario)
EVOLUCAO = {
    "rotulos": [rotulo(calendario[i]) for i in idx],
    "series": [{"nome": gr, "valores": [series_full[gr][i] for i in idx]} for gr in GRUPOS],
}

# --- ranking de condomínios por postagens (top 5)
CONDOMINIOS_RANKING = [
    {"nome": r["nome_condominio"], "valor": to_int(r["total"])}
    for r in q(f"""
        SELECT c.nome_condominio, COUNT(*) AS total
        FROM {g('fato_postagens')} f
        JOIN {g('dim_condominio')} c ON c.condominio_key = f.condominio_key
        WHERE f.data_key IN {PERIODO_SQL}
        GROUP BY c.nome_condominio ORDER BY total DESC, c.nome_condominio LIMIT 5
    """)
]

# --- ranking de usuários por postagens (top 5; só a chave interna, sem dado pessoal)
USUARIOS_RANKING = [
    {"nome": f"usuário {r['usuario_key']}", "valor": to_int(r["total"])}
    for r in q(f"""
        SELECT CAST(usuario_key AS STRING) AS usuario_key, COUNT(*) AS total
        FROM {g('fato_postagens')}
        WHERE data_key IN {PERIODO_SQL} AND usuario_key IS NOT NULL
        GROUP BY usuario_key ORDER BY total DESC, usuario_key LIMIT 5
    """)
]

# --- condomínios por UF (sigla de 2 letras)
UF = {
    r["estado"].strip().upper(): to_int(r["total"])
    for r in q(f"""
        SELECT estado, COUNT(*) AS total FROM {g('dim_condominio')}
        WHERE estado IS NOT NULL GROUP BY estado
    """)
    if r["estado"] and len(r["estado"].strip()) == 2
}

# --- postagens por dia (dias sem postagem entram como 0)
por_dia = {
    r["dia"]: to_int(r["total"])
    for r in q(f"""
        SELECT t.data AS dia, COUNT(*) AS total
        FROM {g('fato_postagens')} f JOIN {g('dim_tempo')} t ON t.data_key = f.data_key
        WHERE t.data BETWEEN DATE'{INI}' AND DATE'{FIM}' GROUP BY t.data
    """)
}
POSTAGENS_DIA = {
    "rotulos": [rotulo(d_ini), rotulo(d_fim)],
    "valores": [por_dia.get(d, 0) for d in calendario],
}

# --- quizzes aprovados por dia: uma linha por tentativa concluída e aprovada.
# fato_pontuacao é referência de pontos, não o universo de tentativas de quiz.
quiz_dia = {
    r["dia"]: to_int(r["total"])
    for r in q(f"""
        SELECT t.data AS dia, COUNT(*) AS total
        FROM {g('fato_tentativas_quiz')} ft
        JOIN {g('dim_tempo')} t ON t.data_key = ft.data_key
        WHERE ft.concluido_em IS NOT NULL AND ft.aprovado = TRUE
          AND t.data BETWEEN DATE'{INI}' AND DATE'{FIM}'
        GROUP BY t.data
    """)
}
idx_q = amostrar(calendario)
QUIZ_DIA = {
    "rotulos": [rotulo(calendario[i]) for i in idx_q],
    "valores": [quiz_dia.get(calendario[i], 0) for i in idx_q],
}

# --- respostas por curso
CURSOS = [
    {"curso": r["titulo_curso"], "total": to_int(r["total"]), "corretas": to_int(r["corretas"])}
    for r in q(f"""
        SELECT c.titulo_curso,
               COUNT(*) AS total,
               COUNT(CASE WHEN fr.correta THEN 1 END) AS corretas
        FROM {g('fato_respostas_quiz')} fr
        JOIN {g('dim_curso')} c ON c.curso_key = fr.curso_key
        WHERE fr.data_key IN {PERIODO_SQL}
        GROUP BY c.titulo_curso ORDER BY total DESC
    """)
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04B — Distribuições, filtros e comparativos
# MAGIC As novas granularidades são agregadas por dia/dimensões. A única chave por pessoa
# MAGIC é um hash efêmero com sal aleatório por exportação; o sal e os IDs naturais não são exportados.

# COMMAND ----------

import secrets
import math

# O identificador pseudônimo só permite somar eventos da mesma pessoa dentro deste snapshot.
# A cada exportação o sal muda, impedindo a ligação direta entre snapshots.
_HASH_SALT = secrets.token_hex(16)

# Agregados diários para KPIs do recorte e filtros por tipo, condomínio, status e categoria.
FATOS_DIARIOS = [
    {
        "d": str(r["d"]), "tipo_evento": "postagem",
        "tipo_usuario": r["tipo_usuario"],
        "condominio_key": str(r["condominio_key"]) if r["condominio_key"] is not None else None,
        "condominio": r["condominio"], "status": r["status"],
        "categoria_key": str(r["categoria_key"]) if r["categoria_key"] is not None else None,
        "categoria": r["categoria"], "n": to_int(r["n"])
    }
    for r in q(f"""
        SELECT CAST(t.data AS STRING) AS d,
               COALESCE(u.tipo_usuario, 'Não informado') AS tipo_usuario,
               f.condominio_key,
               COALESCE(c.nome_condominio, 'Não informado') AS condominio,
               COALESCE(s.status_postagem, 'Não informado') AS status,
               CAST(f.categoria_residuo_key AS STRING) AS categoria_key,
               COALESCE(cat.nome_categoria, 'Não informada') AS categoria,
               COUNT(*) AS n
        FROM {g('fato_postagens')} f
        JOIN {g('dim_tempo')} t ON t.data_key = f.data_key
        LEFT JOIN {g('dim_usuario')} u ON u.usuario_key = f.usuario_key
        LEFT JOIN {g('dim_condominio')} c ON c.condominio_key = f.condominio_key
        LEFT JOIN {g('dim_status_postagem')} s ON s.status_postagem_key = f.status_postagem_key
        LEFT JOIN {g('dim_categoria_residuo')} cat ON cat.categoria_residuo_key = f.categoria_residuo_key
        WHERE t.data BETWEEN DATE'{INI}' AND DATE'{FIM}'
        GROUP BY t.data, u.tipo_usuario, f.condominio_key, c.nome_condominio,
                 s.status_postagem, f.categoria_residuo_key, cat.nome_categoria
    """)
]
FATOS_DIARIOS.extend([
    {
        "d": str(r["d"]), "tipo_evento": "quiz",
        "tipo_usuario": r["tipo_usuario"],
        "condominio_key": str(r["condominio_key"]) if r["condominio_key"] is not None else None,
        "condominio": r["condominio"], "status": None,
        "categoria_key": None, "categoria": None, "n": to_int(r["n"])
    }
    for r in q(f"""
        SELECT CAST(t.data AS STRING) AS d,
               COALESCE(u.tipo_usuario, 'Não informado') AS tipo_usuario,
               ft.condominio_key,
               COALESCE(c.nome_condominio, 'Não informado') AS condominio,
               COUNT(*) AS n
        FROM {g('fato_tentativas_quiz')} ft
        JOIN {g('dim_tempo')} t ON t.data_key = ft.data_key
        LEFT JOIN {g('dim_usuario')} u ON u.usuario_key = ft.usuario_key
        LEFT JOIN {g('dim_condominio')} c ON c.condominio_key = ft.condominio_key
        WHERE ft.concluido_em IS NOT NULL
          AND t.data BETWEEN DATE'{INI}' AND DATE'{FIM}'
        GROUP BY t.data, u.tipo_usuario, ft.condominio_key, c.nome_condominio
    """)
])

# Universos disponíveis para os controles; não envia nomes/e-mails/IDs de pessoas.
UNIVERSO_USUARIOS_POR_TIPO = [
    {"tipo_usuario": r["tipo_usuario"], "usuarios": to_int(r["usuarios"])}
    for r in q(f"""
        SELECT COALESCE(tipo_usuario, 'Não informado') AS tipo_usuario, COUNT(*) AS usuarios
        FROM {g('dim_usuario')} GROUP BY COALESCE(tipo_usuario, 'Não informado')
        ORDER BY tipo_usuario
    """)
]
UNIVERSO_CONDOMINIOS = [
    {"condominio_key": str(r["condominio_key"]), "condominio": r["nome_condominio"] or "Não informado"}
    for r in q(f"""
        SELECT condominio_key, nome_condominio FROM {g('dim_condominio')}
        ORDER BY nome_condominio, condominio_key
    """)
]
UNIVERSO_CATEGORIAS = [
    {"categoria_key": str(r["categoria_residuo_key"]), "categoria": r["nome_categoria"] or "Não informada"}
    for r in q(f"""
        SELECT categoria_residuo_key, nome_categoria FROM {g('dim_categoria_residuo')}
        ORDER BY nome_categoria, categoria_residuo_key
    """)
]
UNIVERSO_STATUS_POSTAGEM = [
    r["status_postagem"] for r in q(f"""
        SELECT DISTINCT status_postagem FROM {g('dim_status_postagem')}
        WHERE status_postagem IS NOT NULL ORDER BY status_postagem
    """)
]
USUARIOS_CADASTRADOS_POR_DIA_TIPO = [
    {"d": str(r["d"]), "tipo_usuario": r["tipo_usuario"], "usuarios": to_int(r["usuarios"])}
    for r in q(f"""
        SELECT CAST(registro_em AS DATE) AS d,
               COALESCE(tipo_usuario, 'Não informado') AS tipo_usuario,
               COUNT(*) AS usuarios
        FROM {g('dim_usuario')}
        WHERE registro_em IS NOT NULL AND CAST(registro_em AS DATE) <= DATE'{FIM}'
        GROUP BY CAST(registro_em AS DATE), COALESCE(tipo_usuario, 'Não informado')
        ORDER BY d, tipo_usuario
    """)
]

# Pseudonimização efêmera para permitir somar postagens por usuário após filtros de datas/dimensões.
# Hashes não são IDs naturais e o sal não é incluído no JSON.
PARTICIPACAO_ANONIMA = [
    {
        "d": str(r["d"]), "usuario_hash": r["usuario_hash"],
        "tipo_usuario": r["tipo_usuario"], "elegivel_universo_temporal": bool(r["elegivel_universo_temporal"]),
        "condominio_key": str(r["condominio_key"]) if r["condominio_key"] is not None else None,
        "status": r["status"], "categoria_key": str(r["categoria_key"]) if r["categoria_key"] is not None else None,
        "n_postagens": to_int(r["n_postagens"]), "n_quizzes": to_int(r["n_quizzes"])
    }
    for r in q(f"""
        WITH eventos AS (
            SELECT CAST(t.data AS STRING) AS d,
                   sha2(concat('{_HASH_SALT}', ':', CAST(f.usuario_key AS STRING)), 256) AS usuario_hash,
                   COALESCE(u.tipo_usuario, 'Não informado') AS tipo_usuario,
                   CASE WHEN u.registro_em IS NOT NULL AND CAST(u.registro_em AS DATE) <= DATE'{FIM}' THEN TRUE ELSE FALSE END AS elegivel_universo_temporal,
                   f.condominio_key, COALESCE(s.status_postagem, 'Não informado') AS status,
                   CAST(f.categoria_residuo_key AS STRING) AS categoria_key,
                   COUNT(*) AS n_postagens, CAST(0 AS BIGINT) AS n_quizzes
            FROM {g('fato_postagens')} f
            JOIN {g('dim_tempo')} t ON t.data_key = f.data_key
            LEFT JOIN {g('dim_usuario')} u ON u.usuario_key = f.usuario_key
            LEFT JOIN {g('dim_status_postagem')} s ON s.status_postagem_key = f.status_postagem_key
            WHERE t.data BETWEEN DATE'{INI}' AND DATE'{FIM}' AND f.usuario_key IS NOT NULL
            GROUP BY t.data, f.usuario_key, u.tipo_usuario, u.registro_em, f.condominio_key,
                     s.status_postagem, f.categoria_residuo_key
            UNION ALL
            SELECT CAST(t.data AS STRING) AS d,
                   sha2(concat('{_HASH_SALT}', ':', CAST(ft.usuario_key AS STRING)), 256) AS usuario_hash,
                   COALESCE(u.tipo_usuario, 'Não informado') AS tipo_usuario,
                   CASE WHEN u.registro_em IS NOT NULL AND CAST(u.registro_em AS DATE) <= DATE'{FIM}' THEN TRUE ELSE FALSE END AS elegivel_universo_temporal,
                   ft.condominio_key, CAST(NULL AS STRING) AS status, CAST(NULL AS STRING) AS categoria_key,
                   CAST(0 AS BIGINT) AS n_postagens, COUNT(*) AS n_quizzes
            FROM {g('fato_tentativas_quiz')} ft
            JOIN {g('dim_tempo')} t ON t.data_key = ft.data_key
            LEFT JOIN {g('dim_usuario')} u ON u.usuario_key = ft.usuario_key
            WHERE ft.concluido_em IS NOT NULL AND t.data BETWEEN DATE'{INI}' AND DATE'{FIM}'
              AND ft.usuario_key IS NOT NULL
            GROUP BY t.data, ft.usuario_key, u.tipo_usuario, u.registro_em, ft.condominio_key
        )
        SELECT d, usuario_hash, tipo_usuario, elegivel_universo_temporal, condominio_key,
               status, categoria_key, SUM(n_postagens) AS n_postagens, SUM(n_quizzes) AS n_quizzes
        FROM eventos
        GROUP BY d, usuario_hash, tipo_usuario, elegivel_universo_temporal, condominio_key, status, categoria_key
    """)
]

# Universo anonimizado de usuários: permite incluir zero postagens em recortes por tipo
# sem expor nomes, e-mails ou IDs naturais no JSON.
UNIVERSO_USUARIOS_ANONIMO = [
    {
        "usuario_hash": r["usuario_hash"],
        "tipo_usuario": r["tipo_usuario"],
        "registro_em": str(r["registro_em"]) if r["registro_em"] is not None else None,
    }
    for r in q(f"""
        SELECT
            sha2(concat('{_HASH_SALT}', ':', CAST(usuario_key AS STRING)), 256) AS usuario_hash,
            COALESCE(tipo_usuario, 'Não informado') AS tipo_usuario,
            CAST(registro_em AS DATE) AS registro_em
        FROM {g('dim_usuario')}
        WHERE registro_em IS NOT NULL AND CAST(registro_em AS DATE) <= DATE'{FIM}'
    """)
]

# Notas: apenas tentativas concluídas com nota numérica. A tabela mantém contagens, não linhas individuais.
NOTAS_QUIZ_DIARIAS = [
    {"d": str(r["d"]), "tipo_usuario": r["tipo_usuario"],
     "condominio_key": str(r["condominio_key"]) if r["condominio_key"] is not None else None,
     "nota": to_float(r["nota"]), "frequencia": to_int(r["frequencia"])}
    for r in q(f"""
        SELECT CAST(t.data AS STRING) AS d,
               COALESCE(u.tipo_usuario, 'Não informado') AS tipo_usuario,
               ft.condominio_key, CAST(ft.nota AS DOUBLE) AS nota, COUNT(*) AS frequencia
        FROM {g('fato_tentativas_quiz')} ft
        JOIN {g('dim_tempo')} t ON t.data_key = ft.data_key
        LEFT JOIN {g('dim_usuario')} u ON u.usuario_key = ft.usuario_key
        WHERE ft.concluido_em IS NOT NULL AND ft.nota IS NOT NULL
          AND t.data BETWEEN DATE'{INI}' AND DATE'{FIM}'
        GROUP BY t.data, u.tipo_usuario, ft.condominio_key, CAST(ft.nota AS DOUBLE)
        ORDER BY t.data, u.tipo_usuario, ft.condominio_key, CAST(ft.nota AS DOUBLE)
    """)
]

# Estatística ponderada sem reconstruir arrays de observações no driver.
def _valor_ordem(freqs, pos):
    acumulado = 0
    for valor, freq in freqs:
        acumulado += freq
        if pos < acumulado:
            return valor
    return freqs[-1][0] if freqs else None

def _quantil_tipo7(freqs, p):
    n = sum(freq for _, freq in freqs)
    if not n:
        return None
    pos = (n - 1) * p
    lo, hi = math.floor(pos), math.ceil(pos)
    a, b = _valor_ordem(freqs, lo), _valor_ordem(freqs, hi)
    return float(a + (pos - lo) * (b - a))

def _estatisticas(freqs, unidade, aviso_amostra=False, box_minimo=1):
    freqs = sorted([(float(v), int(n)) for v, n in freqs if v is not None and int(n) > 0], key=lambda x: x[0])
    n = sum(f for _, f in freqs)
    if not n:
        return {"n": 0, "unidade": unidade, "media": None, "mediana": None,
                "desvio_padrao_amostral": None, "histograma": [], "boxplot": None,
                "aviso": "Sem observações disponíveis para o recorte."}
    vals = [v for v, _ in freqs]
    minimo, maximo = vals[0], vals[-1]
    media = sum(v * f for v, f in freqs) / n
    dp = math.sqrt(sum(f * ((v - media) ** 2) for v, f in freqs) / (n - 1)) if n > 1 else None
    q1, mediana, q3 = (_quantil_tipo7(freqs, p) for p in (.25, .5, .75))
    iqr = q3 - q1
    lim_inf, lim_sup = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    dentro = [v for v in vals if lim_inf <= v <= lim_sup]
    out_inf = sum(f for v, f in freqs if v < lim_inf)
    out_sup = sum(f for v, f in freqs if v > lim_sup)
    amplitude = maximo - minimo
    largura_fd = 2 * iqr / (n ** (1 / 3)) if iqr > 0 and n > 1 else 0
    if amplitude <= 0:
        bins = 1
    elif largura_fd > 0:
        bins = max(1, min(30, math.ceil(amplitude / largura_fd)))
    else:
        bins = max(1, min(30, math.ceil(math.log2(n) + 1)))
    largura = amplitude / bins if bins > 1 else 0
    contagens = [0] * bins
    for v, f in freqs:
        idx = min(bins - 1, int((v - minimo) / largura)) if largura > 0 else 0
        contagens[idx] += f
    histograma = []
    for i, f in enumerate(contagens):
        ini_bin = minimo + i * largura if bins > 1 else minimo
        fim_bin = maximo if bins == 1 or i == bins - 1 else minimo + (i + 1) * largura
        histograma.append({"inicio": round(ini_bin, 4), "fim": round(fim_bin, 4),
                           "frequencia": int(f), "faixa": f"{ini_bin:.2f}–{fim_bin:.2f}"})
    box = None
    if n >= box_minimo:
        box = {
            "metodo_quartis": "tipo 7 (interpolação linear)",
            "metodo_whiskers": "Tukey: 1,5 × IQR; whiskers nos extremos observados dentro dos limites",
            "minimo_absoluto": minimo, "q1": q1, "mediana": mediana, "q3": q3,
            "maximo_absoluto": maximo, "iqr": iqr,
            "whisker_inferior": min(dentro) if dentro else minimo,
            "whisker_superior": max(dentro) if dentro else maximo,
            "outliers_inferiores": int(out_inf), "outliers_superiores": int(out_sup),
            "outliers_total": int(out_inf + out_sup)
        }
    aviso = "Amostra pequena (N < 30); interprete a dispersão com cautela." if aviso_amostra and n < 30 else None
    if n < box_minimo:
        aviso = f"Boxplot não exibido: são necessárias pelo menos {box_minimo} observações; N={n}."
    return {"n": int(n), "unidade": unidade, "media": media, "mediana": mediana,
            "desvio_padrao_amostral": dp, "minimo": minimo, "maximo": maximo,
            "histograma": histograma, "boxplot": box, "aviso": aviso}

# Distribuição de postagens por usuário: universo com data de cadastro conhecida até o fim do período,
# incluindo zero postagens. Usuários sem data ou cadastrados depois do fim são contados à parte.
_postagens_usuario = q(f"""
    WITH por_usuario AS (
        SELECT COALESCE(u.tipo_usuario, 'Não informado') AS tipo_usuario,
               u.usuario_key, COUNT(f.postagem_key) AS postagens
        FROM {g('dim_usuario')} u
        LEFT JOIN {g('fato_postagens')} f
          ON f.usuario_key = u.usuario_key AND f.data_key IN {PERIODO_SQL}
        WHERE u.registro_em IS NOT NULL AND CAST(u.registro_em AS DATE) <= DATE'{FIM}'
        GROUP BY COALESCE(u.tipo_usuario, 'Não informado'), u.usuario_key
    )
    SELECT tipo_usuario, postagens, COUNT(*) AS frequencia
    FROM por_usuario GROUP BY tipo_usuario, postagens ORDER BY tipo_usuario, postagens
""")
_freq_por_tipo = {}
for r in _postagens_usuario:
    _freq_por_tipo.setdefault(r["tipo_usuario"], []).append((to_int(r["postagens"]), to_int(r["frequencia"])))
_freq_global = {}
for _tipo, _freqs in _freq_por_tipo.items():
    for _v, _f in _freqs:
        _freq_global[_v] = _freq_global.get(_v, 0) + _f
POSTAGENS_POR_USUARIO = _estatisticas(list(_freq_global.items()), "postagens por usuário", box_minimo=1)
POSTAGENS_POR_USUARIO["usuarios_sem_postagem"] = int(_freq_global.get(0, 0))
POSTAGENS_POR_USUARIO["total_usuarios"] = int(sum(_freq_global.values()))
POSTAGENS_POR_USUARIO["criterio_universo"] = f"Usuários com registro_em conhecido e até {FIM}; inclui zero postagens no período."
POSTAGENS_POR_USUARIO["usuarios_sem_data_cadastro"] = to_int(q1(f"SELECT COUNT(*) AS n FROM {g('dim_usuario')} WHERE registro_em IS NULL").get("n"))
POSTAGENS_POR_USUARIO["usuarios_registrados_apos_periodo"] = to_int(q1(f"SELECT COUNT(*) AS n FROM {g('dim_usuario')} WHERE CAST(registro_em AS DATE) > DATE'{FIM}'").get("n"))
POSTAGENS_POR_USUARIO["segmentos_tipo_usuario"] = {
    tipo: _estatisticas(freqs, "postagens por usuário") for tipo, freqs in _freq_por_tipo.items()
}

_notas_freq = [
    (float(r["nota"]), to_int(r["frequencia"]))
    for r in q(f"""
        SELECT CAST(nota AS DOUBLE) AS nota, COUNT(*) AS frequencia
        FROM {g('fato_tentativas_quiz')}
        WHERE concluido_em IS NOT NULL AND nota IS NOT NULL AND data_key IN {PERIODO_SQL}
        GROUP BY CAST(nota AS DOUBLE) ORDER BY CAST(nota AS DOUBLE)
    """)
]
NOTAS_QUIZ = _estatisticas(_notas_freq, "nota do quiz", aviso_amostra=True, box_minimo=5)
NOTAS_QUIZ["tentativas_concluidas"] = KPIS["quiz_respondidos"]
NOTAS_QUIZ["tentativas_com_nota"] = KPIS["tentativas_com_nota"]
NOTAS_QUIZ["tentativas_sem_nota"] = KPIS["tentativas_sem_nota"]

# Comparação com a janela anterior de igual duração; denominador zero permanece indefinido.
_dias_periodo = (d_fim - d_ini).days + 1
_prev_ini, _prev_fim = d_ini - timedelta(days=_dias_periodo), d_ini - timedelta(days=1)
_prev = q1(f"""
    SELECT COUNT(*) AS total,
           COUNT(CASE WHEN lower(trim(s.status_postagem)) = 'aprovada' THEN 1 END) AS aprovadas,
           COUNT(DISTINCT f.usuario_key) AS usuarios
    FROM {g('fato_postagens')} f
    JOIN {g('dim_status_postagem')} s ON s.status_postagem_key = f.status_postagem_key
    JOIN {g('dim_tempo')} t ON t.data_key = f.data_key
    WHERE t.data BETWEEN DATE'{_prev_ini.isoformat()}' AND DATE'{_prev_fim.isoformat()}'
""")
_prev_quiz = q1(f"""
    SELECT COUNT(*) AS total, COUNT(CASE WHEN aprovado THEN 1 END) AS aprovados
    FROM {g('fato_tentativas_quiz')} ft
    JOIN {g('dim_tempo')} t ON t.data_key = ft.data_key
    WHERE ft.concluido_em IS NOT NULL
      AND t.data BETWEEN DATE'{_prev_ini.isoformat()}' AND DATE'{_prev_fim.isoformat()}'
""")
def _comparar(atual, anterior):
    atual, anterior = float(atual or 0), float(anterior or 0)
    delta = atual - anterior
    return {"atual": atual, "periodo_anterior": anterior, "variacao_absoluta": delta,
            "variacao_percentual": None if anterior == 0 else delta / anterior * 100,
            "base_anterior_zero": anterior == 0}
_taxa_post_atual = 100 * KPIS["postagens_aprovadas"] / KPIS["postagens_total"] if KPIS["postagens_total"] else None
_taxa_post_ant = 100 * to_int(_prev.get("aprovadas")) / to_int(_prev.get("total")) if to_int(_prev.get("total")) else None
_taxa_quiz_atual = 100 * KPIS["quiz_aprovados"] / KPIS["quiz_respondidos"] if KPIS["quiz_respondidos"] else None
_taxa_quiz_ant = 100 * to_int(_prev_quiz.get("aprovados")) / to_int(_prev_quiz.get("total")) if to_int(_prev_quiz.get("total")) else None
COMPARATIVOS = {
    "periodo_anterior": {"data_inicio": _prev_ini.isoformat(), "data_fim": _prev_fim.isoformat()},
    "postagens_total": _comparar(KPIS["postagens_total"], _prev.get("total")),
    "usuarios_que_postaram": _comparar(KPIS["usuarios_que_postaram"], _prev.get("usuarios")),
    "quiz_respondidos": _comparar(KPIS["quiz_respondidos"], _prev_quiz.get("total")),
    "taxa_aprovacao_postagens": {"atual": _taxa_post_atual, "periodo_anterior": _taxa_post_ant,
        "variacao_pontos_percentuais": None if _taxa_post_atual is None or _taxa_post_ant is None else _taxa_post_atual - _taxa_post_ant},
    "taxa_aprovacao_quiz": {"atual": _taxa_quiz_atual, "periodo_anterior": _taxa_quiz_ant,
        "variacao_pontos_percentuais": None if _taxa_quiz_atual is None or _taxa_quiz_ant is None else _taxa_quiz_atual - _taxa_quiz_ant}
}

# Reconcile all new series before the JSON can be written or published.
if sum(x["n"] for x in FATOS_DIARIOS if x["tipo_evento"] == "postagem") != KPIS["postagens_total"]:
    raise RuntimeError("fatos_diarios/postagem não reconcilia com kpis.postagens_total")
if sum(x["n"] for x in FATOS_DIARIOS if x["tipo_evento"] == "quiz") != KPIS["quiz_respondidos"]:
    raise RuntimeError("fatos_diarios/quiz não reconcilia com kpis.quiz_respondidos")
if sum(f for _, f in _freq_global.items()) != POSTAGENS_POR_USUARIO["total_usuarios"]:
    raise RuntimeError("Distribuição de postagens por usuário não reconcilia com o universo cadastrado")
if sum(f for _, f in _notas_freq) != KPIS["tentativas_com_nota"]:
    raise RuntimeError("Distribuição de notas não reconcilia com tentativas_com_nota")
if sum(r["frequencia"] for r in NOTAS_QUIZ_DIARIAS) != KPIS["tentativas_com_nota"]:
    raise RuntimeError("notas_quiz_diarias não reconcilia com tentativas_com_nota")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 05 — Monta, valida e grava
# MAGIC
# MAGIC Se algum bloco obrigatório vier vazio, o notebook **falha** e o arquivo anterior fica intacto
# MAGIC (o painel continua mostrando o último `data.json` bom).

# COMMAND ----------

def _agora() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")

_geracao_texto = _agora()

DATA = {
    "meta": {
        "gerado_em": datetime.strptime(_geracao_texto[:10], "%d/%m/%Y").date().isoformat(),
        "data_inicio": INI,
        "data_fim": FIM,
        "periodo": f"{rotulo(d_ini)} {d_ini.year} – {rotulo(d_fim)} {d_fim.year}",
        "atualizado": f"atualizado em {_geracao_texto}",
        "fonte": "camada Gold · Databricks",
        "lakeview_url": LAKEVIEW_URL,
    },
    "kpis": KPIS,
    "evolucao_usuarios": EVOLUCAO,
    "condominios_ranking": CONDOMINIOS_RANKING,
    "usuarios_ranking": USUARIOS_RANKING,
    "condominios_por_uf": UF,
    "postagens_por_dia": POSTAGENS_DIA,
    "quiz_por_dia": QUIZ_DIA,
    "respostas_por_curso": CURSOS,
    "distribuicoes": {
        "postagens_por_usuario": POSTAGENS_POR_USUARIO,
        "notas_quiz": NOTAS_QUIZ,
    },
    "comparativos": COMPARATIVOS,
    "fatos_diarios": FATOS_DIARIOS,
    "participacao_anonima": PARTICIPACAO_ANONIMA,
    "universo_usuarios_anonimo": UNIVERSO_USUARIOS_ANONIMO,
    "universo_usuarios_por_tipo": UNIVERSO_USUARIOS_POR_TIPO,
    "usuarios_cadastrados_por_dia_tipo": USUARIOS_CADASTRADOS_POR_DIA_TIPO,
    "universo_condominios": UNIVERSO_CONDOMINIOS,
    "universo_categorias": UNIVERSO_CATEGORIAS,
    "universo_status_postagem": UNIVERSO_STATUS_POSTAGEM,
    "notas_quiz_diarias": NOTAS_QUIZ_DIARIAS,
}

# validações mínimas: melhor falhar do que publicar painel vazio
erros = []
if KPIS["usuarios_gerais"] == 0:
    erros.append("dim_usuario vazia")
if KPIS["condominios"] == 0:
    erros.append("dim_condominio vazia")
if not CONDOMINIOS_RANKING:
    erros.append("ranking de condomínios vazio")
if not POSTAGENS_DIA["valores"] or sum(POSTAGENS_DIA["valores"]) != KPIS["postagens_total"]:
    erros.append(
        f"postagens por dia ({sum(POSTAGENS_DIA['valores'])}) não bate com o total ({KPIS['postagens_total']})"
    )
if KPIS["quiz_aprovados"] + KPIS["quiz_reprovados"] != KPIS["quiz_respondidos"]:
    erros.append("aprovados + reprovados difere de quiz_respondidos")
if KPIS["condominios_ativos"] + KPIS["condominios_inativos"] != KPIS["condominios"]:
    erros.append("condomínios ativos + inativos difere do total")
if KPIS["tentativas_com_nota"] + KPIS["tentativas_sem_nota"] != KPIS["quiz_respondidos"]:
    erros.append("tentativas com nota + sem nota difere do total concluído")
if erros:
    raise RuntimeError("data.json NÃO gravado: " + "; ".join(erros))

# COMMAND ----------

payload = json.dumps(DATA, ensure_ascii=False, indent=2)

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.exports")

def _gravar(path: str, conteudo: str):
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(conteudo)
    except Exception:
        dbutils.fs.put(path, conteudo, True)

destino = f"{VOLUME_PATH}/data.json"
historico = f"{VOLUME_PATH}/data_{date.today().strftime('%Y%m%d')}.json"
_gravar(destino, payload)
_gravar(historico, payload)

print(f"OK: {destino} ({len(payload.encode('utf-8')) / 1024:.1f} KB) e cópia {historico}")
print(f"Postagens {KPIS['postagens_total']} · quizzes {KPIS['quiz_respondidos']} · condomínios {KPIS['condominios']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 06 — Publica no GitHub (preservado)
# MAGIC `github_repo` aponta para `EcoCiente-Instituto-J-F/bi-ecociente-painel`.
# MAGIC `github_branch=main` e `github_path=data.json` mantêm o arquivo junto do `index.html`.
# MAGIC O token deve ter acesso ao repositório oficial e **Contents: Read and write**;
# MAGIC a organização pode exigir aprovação. O token do repositório pessoal de teste não basta.
# MAGIC O Job usa `github_required=true` e o secret `ecociente-github/GITHUB_TOKEN`.
# MAGIC Para teste manual, o widget `github_token` tem precedência. Não publique o notebook preenchido.
# MAGIC A consulta do SHA, o PUT e a detecção de conteúdo idêntico continuam presentes.

# COMMAND ----------

import base64
import urllib.error
import urllib.request
from urllib.parse import quote

GH_TOKEN  = _widget("github_token", "")
GH_REPO   = _widget("github_repo", "EcoCiente-Instituto-J-F/bi-ecociente-painel")
GH_BRANCH = _widget("github_branch", "main")
GH_PATH   = _widget("github_path", "data.json").lstrip("/")

GH_REQUIRED = _widget("github_required", "false").lower() == "true"
GH_SECRET_SCOPE = _widget("github_secret_scope", "ecociente-github")
GH_SECRET_KEY = _widget("github_secret_key", "GITHUB_TOKEN")
if not GH_TOKEN:
    try:
        GH_TOKEN = dbutils.secrets.get(scope=GH_SECRET_SCOPE, key=GH_SECRET_KEY).strip()
    except Exception:
        if GH_REQUIRED:
            raise RuntimeError(
                "Token GitHub indisponível. Configure ecociente-github/GITHUB_TOKEN pelo notebook 08. "
                "O JSON foi salvo no Volume, mas não foi publicado."
            ) from None
if GH_REQUIRED and not (GH_TOKEN and GH_REPO):
    raise RuntimeError("Publicação obrigatória: informe token e repositório.")

def _gh(method: str, url: str, body: dict = None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "ecociente-exporta-data-json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        return exc.code, {"message": exc.read().decode("utf-8", "replace").replace(GH_TOKEN, "[TOKEN_OCULTO]")[:300]}

if not (GH_TOKEN and GH_REPO):
    print("github_token/github_repo não informados: data.json ficou só no Volume.")
else:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", GH_REPO):
        raise ValueError(f"github_repo deve ser 'dono/repositorio', recebi {GH_REPO!r}")
    api = f"https://api.github.com/repos/{GH_REPO}/contents/{quote(GH_PATH, safe='/')}"

    # sha atual do arquivo (necessário para atualizar; 404 = arquivo novo)
    status, atual = _gh("GET", f"{api}?ref={quote(GH_BRANCH, safe='')}")
    if status not in (200, 404):
        raise RuntimeError(f"GitHub respondeu {status} ao consultar {GH_PATH}: {atual.get('message')}")
    sha = atual.get("sha") if status == 200 else None

    # não cria commit vazio quando nada mudou
    conteudo_novo = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    if status == 200 and atual.get("content", "").replace(chr(10), "") == conteudo_novo:
        print("GitHub: data.json idêntico ao publicado, nada a commitar.")
    else:
        corpo = {
            "message": f"dados: atualização automática {date.today().isoformat()}",
            "content": conteudo_novo,
            "branch": GH_BRANCH,
        }
        if sha:
            corpo["sha"] = sha
        status, resp = _gh("PUT", api, corpo)
        if status not in (200, 201):
            raise RuntimeError(f"GitHub respondeu {status} ao publicar: {resp.get('message')}")
        print(f"GitHub: publicado em {GH_REPO}@{GH_BRANCH}:{GH_PATH} (HTTP {status}).")

# COMMAND ----------

displayHTML(f"<pre style='font-size:12px'>{payload[:6000]}</pre>")