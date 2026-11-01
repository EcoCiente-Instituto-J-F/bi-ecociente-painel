# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente | 07 — Exporta JSON e publica no GitHub
# MAGIC Lê a camada Gold e preserva a estrutura do painel: nove blocos, sete campos em `meta` e 15 KPIs.
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
    ROUND(AVG(nota), 2) AS nota_media
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

# --- quizzes aprovados por dia (fato_pontuacao, evento 'quiz_aprovado')
quiz_dia = {
    r["dia"]: to_int(r["total"])
    for r in q(f"""
        SELECT t.data AS dia, COUNT(*) AS total
        FROM {g('fato_pontuacao')} fp JOIN {g('dim_tempo')} t ON t.data_key = fp.data_key
        WHERE fp.tipo_evento_referencia = 'quiz_aprovado'
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