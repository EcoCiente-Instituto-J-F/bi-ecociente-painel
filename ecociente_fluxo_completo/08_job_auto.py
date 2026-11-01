# Databricks notebook source
# MAGIC %md
# MAGIC # EcoCiente | 08 — Cria o Job no workspace pessoal
# MAGIC Pasta: `/Workspace/Users/dominicgv09@gmail.com/inter - pipeline/ecociente_fluxo_completo`.
# MAGIC Sete tarefas em sequência: 01 → 02 → 03 → 04 → 05 → 06 → 07.
# MAGIC O 00 é compartilhado por `%run ./00_config`; o 08 não é tarefa da própria carga.
# MAGIC
# MAGIC Diariamente às **06h, America/Sao_Paulo**; alerta para **ecociente.jef@gmail.com**.
# MAGIC Inicialmente pausado: valide com **Run now**, depois defina `ATIVAR_AGENDAMENTO=True`
# MAGIC e execute o 08 novamente, ou despause o agendamento na interface.
# MAGIC
# MAGIC A conexão Aiven é `aiven_ecociente_jdbc`, banco `ecociente`; não usa senha do Aiven em widgets.
# MAGIC O envio GitHub está mantido e é obrigatório no Job: `EcoCiente-Instituto-J-F/bi-ecociente-painel`, `main`, `data.json`.
# MAGIC Se necessário, preencha **somente no Databricks** a variável `GITHUB_TOKEN` da célula seguinte.
# MAGIC O 08 grava esse valor em `ecociente-github/GITHUB_TOKEN`; o 07 lê o secret em cada execução.
# MAGIC Após salvar o secret, apague o valor da variável e salve o notebook novamente.
# MAGIC Não é necessário terminal. O usuário do Job deve ter acesso à conexão, aos schemas/Volume e ao secret.

# COMMAND ----------

import copy
import json

CONFIG_PATH = '/Workspace/Users/dominicgv09@gmail.com/inter - pipeline/ecociente_fluxo_completo/00_config'
# Para a primeira validação, deixe False. Depois do Run now bem-sucedido, use True.
ATIVAR_AGENDAMENTO = False
# Cole somente na SUA cópia no Databricks se o secret ainda não estiver configurado.
# O 08 salva no Secret Scope; o token NÃO entra nos parâmetros do Job.
GITHUB_TOKEN=""
GITHUB_SECRET_SCOPE = "ecociente-github"
GITHUB_SECRET_KEY = "GITHUB_TOKEN"
JOB_TEMPLATE = json.loads('{"name": "ecociente_fluxo_completo_diario", "max_concurrent_runs": 1, "description": "Workspace pessoal. Aiven somente leitura; JSON publicado no repositório oficial.", "tags": {"projeto": "ecociente", "configuracao": "fluxo_pessoal_v1"}, "schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "America/Sao_Paulo", "pause_status": "PAUSED"}, "email_notifications": {"on_failure": ["ecociente.jef@gmail.com"]}, "tasks": [{"task_key": "01_datamart", "run_if": "ALL_SUCCESS", "max_retries": 0, "notebook_task": {"notebook_path": "/Workspace/Users/dominicgv09@gmail.com/inter - pipeline/ecociente_fluxo_completo/01_datamart", "source": "WORKSPACE", "base_parameters": {"run_id": "{{job.run_id}}", "source_schema": "public", "target_catalog": "workspace", "bronze_schema": "ecociente_bronze", "silver_schema": "ecociente_silver", "gold_schema": "ecociente_gold", "quality_schema": "ecociente_quality", "timezone": "America/Sao_Paulo", "jdbc_connection": "aiven_ecociente_jdbc", "refresh_datamart": "false"}}}, {"task_key": "02_bronze", "run_if": "ALL_SUCCESS", "max_retries": 0, "notebook_task": {"notebook_path": "/Workspace/Users/dominicgv09@gmail.com/inter - pipeline/ecociente_fluxo_completo/02_bronze", "source": "WORKSPACE", "base_parameters": {"run_id": "{{job.run_id}}", "source_schema": "public", "target_catalog": "workspace", "bronze_schema": "ecociente_bronze", "silver_schema": "ecociente_silver", "gold_schema": "ecociente_gold", "quality_schema": "ecociente_quality", "timezone": "America/Sao_Paulo", "jdbc_connection": "aiven_ecociente_jdbc", "refresh_datamart": "false"}}, "depends_on": [{"task_key": "01_datamart"}]}, {"task_key": "03_silver", "run_if": "ALL_SUCCESS", "max_retries": 0, "notebook_task": {"notebook_path": "/Workspace/Users/dominicgv09@gmail.com/inter - pipeline/ecociente_fluxo_completo/03_silver", "source": "WORKSPACE", "base_parameters": {"run_id": "{{job.run_id}}", "source_schema": "public", "target_catalog": "workspace", "bronze_schema": "ecociente_bronze", "silver_schema": "ecociente_silver", "gold_schema": "ecociente_gold", "quality_schema": "ecociente_quality", "timezone": "America/Sao_Paulo", "jdbc_connection": "aiven_ecociente_jdbc", "refresh_datamart": "false"}}, "depends_on": [{"task_key": "02_bronze"}]}, {"task_key": "04_gold", "run_if": "ALL_SUCCESS", "max_retries": 0, "notebook_task": {"notebook_path": "/Workspace/Users/dominicgv09@gmail.com/inter - pipeline/ecociente_fluxo_completo/04_gold", "source": "WORKSPACE", "base_parameters": {"run_id": "{{job.run_id}}", "source_schema": "public", "target_catalog": "workspace", "bronze_schema": "ecociente_bronze", "silver_schema": "ecociente_silver", "gold_schema": "ecociente_gold", "quality_schema": "ecociente_quality", "timezone": "America/Sao_Paulo", "jdbc_connection": "aiven_ecociente_jdbc", "refresh_datamart": "false"}}, "depends_on": [{"task_key": "03_silver"}]}, {"task_key": "05_testes_gold", "run_if": "ALL_SUCCESS", "max_retries": 0, "notebook_task": {"notebook_path": "/Workspace/Users/dominicgv09@gmail.com/inter - pipeline/ecociente_fluxo_completo/05_testes_gold", "source": "WORKSPACE", "base_parameters": {"run_id": "{{job.run_id}}", "source_schema": "public", "target_catalog": "workspace", "bronze_schema": "ecociente_bronze", "silver_schema": "ecociente_silver", "gold_schema": "ecociente_gold", "quality_schema": "ecociente_quality", "timezone": "America/Sao_Paulo", "jdbc_connection": "aiven_ecociente_jdbc", "refresh_datamart": "false"}}, "depends_on": [{"task_key": "04_gold"}]}, {"task_key": "06_kpis_encerramento", "run_if": "ALL_SUCCESS", "max_retries": 0, "notebook_task": {"notebook_path": "/Workspace/Users/dominicgv09@gmail.com/inter - pipeline/ecociente_fluxo_completo/06_kpis_encerramento", "source": "WORKSPACE", "base_parameters": {"run_id": "{{job.run_id}}", "source_schema": "public", "target_catalog": "workspace", "bronze_schema": "ecociente_bronze", "silver_schema": "ecociente_silver", "gold_schema": "ecociente_gold", "quality_schema": "ecociente_quality", "timezone": "America/Sao_Paulo", "jdbc_connection": "aiven_ecociente_jdbc", "refresh_datamart": "false"}}, "depends_on": [{"task_key": "05_testes_gold"}]}, {"task_key": "07_exporta_data_json", "run_if": "ALL_SUCCESS", "max_retries": 0, "notebook_task": {"notebook_path": "/Workspace/Users/dominicgv09@gmail.com/inter - pipeline/ecociente_fluxo_completo/07_exporta_data_json", "source": "WORKSPACE", "base_parameters": {"run_id": "{{job.run_id}}", "catalog": "workspace", "gold_schema": "ecociente_gold", "volume_path": "/Volumes/workspace/ecociente_gold/exports", "data_inicio": "", "data_fim": "", "lakeview_url": "", "github_repo": "EcoCiente-Instituto-J-F/bi-ecociente-painel", "github_branch": "main", "github_path": "data.json", "github_secret_scope": "ecociente-github", "github_secret_key": "GITHUB_TOKEN", "github_required": "true"}}, "depends_on": [{"task_key": "06_kpis_encerramento"}]}]}')

def normalize_path(path):
    return (path[10:] if path.startswith('/Workspace/') else path).removesuffix('.ipynb').removesuffix('.py')

def resolve_paths(client, settings):
    folder = CONFIG_PATH.rsplit('/', 1)[0]
    available = {}
    for item in client.workspace.list(path=folder):
        if getattr(item.object_type, 'value', item.object_type) == 'NOTEBOOK':
            available.setdefault(normalize_path(item.path), []).append(item.path)
    result = copy.deepcopy(settings)
    expected = [CONFIG_PATH] + [t['notebook_task']['notebook_path'] for t in result['tasks']]
    for path in expected:
        matches = available.get(normalize_path(path), [])
        if len(matches) != 1:
            raise RuntimeError('Notebook ausente ou ambíguo; nenhum Job alterado: ' + path)
    for task in result['tasks']:
        path = task['notebook_task']['notebook_path']
        task['notebook_task']['notebook_path'] = available[normalize_path(path)][0]
    return result

def configure_github_secret(client, utils, token):
    if token.strip():
        scopes = {s.name for s in client.secrets.list_scopes()}
        if GITHUB_SECRET_SCOPE not in scopes:
            client.secrets.create_scope(scope=GITHUB_SECRET_SCOPE)
        client.secrets.put_secret(scope=GITHUB_SECRET_SCOPE, key=GITHUB_SECRET_KEY, string_value=token.strip())
    try:
        configured = utils.secrets.get(scope=GITHUB_SECRET_SCOPE, key=GITHUB_SECRET_KEY)
    except Exception:
        raise RuntimeError(
            'Configure GITHUB_TOKEN na primeira célula do 08 e execute novamente. '
            'O token será salvo no secret; não o envie por chat nem ao repositório. Nenhum Job alterado.'
        ) from None
    if not configured.strip():
        raise RuntimeError('Token GitHub vazio. Nenhum Job alterado.')

def same_job_scope(current, desired):
    if current.get('tags', {}).get('configuracao') != 'fluxo_pessoal_v1':
        return False
    old = {t.get('task_key'): normalize_path(t.get('notebook_task', {}).get('notebook_path', ''))
           for t in current.get('tasks', [])}
    new = {t['task_key']: normalize_path(t['notebook_task']['notebook_path']) for t in desired['tasks']}
    return old == new

def verify_saved(saved, expected):
    if not same_job_scope(saved, expected):
        raise RuntimeError('API não confirmou as sete tarefas/caminhos. Confira o Job antes de ativar.')
    actual = {t['task_key']: t for t in saved['tasks']}
    for desired in expected['tasks']:
        task = actual[desired['task_key']]
        if (task['notebook_task'].get('base_parameters') != desired['notebook_task']['base_parameters']
            or task.get('depends_on', []) != desired.get('depends_on', [])
            or task.get('run_if', 'ALL_SUCCESS') != 'ALL_SUCCESS'
            or task.get('max_retries', 0) != 0):
            raise RuntimeError('Parâmetros/dependências divergentes. Confira o Job.')
    if (saved.get('schedule') != expected['schedule']
        or saved.get('max_concurrent_runs', 1) != 1
        or saved.get('email_notifications', {}).get('on_failure') != expected['email_notifications']['on_failure']
        or any(saved.get(k) for k in ('parameters', 'trigger', 'triggers', 'continuous'))):
        raise RuntimeError('Agendamento/alertas/parâmetros globais divergentes. Confira o Job.')

def create_or_update_job(client, settings, models):
    matches = [j for j in client.jobs.list(name=settings['name'])
               if j.settings and j.settings.name == settings['name']]
    if len(matches) > 1:
        raise RuntimeError('Mais de um Job com esse nome; nenhum alterado.')
    if matches:
        job_id = matches[0].job_id
        current = client.jobs.get(job_id=job_id).settings.as_dict()
        if not same_job_scope(current, settings):
            raise RuntimeError('O Job existente não pertence a esta pasta/versão; nenhum alterado.')
        remove = [k for k in ('parameters', 'trigger', 'triggers', 'continuous') if current.get(k)]
        client.jobs.update(job_id=job_id, new_settings=models.JobSettings.from_dict(settings), fields_to_remove=remove)
    else:
        args = copy.deepcopy(settings)
        args['tasks'] = [models.Task.from_dict(t) for t in args['tasks']]
        args['schedule'] = models.CronSchedule.from_dict(args['schedule'])
        args['email_notifications'] = models.JobEmailNotifications.from_dict(args['email_notifications'])
        job_id = client.jobs.create(**args).job_id
    verify_saved(client.jobs.get(job_id=job_id).settings.as_dict(), settings)
    return job_id

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs as job_models
w = WorkspaceClient()
REQUESTED_CONFIG = resolve_paths(w, JOB_TEMPLATE)
configure_github_secret(w, dbutils, GITHUB_TOKEN)
GITHUB_TOKEN = ""
REQUESTED_CONFIG['schedule']['pause_status'] = 'UNPAUSED' if ATIVAR_AGENDAMENTO else 'PAUSED'
for task in REQUESTED_CONFIG['tasks']:
    print(task['task_key'] + ' -> ' + task['notebook_task']['notebook_path'])
print('06h America/Sao_Paulo; alerta ecociente.jef@gmail.com; Aiven somente leitura.')
print('GitHub oficial configurado. O token não é armazenado nos parâmetros do Job.')

# COMMAND ----------

job_id = create_or_update_job(w, REQUESTED_CONFIG, job_models)
print('Job configurado: ' + str(job_id))
print(w.config.host.rstrip('/') + '/#job/' + str(job_id))
print('Agendamento ' + ('ATIVO' if ATIVAR_AGENDAMENTO else 'PAUSADO para validação manual'))
print('Nenhuma carga foi iniciada pelo 08. Use Run now para validar o fluxo e o envio ao GitHub.')