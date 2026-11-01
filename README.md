# ecociente-painel-bi

<p align="center">
  <img src="Card%20README%20-%20bi-ecociente-painel.png" alt="ecociente-painel-bi: Painel de Business Intelligence do EcoCiente" width="100%">
</p>

![HTML5](https://img.shields.io/badge/HTML5-E34F26?style=for-the-badge&logo=html5&logoColor=white)
![CSS](https://img.shields.io/badge/CSS-663399?style=for-the-badge&logo=css&logoColor=white)
![JavaScript](https://img.shields.io/badge/JavaScript-F7DF1E?style=for-the-badge&logo=javascript&logoColor=black)
![GitHub repo size](https://img.shields.io/github/repo-size/EcoCiente-Instituto-J-F/ecociente-painel-bi?style=for-the-badge)
![GitHub last commit](https://img.shields.io/github/last-commit/EcoCiente-Instituto-J-F/ecociente-painel-bi?style=for-the-badge)
![License](https://img.shields.io/badge/license-MIT-green?style=for-the-badge)

> Painel de Business Intelligence do EcoCiente para acompanhar o engajamento dos usuários, a participação dos condomínios, as postagens e o desempenho nos quizzes. Os indicadores, gráficos e rankings vêm de um arquivo JSON gerado pelo pipeline de dados no Databricks.

## Sobre o projeto

O painel é uma página estática em HTML, CSS e JavaScript puro, sem framework, sem etapa de build e sem dependências instaladas. Os gráficos são desenhados em SVG pelo próprio script da página. O único recurso externo são as fontes IBM Plex, carregadas do Google Fonts.

A página tem dois modos de leitura, alternados no topo:

- **Relatório:** texto gerencial com resumo, KPIs, comparativos, constatações, plano de ação e conclusão. Os trechos que dependem de números são reescritos a partir dos dados. Os textos de contexto (mercado, produto, metodologia) ficam fixos no HTML.
- **Visualização** (`#visualizacao`): dashboard com KPIs, gráficos e rankings.

O botão de exportação abre a janela de impressão do navegador para salvar o relatório em PDF.

Entre as informações apresentadas estão:

- Usuários cadastrados e evolução por tipo de conta.
- Condomínios cadastrados, ativos e distribuição por estado.
- Volume de postagens e situação das validações.
- Rankings de condomínios e usuários por postagens.
- Tentativas, aprovações, notas e respostas dos quizzes.

### De onde vêm os dados

O pipeline executado no Databricks consulta o PostgreSQL do Neon e organiza os dados em Bronze, Silver e Gold. Depois dos testes e dos KPIs, o notebook `07_exporta_data_json` lê a Gold, monta e valida o JSON, grava uma cópia em um Volume e publica o `data.json` deste repositório pela API do GitHub.

O Job utiliza os notebooks `00` a `08` descritos abaixo. A pasta atual do repositório contém os arquivos do painel; a execução e as conexões são configuradas no workspace Databricks.

O `index.html` traz uma cópia dos dados embutida em `<script type="application/json" id="viz-data">`. Ela é usada sempre que a página é carregada. Quando a página é servida por HTTP(S), o script também busca o `data.json` (com `cache: 'no-store'`) e redesenha o painel com os dados mais recentes. Aberta direto do disco (`file://`), a página mostra só a cópia embutida.

### Formato do `data.json`

O contrato mantém **nove blocos**, **sete campos em `meta`** e **15 KPIs**:

- `meta`: `gerado_em`, `data_inicio`, `data_fim`, `periodo`, `atualizado`, `fonte` e `lakeview_url`.
- `kpis`: totais de usuários, cooperativas, condomínios, postagens, quizzes e nota média.
- `evolucao_usuarios`: `rotulos` e `series`, cada série com nome e valores.
- `condominios_ranking`: lista de `{ nome, valor }`, por postagens.
- `usuarios_ranking`: lista de `{ nome, valor }`, identificando usuários como `usuário <id>`.
- `condominios_por_uf`: quantidades de condomínios por UF.
- `postagens_por_dia`: valores para cada dia, com rótulos de início e fim do período.
- `quiz_por_dia`: rótulos e valores de quizzes **aprovados** por dia.
- `respostas_por_curso`: lista de `{ curso, total, corretas }`.

Sem `data_inicio` e `data_fim` nos parâmetros do exportador, a janela é obtida pela primeira e última data de postagem na Gold. Os parâmetros também aceitam um intervalo explícito em `AAAA-MM-DD`.

## Pré-requisitos

- Um navegador atualizado.
- Para ver o painel lendo o `data.json`, qualquer servidor HTTP estático. Os exemplos abaixo usam Python 3, mas a extensão Live Server do VS Code ou outro servidor também funcionam.

## Instalando

```bash
git clone https://github.com/EcoCiente-Instituto-J-F/ecociente-painel-bi.git
cd ecociente-painel-bi
```

Não há dependências para instalar.

## Usando

Sirva a pasta por HTTP para que a página leia o `data.json`:

```bash
python -m http.server 8000
```

No Windows, se o comando `python` não estiver no PATH:

```powershell
py -m http.server 8000
```

Depois abra `http://localhost:8000` no navegador. Para ir direto à visualização, use `http://localhost:8000/#visualizacao`.

Também é possível abrir o `index.html` com dois cliques, mas nesse caso o painel mostra apenas os dados embutidos no HTML.

## Validação

O painel não possui suíte automatizada de testes nem CI nesta pasta. O pipeline executa os testes de dados no notebook `05_testes_gold`. Após atualizar os dados ou alterar a página, confira:

- A atualização do `data.json` no repositório e se ele é um JSON válido (por exemplo, `python -m json.tool data.json`).
- A data de atualização exibida no painel.
- A correspondência dos indicadores com os resultados do Databricks.
- Os dois modos de leitura (Relatório e Visualização), em tela larga e estreita, e o console do navegador sem erros.

## Estrutura do projeto

```text
ecociente-painel-bi/
├── index.html                            # Página do painel: HTML, CSS e JS inline, com cópia embutida dos dados
├── data.json                             # Dados do painel, atualizados pelo pipeline do Databricks
├── Card README - bi-ecociente-painel.png # Imagem de capa deste README
├── README.md                             # Este documento
├── CONTRIBUTING.md                       # Como contribuir
├── CODE_OF_CONDUCT.md                    # Código de Conduta
├── SECURITY.md                           # Política de Segurança
└── LICENSE                               # Licença MIT
```

## Pipeline de dados no Databricks

```text
Neon — banco ecociente / schema public
  ├── 22 tabelas operacionais → Bronze → Silver
  └── 9 tabelas do datamart → cópias locais no Databricks
                                      │
                      Silver + datamart → Gold
                                          │
                                    Testes e KPIs
                                          │
                              data.json no Volume
                                          │
                          GitHub Contents API → painel
```

O fluxo **apenas consulta o Neon**. Ele cria/substitui tabelas, grava logs e exporta o JSON no Databricks; a tarefa final atualiza o arquivo configurado no GitHub.

O Job **não executa `sp_carga_datamart()`**. As nove tabelas do datamart precisam existir e estar carregadas no Neon antes da carga. A equipe responsável pelo banco deve manter esse processo de origem atualizado; agendar este Job não agenda a procedure.

### Função dos notebooks

- **`00_config`:** parâmetros, nomes dos destinos, conexão JDBC, contrato de origem e funções de log. É carregado pelas tarefas `01` a `06` com `%run ./00_config`.
- **`01_datamart`:** valida a conexão, o banco `ecociente` e `SELECT 1`; recusa `refresh_datamart=true` nesta versão.
- **`02_bronze`:** audita e copia as 22 tabelas operacionais, incluindo metadados da carga.
- **`03_silver`:** valida chaves e campos obrigatórios, registra inconsistências, aplica limpeza e tipagem e exclui registros inválidos. Não leva `senha_hash` para a Silver.
- **`04_gold`:** copia o datamart e cria dimensões e fatos, complementando campos pela Silver e conferindo as dependências de colunas.
- **`05_testes_gold`:** valida grãos, referências, medidas e ausência de colunas sensíveis. Falhas interrompem o fluxo antes dos KPIs e da publicação.
- **`06_kpis_encerramento`:** calcula os KPIs por condomínio, categoria e mês, com registros de execução.
- **`07_exporta_data_json`:** monta e valida o JSON, grava no Volume e publica no GitHub. No Job, ausência de token ou falha na publicação gera falha da tarefa.
- **`08_job_auto`:** configura o Secret do GitHub e cria ou atualiza o Job desta versão; não inicia a carga.

Notebooks `09` de auditoria são diagnósticos separados e não devem ser adicionados à sequência diária.

### Requisitos de execução

- Workspace Databricks com Unity Catalog e **Serverless de notebooks**, que executa Python e SQL. O SQL Starter Warehouse não executa as células Python deste fluxo.
- Acesso aos notebooks, à conexão, aos schemas/tabelas, ao Volume e ao Secret.
- Permissão para criar/administrar o Job. A identidade de **Run as** também precisa acessar os recursos das tarefas.
- Banco `ecociente`, schema `public`, com as tabelas operacionais e o datamart carregados.
- Repositório GitHub com a branch configurada, commit inicial e token autorizado para escrita.

A execução do Job não depende de MCP nem de manter o navegador aberto.

## Configuração inicial do Job

### 1. Importar os notebooks

Importe os arquivos `.py` do pacote como **notebooks Databricks**, mantendo todos na mesma pasta e sem sufixos como `(1)` nos nomes.

No `08`, confira `CONFIG_PATH` e os caminhos das sete tarefas em `JOB_TEMPLATE`. Eles devem apontar para a pasta importada. Por exemplo:

```text
/Workspace/Users/SEU_EMAIL/ecociente_fluxo_completo/00_config
```

Os arquivos atuais foram preparados para uma pasta de usuário específica; adapte os caminhos antes de usar outro workspace ou pasta. Os `%run ./00_config` continuam relativos à pasta compartilhada. O criador resolve nomes sem extensão, com `.py` ou `.ipynb`, mas rejeita caminhos ausentes ou ambíguos.

Copiar os notebooks não transfere a conexão, os Secrets, os dados nem as permissões de outro workspace.

### 2. Conferir a conexão Neon

O nome mantido no código é **`aiven_ecociente_jdbc`**, mas a conexão agora deve apontar para o **Neon**. O nome antigo foi preservado por compatibilidade.

Formato da URL, sem usuário e senha:

```text
jdbc:postgresql://SEU_HOST_DO_NEON:5432/ecociente?sslmode=require&channelBinding=require
```

Use o host/porta da branch correta e o banco **`ecociente`**, não o banco padrão `neondb`. Configure usuário e senha nas opções da conexão Unity Catalog. Mantenha o JAR PostgreSQL e o driver `org.postgresql.Driver`, com `dbtable` e `numPartitions` permitidos na lista de opções externas.

Se mudar o nome da conexão, ajuste `jdbc_connection` nas tarefas do `08` e o padrão correspondente no `00`. [Documentação JDBC](https://docs.databricks.com/aws/en/connect/jdbc-connection)

Para testar em uma célula Python:

```python
(
    spark.read.format("jdbc")
    .option("databricks.connection", "aiven_ecociente_jdbc")
    .option(
        "dbtable",
        "(SELECT current_database() AS banco, current_user AS usuario, 1 AS teste) AS t"
    )
    .load()
    .show()
)
```

O resultado deve indicar `banco = ecociente` e `teste = 1`. Esse teste não altera o banco.

As nove tabelas exigidas do datamart são:

```text
dim_tempo, dim_usuario, dim_condominio, dim_cooperativa,
dim_categoria_residuo, dim_status_postagem, dim_status_agendamento,
fato_postagens, fato_coletas
```

O `04` cria cópias `_datamart_*` na Bronze e faz os joins seguintes localmente. Cópias só são reutilizadas com o mesmo `run_id`; uma nova carga refaz as cópias.

### 3. Configurar o repositório e o token

O destino deste painel é:

```text
github_repo: EcoCiente-Instituto-J-F/ecociente-painel-bi
github_branch: main
github_path: data.json
```

**Revise o `github_repo` da tarefa `07` em `JOB_TEMPLATE`, no `08`, e o padrão de `GH_REPO` no `07`. Pacotes anteriores usam `bi-ecociente-painel`; atualizar este README não altera os parâmetros dos notebooks ou de um Job já criado.** Depois de ajustar os arquivos, execute o `08` para aplicar as configurações ao Job.

Crie um token GitHub fine-grained com a organização como **Resource owner**, acesso somente a este repositório e **Contents: Read and write**. A conta do token também deve ter escrita no repositório. Se estiver pendente, um proprietário da organização precisa aprová-lo. Acompanhe a validade para renovar antes de expirar. [Documentação dos tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens)

Na sua cópia do `08` dentro do Databricks, se o Secret ainda não estiver configurado, preencha:

```python
GITHUB_TOKEN = "COLE_O_TOKEN_SOMENTE_NO_DATABRICKS"
```

Ao executar, o `08` salva o valor no scope **`ecociente-github`**, chave **`GITHUB_TOKEN`**. É necessário poder criar o scope, se ausente, e gravar o Secret. Esse procedimento não exige terminal.

Depois, remova o valor **do texto da célula**, salve o notebook e mantenha:

```python
GITHUB_TOKEN = ""
```

O Secret será reutilizado nas execuções seguintes. Limpar a variável na memória não apaga o token do texto nem de históricos anteriores. O widget `github_token` do `07` tem prioridade sobre o Secret: deixe-o vazio para evitar usar um token antigo.

## Executar o Job

1. Confira caminhos, conexão, repositório e credenciais.
2. No `08`, deixe inicialmente `ATIVAR_AGENDAMENTO = False`.
3. Execute **Run all** no `08`.
4. Abra o link exibido para o Job **`ecociente_fluxo_completo_diario`**.
5. Confira as sete tarefas e clique em **Run now**.
6. Acompanhe o resultado na aba **Runs**.

```text
01_datamart → 02_bronze → 03_silver → 04_gold
    → 05_testes_gold → 06_kpis_encerramento → 07_exporta_data_json
```

Não é necessário rodar cada notebook manualmente. O `00` é compartilhado, não uma tarefa separada; o `08` apenas configura o Job. Todas as tarefas recebem o mesmo `run_id = {{job.run_id}}` para correlacionar logs e cópias. Execuções avulsas sem o parâmetro podem gerar IDs diferentes.

O `08` atualiza apenas um Job compatível com a versão e a pasta esperadas. Se encontrar configuração incompatível ou mais de um Job com o mesmo nome, interrompe a configuração para revisão.

### Conferir o resultado

Confirme as sete tarefas concluídas, os testes do `05`, os KPIs do `06` e a publicação do `07`. O encerramento registrado no `06` sozinho não comprova que o JSON foi publicado.

Saída padrão:

```text
/Volumes/workspace/ecociente_gold/exports/data.json
```

Também é gravado `data_AAAAMMDD.json`. Essa cópia é diária: uma segunda exportação no mesmo dia substitui o arquivo daquele dia.

O `07` consulta o SHA atual e cria/atualiza o arquivo pela API de Contents do GitHub. Conteúdo idêntico não gera outro commit; porém, a data de atualização em `meta` normalmente muda entre exportações. Se a publicação falhar, o JSON pode já estar salvo no Volume.

O `07` não publica o `index.html`. Mantenha o HTML e o JSON na mesma pasta. Recarregue o painel depois da atualização. Se o carregamento do JSON falhar, o HTML pode continuar mostrando os dados de referência embutidos sem aviso explícito; confira o arquivo no GitHub e a atualização exibida.

### Destinos e logs

No catálogo `workspace`, os destinos padrão são `ecociente_bronze`, `ecociente_silver`, `ecociente_gold` e `ecociente_quality`. O exportador usa o parâmetro `catalog`; as tarefas anteriores usam `target_catalog`.

Os principais controles estão em:

```text
workspace.ecociente_quality.pipeline_execution_log
workspace.ecociente_quality.data_quality_log
workspace.ecociente_quality.invalid_keys
```

Filtre por `run_id` para investigar uma carga. Reparações podem acrescentar entradas com o mesmo ID no histórico de execução.

As tabelas de destino são criadas/substituídas conforme os notebooks. O fluxo não é incremental e não preserva automaticamente um histórico completo das versões de todas as tabelas.

## Ativar o agendamento automático

Após a primeira execução e publicação bem-sucedidas, altere no `08`:

```python
ATIVAR_AGENDAMENTO = True
```

Execute **Run all** novamente. O Secret existente pode ser reutilizado com o token vazio no texto.

A configuração preparada é **todos os dias às 12h10**, fuso **`America/Sao_Paulo`**, uma execução simultânea e alerta de falha para **`ecociente.jef@gmail.com`**.

Para manter esse horário ao recriar ou atualizar o Job pelo `08`, confira em `JOB_TEMPLATE` a expressão Quartz `0 10 12 * * ?`. Executar o criador reaplica o horário salvo no código, mesmo que o agendamento tenha sido alterado pela interface.

No Job, confira **Schedules & Triggers**, estado **Active** e a próxima execução. É possível retomar o agendamento por **Resume** na interface. [Agendamentos do Databricks](https://docs.databricks.com/aws/en/jobs/triggers)

**O `08` exportado começa com `ATIVAR_AGENDAMENTO=False`. Rodá-lo assim volta a pausar o Job, mesmo que você tenha ativado o schedule pela interface.** Preserve o estado desejado ao atualizar configurações ou trocar o token.

O valor salvo no repositório não comprova o estado atual do Job; confira sempre o workspace.

## Reparar uma execução com falha

1. Abra **Jobs & Pipelines → ecociente_fluxo_completo_diario → Runs**.
2. Entre na execução que falhou e leia o erro da tarefa.
3. Corrija a causa: código, dados, configuração ou acesso.
4. Na execução original, clique em **Repair run**.
5. Confira as tarefas com falha e as dependentes que serão repetidas e confirme.

A reparação reexecuta a tarefa desde o início, com as configurações atuais. Preserve os parâmetros e o `run_id` da carga para reaproveitar as cópias correspondentes. [Reparação de Jobs](https://docs.databricks.com/aws/en/jobs/repair-job-failures)

Use **Run now** para iniciar uma carga nova. Se só o `07` falhou por token/permissão, normalmente basta reparar essa tarefa após corrigir o acesso. Não é necessário rodar o `08` para reparar uma falha de dados.

## Compatibilidade da Gold com o Neon

A versão auditada preserva os campos consumidos pelos testes, KPIs e JSON com estes mapeamentos:

- `registro_em`: data de cadastro obtida de `Silver.tb_usuarios`, por `id_usuario`, sem usar a data de carga.
- Atividade da cooperativa: obtida pelo vínculo `tb_cooperativas.usuario_id → tb_usuarios.ativo`. Nesse indicador, cooperativa ativa significa usuário associado ativo.
- Descrição e cor da categoria: lookup de categorias da Silver.
- `limite_pontos_diario`: alias de `dim_categoria_residuo.limite_pontos_ciclo`; a procedure fornecida copia aquele campo operacional para esse destino.
- Nome do status da postagem: lido de `dim_status_postagem.codigo`, preenchido com o nome operacional do status.
- Pontos teóricos: lidos de `fato_postagens.pontos_base_categoria`, inclusive para postagens não aprovadas, conforme a procedure. Isso não comprova crédito efetivo ao usuário.

As complementações por `LEFT JOIN` preservam as linhas do datamart; campos sem correspondência ficam como `NULL`, sem valor inventado. A existência das colunas não garante a cobertura dos vínculos, a atualidade dos dados ou a validade das contagens.

A lógica de referência da `fato_pontuacao`, com `credito_confirmado=false`, é mantida. A procedure foi analisada para confirmar os mapeamentos; não é executada por este fluxo.

## Problemas comuns

- **Não rodou às 12h10:** confira estado do schedule, fuso e próxima execução. Rodar manualmente não ativa o agendamento.
- **GitHub 403:** confira acesso de escrita da conta, proprietário e repositório do token, `Contents: Read and write`, aprovação da organização e eventual token antigo no widget do `07`.
- **Token expirado:** salve o novo pelo `08`, mantendo o estado desejado do agendamento; depois repare o `07` e remova o token do texto da célula.
- **Conexão não encontrada:** confira o nome e o workspace. Conexões e permissões não acompanham uma cópia dos notebooks.
- **Conecta, mas não encontra tabelas:** confira host, branch, banco `ecociente` e schema `public`.
- **Faltam tabelas do datamart:** solicite a carga à equipe responsável pelo banco; o Job não chama a procedure.
- **Sem vagas de conexão:** reduza suas consultas simultâneas e repare após liberar capacidade. O helper tenta até três vezes para esse tipo de falha. Não encerre sessões de terceiros.
- **Coluna não resolvida:** compare o contrato com os schemas reais antes de substituir por um nome semelhante.
- **`pg_dim_*` não encontrada em outra sessão:** essas views são temporárias. Inspecione as cópias persistidas `workspace.ecociente_bronze._datamart_*` ou repare a tarefa inteira.
- **Caminho ambíguo no `08`:** confira versões importadas com o mesmo nome lógico e os caminhos das tarefas.

## Contribuindo

Leia o [CONTRIBUTING.md](CONTRIBUTING.md) para saber como preparar o ambiente, validar sua mudança e abrir um pull request. Ao participar, você concorda em seguir o [Código de Conduta](CODE_OF_CONDUCT.md).

## Segurança

Encontrou uma vulnerabilidade? Siga o [SECURITY.md](SECURITY.md). **Não abra issue pública.**

Não versione senhas, tokens, credenciais da conexão ou valores exportados dos Secrets. Antes de publicar notebooks, deixe `GITHUB_TOKEN=""` no texto do `08`, o token manual do `07` vazio e limpe resultados e metadados sensíveis de widgets.

Nomes de conexões, Secret Scopes e chamadas a `dbutils.secrets.get(...)` não são os valores das credenciais. Se uma credencial real for publicada, substitua/revogue o valor; removê-lo do arquivo atual não o apaga do histórico.

O `data.json` publicado fica disponível aos visitantes. Inclua apenas informações autorizadas para divulgação. Reveja também e-mails e caminhos pessoais nos arquivos que serão públicos.

## Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE).
