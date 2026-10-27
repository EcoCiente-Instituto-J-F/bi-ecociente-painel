# bi-ecociente-painel

<p align="center">
  <img src="Card%20README%20-%20bi-ecociente-painel.png" alt="bi-ecociente-painel: Painel de Business Intelligence do EcoCiente" width="100%">
</p>

![HTML5](https://img.shields.io/badge/HTML5-E34F26?style=for-the-badge&logo=html5&logoColor=white)
![CSS](https://img.shields.io/badge/CSS-663399?style=for-the-badge&logo=css&logoColor=white)
![JavaScript](https://img.shields.io/badge/JavaScript-F7DF1E?style=for-the-badge&logo=javascript&logoColor=black)
![GitHub repo size](https://img.shields.io/github/repo-size/EcoCiente-Instituto-J-F/bi-ecociente-painel?style=for-the-badge)
![GitHub last commit](https://img.shields.io/github/last-commit/EcoCiente-Instituto-J-F/bi-ecociente-painel?style=for-the-badge)
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

O notebook de exportação, que fica no Databricks e não neste repositório, lê a camada Gold, monta e valida o JSON e salva uma cópia em um Volume. Quando a publicação está configurada, ele também atualiza o `data.json` deste repositório pela API do GitHub.

O `index.html` traz uma cópia dos dados embutida em `<script type="application/json" id="viz-data">`. Ela é usada sempre que a página é carregada. Quando a página é servida por HTTP(S), o script também busca o `data.json` (com `cache: 'no-store'`) e redesenha o painel com os dados mais recentes. Aberta direto do disco (`file://`), a página mostra só a cópia embutida.

### Formato do `data.json`

| Chave | Conteúdo |
| --- | --- |
| `meta` | Data de geração, início e fim do período, rótulo do período, texto de atualização, fonte e `lakeview_url` (link opcional para o Lakeview) |
| `kpis` | Totais de usuários, cooperativas, condomínios, postagens, quizzes e nota média |
| `evolucao_usuarios` | `rotulos` e `series` (nome e valores) por tipo de conta |
| `condominios_ranking` | Lista de `{ nome, valor }` com postagens por condomínio |
| `usuarios_ranking` | Lista de `{ nome, valor }` com postagens por usuário (identificados como `usuário <id>`) |
| `condominios_por_uf` | Mapa UF → quantidade de condomínios |
| `postagens_por_dia` | `rotulos` e `valores` diários de postagens |
| `quiz_por_dia` | `rotulos` e `valores` de quizzes respondidos |
| `respostas_por_curso` | Lista de `{ curso, total, corretas }` |

## Pré-requisitos

- Um navegador atualizado.
- Para ver o painel lendo o `data.json`, qualquer servidor HTTP estático. Os exemplos abaixo usam Python 3, mas a extensão Live Server do VS Code ou outro servidor também funcionam.

## Instalando

```bash
git clone https://github.com/EcoCiente-Instituto-J-F/bi-ecociente-painel.git
cd bi-ecociente-painel
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

O repositório não tem testes automatizados nem CI. Após atualizar os dados ou alterar a página, confira:

- A atualização do `data.json` no repositório e se ele é um JSON válido (por exemplo, `python -m json.tool data.json`).
- A data de atualização exibida no painel.
- A correspondência dos indicadores com os resultados do Databricks.
- Os dois modos de leitura (Relatório e Visualização), em tela larga e estreita, e o console do navegador sem erros.

## Estrutura do projeto

```text
bi-ecociente-painel/
├── index.html                            # Página do painel: HTML, CSS e JS inline, com cópia embutida dos dados
├── data.json                             # Dados do painel, atualizados pelo pipeline do Databricks
├── Card README - bi-ecociente-painel.png # Imagem de capa deste README
├── README.md                             # Este documento
├── CONTRIBUTING.md                       # Como contribuir
├── CODE_OF_CONDUCT.md                    # Código de Conduta
├── SECURITY.md                           # Política de Segurança
└── LICENSE                               # Licença MIT
```

## Contribuindo

Leia o [CONTRIBUTING.md](CONTRIBUTING.md) para saber como preparar o ambiente, validar sua mudança e abrir um pull request. Ao participar, você concorda em seguir o [Código de Conduta](CODE_OF_CONDUCT.md).

## Segurança

Encontrou uma vulnerabilidade? Siga o [SECURITY.md](SECURITY.md). **Não abra issue pública.**

## Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE).
