## ecociente-painel-bi
Painel de Business Intelligence do EcoCiente para acompanhar o engajamento dos usuários, a participação dos condomínios, as postagens e o desempenho nos quizzes. Desenvolvido em HTML, CSS e JavaScript, apresenta indicadores, gráficos e rankings alimentados por um arquivo JSON gerado pelo pipeline de dados no Databricks

## Sobre o painel

Desenvolvido em HTML, CSS e JavaScript, o painel apresenta indicadores, gráficos e rankings a partir de um arquivo `data.json` gerado pelo pipeline de dados no Databricks.

Entre as informações apresentadas estão:

- Usuários cadastrados e evolução por tipo de conta.
- Condomínios cadastrados, ativos e distribuição por estado.
- Volume de postagens e situação das validações.
- Rankings de condomínios e usuários por postagens.
- Tentativas, aprovações, notas e respostas dos quizzes.

## Estrutura do repositório

```text
ecociente-painel-bi/
├── index.html         # Página do painel
├── data.json          # Dados utilizados pelo painel
└── README.md          # Documentação
```

## Atualização dos dados

O notebook de exportação lê a camada Gold do Databricks, monta e valida o JSON e salva uma cópia em um Volume. Quando a publicação está configurada, também atualiza o `data.json` neste repositório pela API do GitHub.

Ao abrir ou recarregar a página, o painel busca esse arquivo e atualiza os indicadores, gráficos e textos associados aos dados.

## Validação

Após atualizar os dados, confira:

- A atualização do `data.json` no repositório.
- A data de atualização exibida no painel.
- A correspondência dos indicadores com os resultados do Databricks.