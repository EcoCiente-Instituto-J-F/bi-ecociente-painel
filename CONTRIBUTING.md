# Contribuindo com o bi-ecociente-painel

Ao participar, você concorda em seguir o [Código de Conduta](CODE_OF_CONDUCT.md). Para reportar um problema de segurança, **não** abra uma issue — siga o [SECURITY.md](SECURITY.md).

## Idioma

Issues, pull requests, mensagens de commit, comentários no código e textos do painel são escritos em português do Brasil.

## Configurando o ambiente

O projeto é uma página estática, sem dependências nem etapa de build.

```bash
git clone https://github.com/EcoCiente-Instituto-J-F/bi-ecociente-painel.git
cd bi-ecociente-painel
python -m http.server 8000   # no Windows: py -m http.server 8000
```

Abra `http://localhost:8000`. Sirva a pasta por HTTP: aberta direto do disco (`file://`), a página não lê o `data.json` e mostra só os dados embutidos no HTML.

## Testando sua mudança

Não há testes automatizados nem CI neste repositório. Antes de abrir o pull request, valide manualmente:

- O `data.json` continua sendo um JSON válido: `python -m json.tool data.json`.
- A página carrega servida por HTTP, sem erros no console do navegador.
- Os modos **Relatório** e **Visualização** (`#visualizacao`) mostram os números esperados e batem com o `data.json`.
- O layout funciona em tela larga e em tela estreita (celular).
- A exportação para PDF (janela de impressão) continua legível.

Descreva no pull request o que você conferiu. Use apenas dados fictícios ou já agregados, nunca dados de produção que identifiquem pessoas.

## Regras do projeto

- **Sem dependências novas sem discussão.** Hoje a página usa só HTML, CSS e JavaScript puro, com gráficos em SVG. O único recurso externo são as fontes do Google Fonts. Abra uma issue antes de propor uma biblioteca, CDN ou etapa de build.
- **Os números vêm dos dados.** Todo número, percentual ou frase que depende de número é gerado pelo script a partir dos dados. Não escreva valores fixos no texto do HTML.
- **Formato do `data.json` estável.** O arquivo é gerado pelo notebook de exportação no Databricks. Se mudar uma chave ou estrutura, a mudança precisa ser feita também no notebook, e a cópia embutida em `<script type="application/json" id="viz-data">` no `index.html` deve seguir o mesmo formato.
- **Não edite o `data.json` à mão para publicar dados reais.** Ele é atualizado pelo pipeline. Alterações manuais servem apenas para testes locais e não devem ir para o pull request.
- **Nenhum dado pessoal.** Os rankings usam identificadores anônimos (`usuário <id>`). Não inclua nomes, e-mails, telefones ou endereços de pessoas no `data.json`, no HTML ou em capturas de tela.
- **Nenhum segredo no repositório.** Tokens do GitHub, credenciais do Databricks e links privados não entram em commits.
- **Escape o texto vindo dos dados.** Ao inserir valores do JSON no HTML, use a função `esc()` já existente no script.

## Pull requests

1. Crie uma branch a partir da `main` atualizada.
2. Faça commits focados, com mensagens no formato `prefixo: descrição`, usando `feat:`, `fix:`, `chore:`, `docs:`, `test:` ou `refactor:`.
3. Abra o pull request para a `main` explicando o que mudou e por quê, com capturas de tela quando a mudança for visual.
4. Aguarde a revisão de um mantenedor antes do merge.

## Reportando bugs e propondo funcionalidades

Abra uma [issue](https://github.com/EcoCiente-Instituto-J-F/bi-ecociente-painel/issues) com:

- **Bug:** o que aconteceu, o que era esperado, navegador e versão, modo de leitura (Relatório ou Visualização), passos para reproduzir e, se possível, uma captura de tela.
- **Funcionalidade:** o problema que ela resolve, quem usa a informação e, se for um novo indicador, de qual dado da camada Gold ele viria.
