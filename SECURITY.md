# Política de Segurança

O bi-ecociente-painel é uma página estática que exibe dados agregados da plataforma EcoCiente: totais de usuários, condomínios, postagens e quizzes, além de rankings em que os usuários aparecem por identificador anônimo (`usuário <id>`). O `data.json` é publicado neste repositório pelo pipeline do Databricks, por meio da API do GitHub.

## Versões suportadas

| Versão | Suportada |
| --- | --- |
| `main` (último commit) | :white_check_mark: |
| Commits anteriores | :x: |

## Reportando uma vulnerabilidade

**Não** abra issue, pull request ou discussão pública sobre a vulnerabilidade.

Reporte de forma privada pelo GitHub: <https://github.com/EcoCiente-Instituto-J-F/bi-ecociente-painel/security/advisories/new> (aba **Security** → **Advisories** → **Report a vulnerability**).

> Mantenedores: esse formulário só aparece se o *Private vulnerability reporting* estiver ativado em **Settings** → **Code security**.

Inclua no relato:

- O impacto: o que uma pessoa mal-intencionada consegue ver ou fazer.
- Os passos para reproduzir, usando apenas dados falsos.
- O commit testado, o navegador e como a página foi servida (HTTP, `file://`, GitHub Pages etc.).
- Uma ideia de correção, se você tiver.

O que acontece depois:

1. Confirmamos o recebimento do relato.
2. Investigamos e corrigimos de forma privada.
3. Publicamos o advisory com o crédito a você, se quiser.

Pedimos que você aguarde um prazo razoável para a correção antes de divulgar qualquer detalhe publicamente.

## Escopo

**Dentro do escopo:**

- Execução de script (XSS) na página a partir de conteúdo do `data.json` ou da URL.
- Exposição de dados pessoais no `data.json`, no `index.html` ou no histórico do Git (nomes, e-mails, telefones, endereços ou identificadores que permitam reidentificar usuários).
- Segredos commitados, como tokens do GitHub usados pelo pipeline, credenciais do Databricks ou links privados.
- Formas de alterar o `data.json` publicado sem passar pelo pipeline ou pela revisão dos mantenedores.

**Fora do escopo:**

- Bugs visuais ou de cálculo sem impacto de segurança (abra uma issue normal).
- Falhas em serviços de terceiros, como Google Fonts, GitHub ou Databricks.
- Ataques de negação de serviço contra o servidor onde você hospeda a página.

## Como o sistema protege os dados

| Camada | Onde | O que faz |
| --- | --- | --- |
| Anonimização | `data.json` → `usuarios_ranking` | Usuários aparecem como `usuário <id>`, sem nome ou contato |
| Agregação | `data.json` → `kpis`, `condominios_por_uf`, séries diárias | O painel recebe apenas contagens e médias, não registros individuais |
| Escape de texto | `index.html`, função `esc()` | Escapa `&`, `<` e `>` antes de inserir alguns textos no relatório |
| Links externos | `index.html`, link do Lakeview | Abre com `target="_blank" rel="noopener"` |

O escape ainda não cobre todos os pontos que inserem dados no HTML (veja o checklist abaixo).

## Checklist de deploy

- Publique apenas `data.json` gerado pelo pipeline e conferido contra o Databricks.
- Guarde o token do GitHub usado pelo notebook em um secret scope do Databricks, nunca no código do notebook nem neste repositório. Use um token com permissão mínima, restrito a este repositório.
- Confira que o `data.json` não contém dados pessoais antes de cada publicação.
- Preencha `meta.lakeview_url` apenas com uma URL `https://` confiável.
- Trate o `data.json` como entrada confiável apenas enquanto o modo **Visualização** inserir `nome`, `curso`, `fonte`, `atualizado` e `lakeview_url` no HTML sem passar por `esc()`. Corrigir isso é a melhoria de segurança prioritária.
