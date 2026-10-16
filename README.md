# deploy_metrics

Mede entregas em produção em ambiente **ativo-ativo** a partir da API do GitLab (self-managed), usando `python-gitlab`.

Para cada pipeline do período, o script calcula:

| Métrica | Definição |
|---|---|
| **Deploy completo** | Todos os jobs de `--deploy-jobs` (padrão: `deploy-prd-bc` **e** `deploy-prd-mt`) com a última tentativa em `success`. Se só um cluster entregou, não conta. |
| **Rollback único** | Um ou mais jobs cujo nome casa com `--rollback-regex` (padrão `^rollback`) com `success`. Vários jobs de rollback no mesmo pipeline (um por cluster) contam como **1**. |
| **Entrega de primeira** | Deploy completo sem retry manual nos jobs de deploy. |
| **Entrega com retry manual** | Deploy completo com pelo menos um retry manual, **mesmo que a tentativa anterior tenha dado sucesso**. |
| **Frequência de deploy** | Deploys completos por grupo e para o GitLab todo, usando a **data real do deploy** (`finished_at` do último job de deploy concluído), no fuso `--tz`. Detalhes na seção própria abaixo. |
| **Cruzamento** | Cada deploy completo cai em um de quatro grupos: `primeira`, `primeira_com_rollback`, `retry_manual`, `retry_manual_com_rollback`. |

## Requisitos

- Python 3.9+
- Token do GitLab com escopo **`read_api`** (pessoal, de projeto ou de grupo) e acesso de leitura aos projetos
- Acesso de rede à URL do GitLab

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Execução

```bash
export GITLAB_URL=https://gitlab.suaempresa.com.br
export GITLAB_TOKEN=glpat-xxxxxxxxxxxxxxxxxxxx

python deploy_metrics.py --out ./saida          # varre TODOS os projetos do GitLab
```

Sem `--project` nem `--group`, o script lista todos os projetos visíveis ao token (não arquivados e com atividade desde `--since`), consulta os pipelines do período em cada um e ignora os que não têm jobs de produção. Um token de administrador enxerga a instância inteira; com token comum, só os projetos que ele alcança. Projetos sem permissão, com CI desabilitado, sem pipelines ou com erro de API são registrados e a varredura continua.

Padrões: período `2026-01-01` a `2026-09-30`, jobs `deploy-prd-bc,deploy-prd-mt`.

### Exemplos

```bash
# Só um grupo (inclui subgrupos)
python deploy_metrics.py --group plataforma/pagamentos

# Vários projetos específicos (ID numérico ou path), somente a branch main
python deploy_metrics.py --project 123 --project grupo/app-b --ref main

# Varredura total mais completa (não descarta projetos pela última atividade) e mais rápida
python deploy_metrics.py --all-activity --project-workers 8

# Outro período
python deploy_metrics.py --project 123 --since 2026-04-01 --until 2026-06-30

# Definir deploy completo com os quatro jobs e outro padrão de rollback
python deploy_metrics.py --project 123 \
  --deploy-jobs deploy-prd,deploy-prd-bc,deploy-prd-mt,deploy-prd-dr \
  --rollback-regex '^(rollback|revert)-prd'
```

### Regex em vez de nomes de job

```bash
python deploy_metrics.py --deploy-regex '^deploy[-_]prd' --rollback-regex '^rollback[-_]prd'
```

- O regex usa `re.search`; ancore com `^` para não casar nomes inesperados. `[-_]` cobre hífen e underscore.
- **Retries são contados normalmente:** cada retry é uma nova tentativa do mesmo nome de job, então vale para qualquer job que o regex capture, inclusive nomes de `parallel:matrix` como `deploy_prd: [bc]`.
- **Deploy completo com regex:** pelo menos `--deploy-min-jobs` jobs casados com sucesso na última tentativa e nenhum dos que rodaram falhou. Jobs manuais nunca acionados (`manual`/`skipped`, por exemplo um `deploy-prd-dr` opcional) são ignorados. Com `--deploy-min-jobs 1`, um deploy em um só cluster passa a contar como completo.
- Jobs que casam com o regex de rollback nunca são tratados como deploy, mesmo que o nome também case com o regex de deploy.
- `--deploy-regex` e `--deploy-jobs` não se combinam; o regex tem prioridade.

### GitLab com certificado autoassinado

Duas opções:

```bash
# 1) Desativar a verificação SSL (os warnings são suprimidos)
python deploy_metrics.py --project 123 --insecure
export GITLAB_INSECURE=1        # equivalente, sem repetir a flag

# 2) Manter a verificação, apontando para a CA interna (recomendado)
export REQUESTS_CA_BUNDLE=/caminho/ca-interna.pem
```

> Com `--insecure`, o token trafega numa conexão sem verificação de identidade do servidor e fica exposto a interceptação. Prefira a opção 2 sempre que possível.

## Parâmetros

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--url` | `$GITLAB_URL` | URL do GitLab |
| `--token` | `$GITLAB_TOKEN` | Token com `read_api` |
| `--project` | (todos) | ID ou path do projeto; repetível. Sem `--project`/`--group`, varre o GitLab todo |
| `--group` | (nenhum) | ID ou path de um grupo, incluindo subgrupos |
| `--membership` | desligado | Na varredura total, só projetos dos quais o token é membro |
| `--include-archived` | desligado | Inclui projetos arquivados |
| `--all-activity` | desligado | Não filtra projetos por `last_activity_at >= --since` (mais lento, mais completo) |
| `--since` / `--until` | `2026-01-01` / `2026-09-30` | Período, formato `AAAA-MM-DD`, inclusivo, pela data de **criação do pipeline** |
| `--deploy-jobs` | `deploy-prd-bc,deploy-prd-mt` | Jobs que, juntos, definem um deploy completo |
| `--deploy-regex` | (nenhum) | Alternativa a `--deploy-jobs`: regex dos jobs de deploy (`re.search`), ex.: `'^deploy[-_]prd'` |
| `--deploy-min-jobs` | `2` | Com `--deploy-regex`: mínimo de jobs com sucesso para o deploy ser completo (2 = os dois clusters) |
| `--rollback-regex` | `^rollback` | Regex dos jobs de rollback |
| `--ref` | (todas) | Filtra por branch/tag |
| `--auto-retry-gap` | `60` | Segundos; retry mais rápido que isso é tratado como automático |
| `--workers` | `4` | Threads por projeto (uma chamada de jobs por pipeline) |
| `--project-workers` | `4` | Projetos processados em paralelo |
| `--insecure` | desligado | Desativa verificação SSL e suprime warnings (`GITLAB_INSECURE=1`) |
| `--tz` | `America/Sao_Paulo` | Fuso para datas e semanas da frequência de deploy |
| `--sem-revertidos` | desligado | Frequência: exclui deploys seguidos de rollback |
| `--out` | `.` | Diretório de saída |

## Saídas

Em `--out`:

- **`pipelines.csv`**: uma linha por pipeline relevante (com deploy ou rollback): projeto, id, URL, ref, mês, `deploy_completo`, `entrega` (`primeira`/`retry_manual`), `retries_manuais`, `retries_apos_sucesso`, `rollback` (`success`/`failed`/`none`). Serve para auditar cada caso pelo link.
- **`frequencia_deploy.csv`**: frequência de deploy por escopo (ver abaixo).
- **`frequencia_semanal.csv`**: deploys por semana ISO (`AAAA-Www`) e por escopo, para gráficos de tendência. Semanas sem deploy não aparecem.
- **`erros.csv`**: projetos sem acesso/CI desabilitado/erro de API e pipelines cujos jobs não puderam ser lidos (projeto, pipeline, motivo).
- **`resumo_mensal.csv`**: totais por mês e linha `TOTAL`, com os quatro grupos do cruzamento, `rollbacks_unicos`, `rollbacks_sem_deploy_completo`, `rollbacks_falhos`, `pct_retry_manual` e `pct_rollback`.
- Resumo no terminal:

```
Projetos: N listados | N com deploy/rollback | N sem pipelines no período | N sem jobs de produção | N sem acesso/CI desabilitado | N com erro
Deploys completos ........ N
  entregues de primeira .. N  (com rollback: N)
  com retry manual ....... N  (com rollback: N)
    retries após sucesso . N de N retries manuais
Rollbacks únicos ......... N  (sem deploy completo: N, falhos: N)
```

## Frequência de deploy por grupo e no GitLab todo

`frequencia_deploy.csv` tem uma linha por escopo: `(GITLAB TODO)` (nível 0) e cada grupo/subgrupo (nível 1, 2, ...). O cálculo é em cascata: um deploy em `pay/core/api` conta em `pay` e em `pay/core`. Projetos fora de grupos (namespaces de usuário) aparecem como um grupo com o nome do usuário.

| Coluna | Significado |
|---|---|
| `deploys` | Deploys completos no período |
| `deploys_revertidos` / `deploys_mantidos` | Quantos tiveram rollback no mesmo pipeline e quantos não |
| `projetos_com_deploy` | Projetos distintos que entregaram |
| `dias_com_deploy` | Dias distintos com ao menos um deploy |
| `deploys_por_dia` / `deploys_por_semana` | `deploys / dias corridos do período` |
| `pct_semanas_com_deploy` | Consistência: % das semanas do período com pelo menos um deploy |
| `intervalo_mediano_horas` | Mediana do tempo entre deploys consecutivos |
| `classificacao_dora` | Faixa pela média do período |

Faixas (aproximação das métricas DORA, sobre dias corridos): **Elite** > 1 deploy/dia; **Alto** entre 1/dia e 1/semana; **Médio** entre 1/semana e 1/mês; **Baixo** menos de 1/mês.

- Só entram deploys completos cuja data real caiu entre `--since` e `--until`. Um pipeline criado antes de `--since` e concluído dentro do período não é coletado, porque a busca de pipelines usa a data de criação.
- Por padrão, deploys revertidos contam. Use `--sem-revertidos` para medir só o que ficou em produção.
- A média por dia usa o período todo como denominador. Em grupos que começaram a entregar no meio do período, a frequência fica subestimada; compare com `frequencia_semanal.csv`.

## Como o retry manual é identificado

A API do GitLab **não informa** se um retry foi manual ou automático. Cada retry vira um novo job com o mesmo nome no pipeline (`include_retried=true` traz todos). O script classifica a tentativa como manual quando ocorre qualquer um dos casos abaixo:

1. a tentativa anterior terminou em `success` (retry automático só ocorre após falha);
2. o intervalo entre o fim da tentativa anterior e a criação da seguinte é maior que `--auto-retry-gap`;
3. o usuário da tentativa difere do usuário da anterior.

É uma heurística. O caso que escapa é um retry manual feito por quem disparou o job, logo após a falha e em menos de `--auto-retry-gap`; ele é contado como automático. Ajuste `--auto-retry-gap` conforme a realidade dos seus runners.

## Limitações e cuidados

- **Desempenho:** uma chamada de jobs por pipeline. Em instâncias grandes a varredura total pode levar horas; use `--group`, `--ref`, reduza o período ou ajuste `--workers`/`--project-workers` (cuidado com o rate limit do servidor).
- **Retenção:** pipelines/jobs removidos por política de expiração não aparecem.
- **Erros de coleta:** nada derruba a execução, exceto token inválido (401), que encerra com mensagem clara. Falhas por projeto ou pipeline vão para `erros.csv` e **não entram** na contagem. Erros transitórios (429/5xx) são repetidos automaticamente. `Ctrl+C` grava os resultados parciais.
- **Nada encontrado:** se não houver nenhum deploy completo, o script lista os nomes de job com `deploy`/`rollback` que viu, para você ajustar `--deploy-jobs` e `--rollback-regex`.
- **Filtro por atividade:** na varredura total, projetos sem atividade desde `--since` são descartados. Um projeto que só roda por agendamento, sem commits no período, seria perdido; use `--all-activity` nesse caso.
- **Rollback sem deploy completo:** reportado à parte. É possível reverter um deploy que entregou em apenas um dos clusters.
- **Última tentativa:** um deploy que teve sucesso e depois foi reexecutado com falha conta como **não completo**.
- **Nomes de rollback:** se o regex for amplo demais, pode capturar jobs que não são rollback; confira em `pipelines.csv`.
