# deploy_metrics

Mede entregas em produção em ambiente **ativo-ativo** a partir da API do GitLab (self-managed), usando `python-gitlab`.

Para cada pipeline do período, o script calcula:

| Métrica | Definição |
|---|---|
| **Deploy completo** | Todos os jobs de `--deploy-jobs` (padrão: `deploy-prd-bc` **e** `deploy-prd-mt`) com a última tentativa em `success`. Se só um cluster entregou, não conta. |
| **Rollback único** | Um ou mais jobs cujo nome casa com `--rollback-regex` (padrão `^rollback`) com `success`. Vários jobs de rollback no mesmo pipeline (um por cluster) contam como **1**. |
| **Entrega de primeira** | Deploy completo sem retry manual nos jobs de deploy. |
| **Entrega com retry manual** | Deploy completo com pelo menos um retry manual, **mesmo que a tentativa anterior tenha dado sucesso**. |
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

python deploy_metrics.py --project 123 --out ./saida
```

Padrões: período `2026-01-01` a `2026-09-30`, jobs `deploy-prd-bc,deploy-prd-mt`.

### Exemplos

```bash
# Vários projetos (ID numérico ou path), somente a branch main
python deploy_metrics.py --project 123 --project grupo/app-b --ref main

# Outro período
python deploy_metrics.py --project 123 --since 2026-04-01 --until 2026-06-30

# Definir deploy completo com os quatro jobs e outro padrão de rollback
python deploy_metrics.py --project 123 \
  --deploy-jobs deploy-prd,deploy-prd-bc,deploy-prd-mt,deploy-prd-dr \
  --rollback-regex '^(rollback|revert)-prd'
```

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
| `--project` | (obrigatório) | ID ou path do projeto; repetível |
| `--since` / `--until` | `2026-01-01` / `2026-09-30` | Período, formato `AAAA-MM-DD`, inclusivo, pela data de **criação do pipeline** |
| `--deploy-jobs` | `deploy-prd-bc,deploy-prd-mt` | Jobs que, juntos, definem um deploy completo |
| `--rollback-regex` | `^rollback` | Regex dos jobs de rollback |
| `--ref` | (todas) | Filtra por branch/tag |
| `--auto-retry-gap` | `60` | Segundos; retry mais rápido que isso é tratado como automático |
| `--workers` | `8` | Threads de coleta (uma chamada de jobs por pipeline) |
| `--insecure` | desligado | Desativa verificação SSL e suprime warnings (`GITLAB_INSECURE=1`) |
| `--out` | `.` | Diretório de saída |

## Saídas

Em `--out`:

- **`pipelines.csv`**: uma linha por pipeline relevante (com deploy ou rollback): projeto, id, URL, ref, mês, `deploy_completo`, `entrega` (`primeira`/`retry_manual`), `retries_manuais`, `retries_apos_sucesso`, `rollback` (`success`/`failed`/`none`). Serve para auditar cada caso pelo link.
- **`resumo_mensal.csv`**: totais por mês e linha `TOTAL`, com os quatro grupos do cruzamento, `rollbacks_unicos`, `rollbacks_sem_deploy_completo`, `rollbacks_falhos`, `pct_retry_manual` e `pct_rollback`.
- Resumo no terminal:

```
Deploys completos ........ N
  entregues de primeira .. N  (com rollback: N)
  com retry manual ....... N  (com rollback: N)
    retries após sucesso . N de N retries manuais
Rollbacks únicos ......... N  (sem deploy completo: N, falhos: N)
```

## Como o retry manual é identificado

A API do GitLab **não informa** se um retry foi manual ou automático. Cada retry vira um novo job com o mesmo nome no pipeline (`include_retried=true` traz todos). O script classifica a tentativa como manual quando ocorre qualquer um dos casos abaixo:

1. a tentativa anterior terminou em `success` (retry automático só ocorre após falha);
2. o intervalo entre o fim da tentativa anterior e a criação da seguinte é maior que `--auto-retry-gap`;
3. o usuário da tentativa difere do usuário da anterior.

É uma heurística. O caso que escapa é um retry manual feito por quem disparou o job, logo após a falha e em menos de `--auto-retry-gap`; ele é contado como automático. Ajuste `--auto-retry-gap` conforme a realidade dos seus runners.

## Limitações e cuidados

- **Desempenho:** uma chamada de jobs por pipeline. Em milhares de pipelines pode demorar; use `--ref`, reduza o período ou ajuste `--workers`.
- **Retenção:** pipelines/jobs removidos por política de expiração não aparecem.
- **Erros de coleta:** pipelines que falharem na API são listados ao final (stderr) e **não entram** na contagem.
- **Rollback sem deploy completo:** reportado à parte. É possível reverter um deploy que entregou em apenas um dos clusters.
- **Última tentativa:** um deploy que teve sucesso e depois foi reexecutado com falha conta como **não completo**.
- **Nomes de rollback:** se o regex for amplo demais, pode capturar jobs que não são rollback; confira em `pipelines.csv`.
