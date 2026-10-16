#!/usr/bin/env python3
"""Métricas de entrega em produção (ambiente ativo-ativo) a partir do GitLab.

Para cada pipeline do período calcula:
  - se o deploy foi COMPLETO (todos os jobs de --deploy-jobs com a última
    tentativa em success; ex.: deploy-prd-bc E deploy-prd-mt);
  - se houve retry manual nos jobs de deploy (mesmo que a tentativa anterior
    tenha dado sucesso);
  - se houve rollback (job(s) cujo nome casa com --rollback-regex). Vários
    jobs de rollback no mesmo pipeline (um por cluster) contam como 1 rollback.

Saídas (em --out): pipelines.csv (detalhe) e resumo_mensal.csv, além do
resumo no terminal.

Uso:
  export GITLAB_URL=https://gitlab.exemplo.com GITLAB_TOKEN=glpat-xxx   # read_api
  python deploy_metrics.py                       # varre TODOS os projetos
  python deploy_metrics.py --group meu-grupo     # só um grupo (com subgrupos)
  python deploy_metrics.py --project 123 --project grupo/outro-projeto
  # GitLab com certificado autoassinado: acrescente --insecure (ou GITLAB_INSECURE=1)
"""
import argparse
import csv
import os
import re
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import gitlab
import requests


# --------------------------------------------------------------------------
# Lógica pura (testável sem GitLab)
# --------------------------------------------------------------------------
def parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def retry_manual(anterior, atual, gap_auto):
    """True se a tentativa `atual` parece ter sido disparada manualmente.

    A API não expõe a origem do retry. Heurística:
      1. tentativa anterior com success -> retry automático nunca ocorre
         após sucesso, logo é manual;
      2. intervalo (fim da anterior -> criação da atual) maior que gap_auto;
      3. usuário da tentativa difere do usuário da anterior.
    """
    if anterior["status"] == "success":
        return True
    fim, criado = parse_ts(anterior.get("finished_at")), parse_ts(atual["created_at"])
    if fim and criado and (criado - fim) > gap_auto:
        return True
    u_ant = (anterior.get("user") or {}).get("id")
    u_atu = (atual.get("user") or {}).get("id")
    return u_ant is not None and u_atu is not None and u_ant != u_atu


def analisar_pipeline(jobs, deploy_jobs, rollback_re, gap_auto):
    por_job = defaultdict(list)
    for j in jobs:
        por_job[j["name"]].append(j)
    for tentativas in por_job.values():
        tentativas.sort(key=lambda a: a["id"])

    deploy_completo = all(
        n in por_job and por_job[n][-1]["status"] == "success" for n in deploy_jobs
    )
    deploy_presente = any(n in por_job for n in deploy_jobs)

    retries_manuais = retries_apos_sucesso = 0
    for n in deploy_jobs:
        t = por_job.get(n, [])
        for ant, atu in zip(t, t[1:]):
            if retry_manual(ant, atu, gap_auto):
                retries_manuais += 1
                if ant["status"] == "success":
                    retries_apos_sucesso += 1

    # Rollback: 1 por pipeline, independente de quantos clusters foram revertidos.
    rb_finais = [t[-1]["status"] for n, t in por_job.items() if rollback_re.search(n)]
    if "success" in rb_finais:
        rollback = "success"
    elif "failed" in rb_finais:
        rollback = "failed"
    else:
        rollback = "none"

    return {
        "deploy_presente": deploy_presente,
        "deploy_completo": deploy_completo,
        "retries_manuais": retries_manuais,
        "retries_apos_sucesso": retries_apos_sucesso,
        "entrega": "retry_manual" if retries_manuais else "primeira",
        "rollback": rollback,
    }


def resumir(linhas):
    """Agrega por mês (e total). Cada linha é o dict de um pipeline."""
    meses = defaultdict(lambda: defaultdict(int))
    for r in linhas:
        for chave in (r["mes"], "TOTAL"):
            m = meses[chave]
            rb = r["rollback"] == "success"
            if r["deploy_completo"]:
                m["deploys_completos"] += 1
                m[f"{r['entrega']}{'_com_rollback' if rb else ''}"] += 1
                m["retries_manuais"] += r["retries_manuais"]
                m["retries_apos_sucesso"] += r["retries_apos_sucesso"]
            if rb:
                m["rollbacks_unicos"] += 1
                if not r["deploy_completo"]:
                    m["rollbacks_sem_deploy_completo"] += 1
            if r["rollback"] == "failed":
                m["rollbacks_falhos"] += 1
    return meses


COLUNAS_RESUMO = [
    "deploys_completos",
    "primeira", "primeira_com_rollback",
    "retry_manual", "retry_manual_com_rollback",
    "retries_manuais", "retries_apos_sucesso",
    "rollbacks_unicos", "rollbacks_sem_deploy_completo", "rollbacks_falhos",
]


def pct(a, b):
    return f"{100 * a / b:.1f}" if b else "0.0"



# --------------------------------------------------------------------------
# Coleta no GitLab
# --------------------------------------------------------------------------
CAMPOS_PIPELINE = [
    "projeto", "pipeline_id", "url", "ref", "criado_em", "mes",
    "deploy_presente", "deploy_completo", "entrega",
    "retries_manuais", "retries_apos_sucesso", "rollback",
]
NOMES_RELEVANTES = re.compile(r"deploy|rollback|revert", re.I)


def motivo(e):
    code = getattr(e, "response_code", None)
    msg = str(getattr(e, "error_message", "") or e)[:200]
    return f"HTTP {code}: {msg}" if code else msg


def coletar_jobs(project, pipeline_id):
    pl = project.pipelines.get(pipeline_id, lazy=True)
    return [j.attributes for j in pl.jobs.list(include_retried=True, per_page=100, iterator=True)]


def listar_projetos(gl, args):
    """Retorna [(id_ou_path, path|None)]. Sem --project, varre o GitLab todo."""
    if args.project:
        return [(ref, None) for ref in args.project]
    filtros = dict(iterator=True, per_page=100, simple=True, order_by="id", sort="asc")
    if not args.include_archived:
        filtros["archived"] = False
    if args.membership:
        filtros["membership"] = True
    if args.group:
        origem = gl.groups.get(args.group).projects.list(include_subgroups=True, **filtros)
    else:
        if not args.all_activity:
            filtros["last_activity_after"] = f"{args.since}T00:00:00Z"
        origem = gl.projects.list(**filtros)
    return [(p.id, p.path_with_namespace) for p in origem]


def processar_projeto(gl, ref, path, args, deploy_jobs, rollback_re, gap_auto):
    """Nunca levanta exceção por problema de um projeto (só 401 aborta tudo).

    status: com_producao | sem_pipelines | sem_jobs_producao | sem_acesso | erro
    """
    res = {"path": path or str(ref), "status": "", "linhas": [], "erros": [], "nomes": Counter()}
    try:
        if path is None:
            project = gl.projects.get(ref)
            res["path"] = path = project.path_with_namespace
        else:
            project = gl.projects.get(ref, lazy=True)
        filtros = dict(
            created_after=f"{args.since}T00:00:00Z",
            created_before=f"{args.until + timedelta(days=1)}T00:00:00Z",
            per_page=100,
            iterator=True,
        )
        if args.ref:
            filtros["ref"] = args.ref
        pipelines = [p.attributes for p in project.pipelines.list(**filtros)]
    except gitlab.exceptions.GitlabAuthenticationError:
        raise
    except gitlab.exceptions.GitlabError as e:
        # 403/404: sem permissão, projeto inexistente ou CI/Pipelines desabilitado
        res["status"] = "sem_acesso" if getattr(e, "response_code", None) in (403, 404) else "erro"
        res["erros"].append((res["path"], "", motivo(e)))
        return res
    except requests.exceptions.RequestException as e:
        res["status"] = "erro"
        res["erros"].append((res["path"], "", f"rede: {e}"[:200]))
        return res

    if not pipelines:
        res["status"] = "sem_pipelines"
        return res

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futuros = {ex.submit(coletar_jobs, project, p["id"]): p for p in pipelines}
        for fut in as_completed(futuros):
            p = futuros[fut]
            try:
                jobs = fut.result()
            except gitlab.exceptions.GitlabAuthenticationError:
                raise
            except Exception as e:  # pipeline sem jobs acessíveis, apagado etc.
                res["erros"].append((path, p["id"], motivo(e)))
                continue
            for j in jobs:
                if NOMES_RELEVANTES.search(j["name"]):
                    res["nomes"][j["name"]] += 1
            a = analisar_pipeline(jobs, deploy_jobs, rollback_re, gap_auto)
            if not a["deploy_presente"] and a["rollback"] == "none":
                continue  # pipeline sem relação com produção
            res["linhas"].append({
                "projeto": path,
                "pipeline_id": p["id"],
                "url": p["web_url"],
                "ref": p["ref"],
                "criado_em": p["created_at"],
                "mes": p["created_at"][:7],
                **a,
            })
    res["status"] = "com_producao" if res["linhas"] else "sem_jobs_producao"
    return res


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.getenv("GITLAB_URL"))
    ap.add_argument("--token", default=os.getenv("GITLAB_TOKEN"))
    ap.add_argument("--insecure", action="store_true",
                    default=os.getenv("GITLAB_INSECURE", "").lower() in ("1", "true", "yes"),
                    help="desativa a verificação SSL (certificado autoassinado) e suprime os warnings; "
                         "também via GITLAB_INSECURE=1")
    ap.add_argument("--project", action="append",
                    help="ID ou path; repetível. Sem --project/--group, varre o GitLab inteiro")
    ap.add_argument("--group", help="ID ou path de um grupo (inclui subgrupos)")
    ap.add_argument("--membership", action="store_true",
                    help="varredura total: só projetos dos quais o token é membro")
    ap.add_argument("--include-archived", action="store_true", help="inclui projetos arquivados")
    ap.add_argument("--all-activity", action="store_true",
                    help="não filtra projetos por last_activity_at >= --since (mais lento, mais completo)")
    ap.add_argument("--since", type=date.fromisoformat, default=date(2026, 1, 1))
    ap.add_argument("--until", type=date.fromisoformat, default=date(2026, 9, 30))
    ap.add_argument("--deploy-jobs", default="deploy-prd-bc,deploy-prd-mt",
                    help="jobs que, juntos, definem um deploy completo (ativo-ativo)")
    ap.add_argument("--rollback-regex", default=r"^rollback",
                    help="regex dos jobs de rollback (qualquer cluster conta como 1)")
    ap.add_argument("--ref", help="filtra por branch/tag (ex.: main)")
    ap.add_argument("--auto-retry-gap", type=int, default=60,
                    help="segundos; retry mais rápido que isso é tratado como automático")
    ap.add_argument("--workers", type=int, default=4, help="threads por projeto (jobs de pipelines)")
    ap.add_argument("--project-workers", type=int, default=4, help="projetos processados em paralelo")
    ap.add_argument("--out", default=".")
    args = ap.parse_args()
    if not args.url or not args.token:
        ap.error("informe --url/--token ou GITLAB_URL/GITLAB_TOKEN")
    if args.since > args.until:
        ap.error("--since não pode ser posterior a --until")

    deploy_jobs = [j.strip() for j in args.deploy_jobs.split(",") if j.strip()]
    rollback_re = re.compile(args.rollback_regex)
    gap_auto = timedelta(seconds=args.auto_retry_gap)
    if args.insecure:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    gl = gitlab.Gitlab(args.url, private_token=args.token, ssl_verify=not args.insecure,
                       retry_transient_errors=True, timeout=60)

    try:
        gl.auth()
        projetos = listar_projetos(gl, args)
    except gitlab.exceptions.GitlabAuthenticationError:
        sys.exit("ERRO: token inválido ou sem o escopo read_api (HTTP 401).")
    except gitlab.exceptions.GitlabError as e:
        sys.exit(f"ERRO ao listar projetos: {motivo(e)}")
    except requests.exceptions.RequestException as e:
        sys.exit(f"ERRO de conexão com {args.url}: {e}\n(certificado autoassinado? use --insecure ou REQUESTS_CA_BUNDLE)")
    if not projetos:
        sys.exit("Nenhum projeto encontrado. Verifique token/permissões, --group, --membership e --all-activity.")
    print(f"{len(projetos)} projetos a varrer", file=sys.stderr)

    linhas, erros = [], []
    contagem, nomes = Counter(), Counter()
    feitos = 0
    ex = ThreadPoolExecutor(max_workers=args.project_workers)
    try:
        futuros = [ex.submit(processar_projeto, gl, ref, path, args, deploy_jobs, rollback_re, gap_auto)
                   for ref, path in projetos]
        for fut in as_completed(futuros):
            try:
                r = fut.result()
            except gitlab.exceptions.GitlabAuthenticationError:
                print("ERRO: token sem permissão (401) durante a coleta; encerrando com resultados parciais.",
                      file=sys.stderr)
                break
            except Exception as e:  # defesa final: um projeto nunca derruba a execução
                erros.append(("?", "", f"inesperado: {e}"[:200]))
                contagem["erro"] += 1
                continue
            linhas += r["linhas"]
            erros += r["erros"]
            nomes.update(r["nomes"])
            contagem[r["status"]] += 1
            feitos += 1
            if feitos % 50 == 0 or feitos == len(projetos):
                print(f"  {feitos}/{len(projetos)} projetos | {len(linhas)} pipelines de produção",
                      file=sys.stderr)
    except KeyboardInterrupt:
        print("\nInterrompido: gravando resultados parciais.", file=sys.stderr)
        for f in futuros:
            f.cancel()
    finally:
        ex.shutdown(wait=False, cancel_futures=True)

    linhas.sort(key=lambda r: (r["projeto"], r["criado_em"]))

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "pipelines.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_PIPELINE)
        w.writeheader()
        w.writerows(linhas)
    with open(os.path.join(args.out, "erros.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["projeto", "pipeline_id", "motivo"])
        w.writerows(erros)

    meses = resumir(linhas)
    with open(os.path.join(args.out, "resumo_mensal.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["mes", *COLUNAS_RESUMO, "pct_retry_manual", "pct_rollback"])
        for mes in sorted(k for k in meses if k != "TOTAL") + (["TOTAL"] if meses else []):
            m = meses[mes]
            rb = m["primeira_com_rollback"] + m["retry_manual_com_rollback"]
            rm = m["retry_manual"] + m["retry_manual_com_rollback"]
            w.writerow([mes, *[m[c] for c in COLUNAS_RESUMO],
                        pct(rm, m["deploys_completos"]), pct(rb, m["deploys_completos"])])

    t = meses["TOTAL"]  # defaultdict: zerado se não houve nada
    n = t["deploys_completos"]
    print(f"\nPeríodo {args.since} a {args.until} | deploy completo = {' + '.join(deploy_jobs)}")
    print(f"Projetos: {len(projetos)} listados | {contagem['com_producao']} com deploy/rollback | "
          f"{contagem['sem_pipelines']} sem pipelines no período | "
          f"{contagem['sem_jobs_producao']} sem jobs de produção | "
          f"{contagem['sem_acesso']} sem acesso/CI desabilitado | {contagem['erro']} com erro")
    print(f"Deploys completos ........ {n}")
    print(f"  entregues de primeira .. {t['primeira'] + t['primeira_com_rollback']}"
          f"  (com rollback: {t['primeira_com_rollback']})")
    print(f"  com retry manual ....... {t['retry_manual'] + t['retry_manual_com_rollback']}"
          f"  (com rollback: {t['retry_manual_com_rollback']})")
    print(f"    retries após sucesso . {t['retries_apos_sucesso']} de {t['retries_manuais']} retries manuais")
    print(f"Rollbacks únicos ......... {t['rollbacks_unicos']}"
          f"  (sem deploy completo: {t['rollbacks_sem_deploy_completo']}, falhos: {t['rollbacks_falhos']})")
    print(f"\nArquivos em {os.path.abspath(args.out)}: pipelines.csv, resumo_mensal.csv, erros.csv")

    if n == 0:
        print("\nNenhum deploy completo encontrado. Confira os nomes dos jobs (--deploy-jobs / --rollback-regex).",
              file=sys.stderr)
        if nomes:
            print("Jobs com 'deploy'/'rollback' no nome vistos nos pipelines (mais frequentes):", file=sys.stderr)
            for nome, qtd in nomes.most_common(15):
                print(f"  {qtd:>6}  {nome}", file=sys.stderr)
        else:
            print("Nenhum job com 'deploy'/'rollback' no nome foi visto. Verifique período, --ref e permissões.",
                  file=sys.stderr)
    if erros:
        print(f"\nATENÇÃO: {len(erros)} ocorrências de erro/acesso (detalhes em erros.csv); "
              "pipelines com erro de coleta não foram contabilizados.", file=sys.stderr)


if __name__ == "__main__":
    main()
