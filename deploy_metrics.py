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
  python deploy_metrics.py --project 123 --project grupo/outro-projeto
  # GitLab com certificado autoassinado: acrescente --insecure (ou GITLAB_INSECURE=1)
"""
import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

import gitlab


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
def coletar_jobs(project, pipeline_id):
    pl = project.pipelines.get(pipeline_id, lazy=True)
    return [j.attributes for j in pl.jobs.list(include_retried=True, per_page=100, iterator=True)]


def processar_projeto(gl, ref_projeto, args, deploy_jobs, rollback_re, gap_auto):
    project = gl.projects.get(ref_projeto)
    filtros = dict(
        created_after=f"{args.since}T00:00:00Z",
        created_before=f"{args.until + timedelta(days=1)}T00:00:00Z",
        per_page=100,
        iterator=True,
    )
    if args.ref:
        filtros["ref"] = args.ref
    pipelines = [p.attributes for p in project.pipelines.list(**filtros)]
    print(f"[{project.path_with_namespace}] {len(pipelines)} pipelines no período", file=sys.stderr)

    linhas, erros = [], []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futuros = {ex.submit(coletar_jobs, project, p["id"]): p for p in pipelines}
        for i, fut in enumerate(as_completed(futuros), 1):
            p = futuros[fut]
            try:
                jobs = fut.result()
            except Exception as e:  # segue adiante, reporta no final
                erros.append((project.path_with_namespace, p["id"], str(e)))
                continue
            a = analisar_pipeline(jobs, deploy_jobs, rollback_re, gap_auto)
            if not a["deploy_presente"] and a["rollback"] == "none":
                continue  # pipeline sem relação com produção
            linhas.append({
                "projeto": project.path_with_namespace,
                "pipeline_id": p["id"],
                "url": p["web_url"],
                "ref": p["ref"],
                "criado_em": p["created_at"],
                "mes": p["created_at"][:7],
                **a,
            })
            if i % 100 == 0:
                print(f"  {i}/{len(pipelines)}", file=sys.stderr)
    return linhas, erros


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.getenv("GITLAB_URL"))
    ap.add_argument("--token", default=os.getenv("GITLAB_TOKEN"))
    ap.add_argument("--insecure", action="store_true",
                    default=os.getenv("GITLAB_INSECURE", "").lower() in ("1", "true", "yes"),
                    help="desativa a verificação SSL (certificado autoassinado) e suprime os warnings; "
                         "também via GITLAB_INSECURE=1")
    ap.add_argument("--project", action="append", required=True, help="ID ou path; repetível")
    ap.add_argument("--since", type=date.fromisoformat, default=date(2026, 1, 1))
    ap.add_argument("--until", type=date.fromisoformat, default=date(2026, 9, 30))
    ap.add_argument("--deploy-jobs", default="deploy-prd-bc,deploy-prd-mt",
                    help="jobs que, juntos, definem um deploy completo (ativo-ativo)")
    ap.add_argument("--rollback-regex", default=r"^rollback",
                    help="regex dos jobs de rollback (qualquer cluster conta como 1)")
    ap.add_argument("--ref", help="filtra por branch/tag (ex.: main)")
    ap.add_argument("--auto-retry-gap", type=int, default=60,
                    help="segundos; retry mais rápido que isso é tratado como automático")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=".")
    args = ap.parse_args()
    if not args.url or not args.token:
        ap.error("informe --url/--token ou GITLAB_URL/GITLAB_TOKEN")

    deploy_jobs = [j.strip() for j in args.deploy_jobs.split(",") if j.strip()]
    rollback_re = re.compile(args.rollback_regex)
    gap_auto = timedelta(seconds=args.auto_retry_gap)
    if args.insecure:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    gl = gitlab.Gitlab(args.url, private_token=args.token, ssl_verify=not args.insecure)

    linhas, erros = [], []
    for proj in args.project:
        l, e = processar_projeto(gl, proj, args, deploy_jobs, rollback_re, gap_auto)
        linhas += l
        erros += e
    linhas.sort(key=lambda r: (r["projeto"], r["criado_em"]))

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "pipelines.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(linhas[0].keys()) if linhas else ["vazio"])
        w.writeheader()
        w.writerows(linhas)

    meses = resumir(linhas)
    with open(os.path.join(args.out, "resumo_mensal.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["mes", *COLUNAS_RESUMO, "pct_retry_manual", "pct_rollback"])
        for mes in sorted(k for k in meses if k != "TOTAL") + ["TOTAL"]:
            m = meses[mes]
            rb = m["primeira_com_rollback"] + m["retry_manual_com_rollback"]
            rm = m["retry_manual"] + m["retry_manual_com_rollback"]
            w.writerow([mes, *[m[c] for c in COLUNAS_RESUMO],
                        pct(rm, m["deploys_completos"]), pct(rb, m["deploys_completos"])])

    t = meses["TOTAL"]
    n = t["deploys_completos"]
    print(f"\nPeríodo {args.since} a {args.until} | deploy completo = {' + '.join(deploy_jobs)}")
    print(f"Deploys completos ........ {n}")
    print(f"  entregues de primeira .. {t['primeira'] + t['primeira_com_rollback']}"
          f"  (com rollback: {t['primeira_com_rollback']})")
    print(f"  com retry manual ....... {t['retry_manual'] + t['retry_manual_com_rollback']}"
          f"  (com rollback: {t['retry_manual_com_rollback']})")
    print(f"    retries após sucesso . {t['retries_apos_sucesso']} de {t['retries_manuais']} retries manuais")
    print(f"Rollbacks únicos ......... {t['rollbacks_unicos']}"
          f"  (sem deploy completo: {t['rollbacks_sem_deploy_completo']}, falhos: {t['rollbacks_falhos']})")
    print(f"\nArquivos em {os.path.abspath(args.out)}: pipelines.csv, resumo_mensal.csv")
    if erros:
        print(f"\nATENÇÃO: {len(erros)} pipelines com erro de coleta (não contabilizados):", file=sys.stderr)
        for e in erros[:10]:
            print("  ", e, file=sys.stderr)


if __name__ == "__main__":
    main()
