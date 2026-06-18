# sales-analytics-iac

Infrastructure-as-code for a sales analytics backend: raw S3 → Glue ETL → curated
S3 → Glue Data Catalog → Athena. The **ingestion-through-Athena layer is defined
once in an AWS SAM template and deployed as a unit**; QuickSight and the embed sit
deliberately outside this boundary (BI layer is configured separately).

## Architecture

```
        ┌──────────────────────── SAM template (sam deploy) ────────────────────────┐
  CSV → │ Raw S3 ──▶ Glue ETL ──▶ Curated S3 (Parquet) ──▶ Glue Catalog ──▶ Athena   │ ──▶ QuickSight ──▶ embed
        │                                  │                  (tables/views, workgroup)│      (out of scope)
        │                                  └──▶ Data-quality Lambda (gate)            │
        └───────────────────────────────────────────────────────────────────────────┘
```

The only `AWS::Serverless::*` resource is the data-quality Lambda. Glue, S3, the
Glue Catalog table, the Athena workgroup and named queries are declared as native
CloudFormation in the same template — SAM specializes in serverless but compiles to
CloudFormation, so the resources mix freely.

## Repo layout

```
.
├── template.yaml              # SAM template — the whole ingestion→Athena backend
├── samconfig.toml             # per-environment deploy config (dev/staging/prod)
├── src/
│   └── data_quality/          # SAM-native Lambda: generic check runner
│       ├── app.py             #   discovers + runs every checks/*.sql, fails on any false
│       ├── requirements.txt
│       └── checks/            #   one .sql per check; each returns: passed (bool), detail
├── glue/
│   └── ingest.py              # Glue ETL script (uploaded to S3, referenced by the job)
├── sql/
│   └── views/                 # Athena view DDL (CREATE OR REPLACE VIEW ...)
└── .github/
    └── workflows/
        └── ci.yml             # validate → lint → build → deploy
```

## Prerequisites

- AWS SAM CLI and AWS CLI configured
- An S3 location for the Glue script (the job's `ScriptLocation` points at the
  curated bucket; CI syncs `glue/ingest.py` there before deploy)
- Python 3.12 for local Lambda packaging

## Deploy

```bash
sam validate --lint           # static check of the template
sam build                     # package the data-quality Lambda
sam deploy --config-env dev    # deploy the dev stack (params in samconfig.toml)
```

`sam deploy` stands the whole analytics backend up reproducibly, and the same
template runs in CI to redeploy on every change to `main`.

## CI/CD

`.github/workflows/ci.yml` runs on every PR and push to `main`:

1. **validate** — `sam validate --lint` + `cfn-lint` on the template
2. **build** — `sam build`
3. **deploy** — on `main` only, assumes an AWS role via GitHub OIDC (no long-lived
   keys), syncs the Glue script to S3, then `sam deploy --no-confirm-changeset`

## Scope note

QuickSight has CloudFormation resources (`AWS::QuickSight::DataSet`, `::Analysis`,
`::Dashboard`), so the automation *could* extend past Athena. It stops at Athena by
choice: the QuickSight subscription, users, and the runtime embed all live outside
CloudFormation, and dashboard definitions in YAML get verbose and brittle. The
boundary marks where "reproducible from one deploy" cleanly ends.
