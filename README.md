# MDIF — Metadata Data-driven Ingestion Framework (Bronze Layer)

MDIF is the **Bronze-layer ingestion framework** for the data products built on **Azure Databricks + Unity Catalog **. It ingests data from files, databases, and APIs through three progressively-cleaned layers — **Landing → Raw → Raw Trusted** — for every source table, using a single generic pipeline driven entirely by **metadata**, instead of one script per source.

Adding a new table to ingest means adding a metadata JSON config — not writing new pipeline code.

---

## 1. Concept

Every source table has a metadata definition (JSON) describing:
- where the data comes from (file/DB/API + format),
- where each layer (Landing/Raw/Raw Trusted) should write its output,
- schema, primary key, and which layers/checks are enabled.

These configs are synced into a **Unity Catalog control table**. A Databricks Job reads the relevant rows at runtime and drives the same generic pipeline for each table, so the framework code stays identical across tables — only the metadata changes.

## 2. Pipeline flow (runtime)

```
start_job → read_metadata → main (Landing → Raw → Raw Trusted, per table) → dashboard_updates → notify_job
```

| Step | File | What it does |
|---|---|---|
| Entry point | `start_job.py` | Loads shared environment (`env.py`) and prints run context |
| Metadata read | `read_metadata.py` | Queries the control table for the active table configs belonging to this job and passes them to the next task |
| Orchestration | `main.py` | For each table: runs the enabled layers in order, resolves ADLS output paths, archives the source file, and logs every step to a Delta pipeline-logs table |
| Monitoring | `dashboard_updates.py` | Upserts a per-table status row (success/failure, row counts, timing) into a monitoring dashboard table |
| Notification | `notify_job.py` | Once all tables in the job finish, builds and sends a run-summary notification via Logic App |

The pipeline supports **partial runs** — e.g. re-running only `Raw + Raw Trusted` from the latest Landing CSV, or only `Raw Trusted` from the latest Raw Parquet — simply by which layer configs are populated in the metadata for that table.

## 3. Ingestion layers (`ingestion_steps/`)

1. **Landing** (`landing.py`)
   Reads the source (file, DB, or API) through the connector registry, optionally applies PII column encryption, and writes the result as **CSV**.

2. **Raw** (`raw.py`)
   Cleans the Landing CSV (drops fully-null rows, handles empty-header columns), standardizes column headers, enforces or infers the schema, deduplicates rows, and writes **Parquet**.

3. **Raw Trusted** (`raw_trusted.py`)
   Runs the four mandatory **structural data-quality checks** (schema, row count, primary key, duplicate rows) via **Great Expectations**, evolves the target schema if needed, and writes to **Delta**. Any failed check is logged to a DQ error table and fails the job.

## 4. Connectors (`connectors/`)

A pluggable reader/writer registry (`connector_registry.py`) that resolves the right connector by `file_format` in the metadata:

| Format | Module |
|---|---|
| CSV | `csv_connector.py` |
| Excel | `excel_connector.py` |
| Parquet | `parquet_connector.py` |
| JSON | `json_connector.py` |
| Delta | `delta_connector.py` |
| Database | `database_connector.py` |
| API (e.g. weather) | `api/weather_api.py` |

## 5. Validation (`validation/validations.py`)

Great Expectations–based structural checks executed on every table at the Raw Trusted layer:
- **Schema validation**
- **Row count validation**
- **Primary key validation**
- **Duplicate row validation**

A failure raises a `ValidationError`, is logged in detail via `utils/dq_logging_utils.py`, and is then re-raised to fail the job run.

## 6. Utilities (`utils/`)

| Module | Purpose |
|---|---|
| `logging_utils.py` | Delta-based structured event logging and run ID generation |
| `dq_logging_utils.py` | Logs structural DQ validation failures to a Delta table |
| `notify_utils.py` | Builds and sends the HTML run-summary notification |
| `path_utils.py` | Resolves standardized ADLS paths per layer/table |
| `archive_utils.py` | Moves ingested source files to an archive location |
| `masking_utils.py` | AES-based reversible PII column encryption/decryption via Azure Key Vault |
| `metadata_utils.py` | JSON parsing/serialization, Spark `Row` → `dict` helpers |
| `schema_config.py` | Schema enforcement and inference |
| `spark_utils.py` | Spark session creation/retrieval |

## 7. Transform (`transform/`)

- `header_standardization.py` — normalizes column names (case, spacing, special characters) before the Raw layer writes Parquet.

## 8. CI/CD (`CICD/`)

Two Azure DevOps pipelines cover code and metadata separately:

- **`azure-framework.yml`** — CI/CD for framework **code** changes (lint/test, then deploy).
- **`azure-metadata.yml`** — manually-triggered pipeline for **metadata/job config** changes, taking `JOB_NAMES` and deploying through DEV → PRE-PROD (approval) → PROD (approval).

Both trigger a Databricks deployment job that runs four notebooks in this order:

| Order | File | Purpose |
|---|---|---|
| 1 | `start_deployment_job.py` | Confirms a metadata folder (and table JSONs) exists in DBFS for each job in `JOB_NAMES` before anything else runs |
| 2 | `validate_metadata.py` | Validates each job's metadata JSON files against required structure (top-level fields, per-layer fields, supported formats) |
| 3 | `control_table_init.py` | Ensures the Unity Catalog control table exists and syncs the validated metadata JSONs into it via Delta `MERGE` |
| 4 | `cd_create_jobs.py` | Uses the Databricks SDK (`WorkspaceClient`) to create/update the actual MDIF Databricks Job per source — building the `start_job → read_metadata → main` task DAG, configuring triggers (schedule / file arrival / manual), and setting `run_as` to the project Service Principal |

## 9. Tests (`tests/`)

- `tests_framework.py` — pytest suite checking Python syntax validity and project structure conventions across all framework files.
- `pyproject.toml` / `ruff.toml` — pytest and lint (ruff) configuration.

```bash
cd tests
pytest
ruff check .
```

## 10. Project structure

```
bronze/
├── main.py                     # Pipeline orchestrator (per-table entry point)
├── start_job.py                # Job entry point / environment summary
├── read_metadata.py            # Loads active control-table configs
├── dashboard_updates.py        # Monitoring dashboard upsert
├── notify_job.py                # Run-summary notification (final step)
├── env.py                      # Environment constants, widgets, derived paths
├── requirements.txt
├── connectors/                 # Format-specific readers/writers + registry
│   └── api/weather_api.py
├── ingestion_steps/            # landing.py, raw.py, raw_trusted.py
├── transform/
│   └── header_standardization.py
├── validation/
│   └── validations.py          # Great Expectations structural checks
├── utils/                      # logging, DQ logging, notify, path, archive,
│                                #   masking, metadata, schema, spark helpers
├── CICD/                       # Deployment DAG notebooks + Azure Pipelines YAMLs
└── tests/                      # pytest structural/syntax tests + lint config
```

## 11. Configuration (`env.py`)

- `CATALOG` / `SCHEMA` / `TABLE` — Unity Catalog location of the control table (`cdap_mars_pc_mdif.metadata.metadata_control_table`)
- `ADLS_BASE` — root ADLS Gen2 path for Landing/Raw/Raw Trusted data
- `SCOPE` / `POLICY_ID` — Key Vault–backed Databricks secret scope (Logic App notification URL, PII encryption keys)
- Widgets: `JOB_NAME` (single job) or `JOB_NAMES` (multiple jobs, JSON array or comma-separated)

## 12. Requirements

```
great-expectations==1.3.0
tenacity>=8.2.3
openpyxl>=3.0.10
typing_extensions>=4.6.0
cryptography>=41.0.0
tzlocal
```
Runs on Databricks Runtime (PySpark, Delta Lake, `dbutils` are assumed available).

## 13. Deploying and running

1. Push metadata JSON changes → `azure-metadata.yml` runs `start_deployment_job → validate_metadata → control_table_init → cd_create_jobs`, creating/updating the Databricks Job for each source.
2. Trigger the resulting Databricks Job (schedule, file arrival, or manual) with `JOB_NAME`/`JOB_NAMES` set.
3. The job runs `start_job → read_metadata → main` (once per table) `→ dashboard_updates → notify_job`, logging every table to Delta and sending a final run summary via Logic App.
