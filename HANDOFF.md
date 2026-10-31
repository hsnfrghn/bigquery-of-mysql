# HANDOFF — gcp-data-mesh (Retail Data Mesh on GCP)

Execution plan for the agent that builds this repo with Hasan. Read `CLAUDE.md` first.

## Goal (what must be true at the end)

The résumé (page 2, Selected Projects) currently says:

> **Retail Data Mesh on GCP** · Terraform, BigQuery, Dataflow, dbt, Airflow · hsnfrghn/gcp-data-mesh
> Terraform-provisioned GCP platform where two domains (orders, inventory) each own and publish a
> tested dbt data product in BigQuery; a Python Beam pipeline on Dataflow ingests order events, and
> Airflow schedules the runs and freshness checks.

Done = every clause of that sentence is true and demonstrable in the repo, **or** the résumé wording
is changed in Step 9 to match what was actually done. Hasan can explain every file.

Why it matters for the job (DataChef posting): must-haves are strong Python, strong SQL, Terraform,
GCP (BigQuery/Dataflow); nice-to-haves are dbt, Airflow, data mesh and retail/e-commerce data. The
first 6-month outcome includes "deliver data products and pipelines that meet team SLAs and quality
standards" — so tests, contracts and freshness checks are the point, not decoration.

## Architecture (keep it exactly this small)

```
generator/  --(JSONL order events, CSV inventory snapshots, partitioned by date)-->  data/landing/  (local)  or  gs://<bucket>/landing/ (gcp)
     |
pipelines/orders_ingest  (Apache Beam, batch, one date per run)
     |  parse + validate -> orders, order_lines, dead_letter
     v
data/raw/*.parquet (local)  or  BigQuery dataset orders_raw (gcp)
     |
dbt/  (one dbt project, two domains = two dbt groups)
     |  orders domain    : stg -> fct_orders (incremental) -> orders__daily_sales   [PUBLIC data product, contract]
     |  inventory domain : stg -> inventory__stock_cover (uses orders__daily_sales) [PUBLIC data product, contract]
     v
DuckDB file data/warehouse.duckdb (local)  or  BigQuery datasets orders_products / inventory_products (gcp)

airflow/  DAG retail_data_mesh_daily:  ingest(ds) -> dbt source freshness -> dbt build orders -> dbt build inventory
infra/terraform/  module domain_data_product (datasets + service account + IAM per domain), env dev
.github/workflows/ci.yml  lint, tests, full dbt build on DuckDB, terraform validate
```

**Data mesh, as shown here** (Hasan must be able to say this in an interview):
1. *Domain ownership* — `orders` and `inventory` each have their own dbt group, owner, datasets and
   service account (Terraform module instantiated once per domain).
2. *Data as a product* — each domain publishes exactly one `access: public` model with an enforced
   **contract**, tests and a freshness SLA; everything else is `private`/`protected`.
3. *Self-serve platform* — the Terraform module + Makefile + Airflow DAG are the "platform": a new
   domain is one module block and one dbt folder.
4. *Federated governance* — shared rules enforced everywhere: contracts on public models, naming
   convention `<domain>__<product>`, CI blocks merges that break them.

## Tool versions (defaults; verify at execution time — see CLAUDE.md)

| Tool | Default | Notes |
|---|---|---|
| Python | 3.11 (via `uv python install 3.11`) | system Python is 3.10; Beam + dbt are happiest on 3.11 |
| uv | already installed (0.12.x) | `uv init --package` not needed; plain `uv init` + `uv add` |
| apache-beam[gcp] | latest 2.x supporting 3.11 | DirectRunner locally |
| dbt-core + dbt-duckdb + dbt-bigquery | latest 1.x that all three agree on | install all three in the same env |
| duckdb | whatever dbt-duckdb pulls in | |
| Terraform | latest 1.x binary | not installed yet: install from HashiCorp's apt repo or the zip release |
| google provider | current major (`~> 7.0` if that is current; check registry.terraform.io) | |
| Airflow | Docker image `apache/airflow:2.10.5-python3.11` | 2.x on purpose: far more examples online; Airflow 3 is a stretch, not a goal |
| Docker Compose | installed (v5) | |

Optional, verified plugins from `claude-plugins-official` that may help the executing agent:
`terraform` (HashiCorp's Terraform MCP: provider/registry docs), `astronomer-data-agents` (Airflow
DAG authoring), `data-agent-kit` (Google Cloud data engineering). Only install them if Hasan agrees;
the plan works without them.

## Target repository layout

```
gcp-data-mesh/
├── CLAUDE.md  HANDOFF.md  LEARNING.md  README.md  Makefile  pyproject.toml  .python-version
├── generator/generate.py               # synthetic retail data
├── pipelines/orders_ingest/
│   ├── pipeline.py                     # Beam pipeline (entry point)
│   └── transforms.py                   # pure parse/validate functions (unit-tested)
├── dbt/
│   ├── dbt_project.yml  packages.yml  profiles.example.yml
│   ├── models/orders/      _orders__sources.yml  _orders__models.yml  stg_orders__orders.sql
│   │                        stg_orders__order_lines.sql  fct_orders.sql  orders__daily_sales.sql
│   ├── models/inventory/   _inventory__sources.yml  _inventory__models.yml
│   │                        stg_inventory__snapshots.sql  inventory__stock_cover.sql
│   ├── models/groups.yml
│   └── tests/assert_no_negative_revenue.sql
├── airflow/  Dockerfile  docker-compose.yml  dags/retail_data_mesh_daily.py
├── infra/terraform/
│   ├── modules/domain_data_product/  main.tf variables.tf outputs.tf
│   └── envs/dev/  main.tf variables.tf outputs.tf versions.tf terraform.tfvars.example
├── tests/  test_generator.py  test_transforms.py
└── .github/workflows/ci.yml
```

---

## Progress

| Step | Title | Est. | Status |
|---|---|---|---|
| 0 | GCP access decision + toolchain | 1 h | todo |
| 1 | Scaffold + synthetic data generator | 3 h | todo |
| 2 | Terraform module + dev env | 4 h | todo |
| 3 | Beam ingestion pipeline | 6 h | todo |
| 4 | dbt: orders domain | 4 h | todo |
| 5 | dbt: inventory domain + mesh rules | 3 h | todo |
| 6 | Airflow DAG | 4 h | todo |
| 7 | CI | 2 h | todo |
| 8 | README + (optional) GCP run | 2–4 h | todo |
| 9 | Explain-back + résumé sync | 2 h | todo |

---

## Step 0 — GCP access decision + toolchain (1 h)

**Do:**
1. Ask Hasan: "Do you have a Google Cloud project with billing that you are legitimately allowed to
   use (your own account from a supported country, or an employer/friend's org that explicitly
   grants you a project)?" Record the answer in README → "Run modes". If no → local mode only;
   Terraform is validated, never applied. Do not discuss workarounds.
2. Install Terraform 1.x. Install Python 3.11 with `uv python install 3.11`; write `.python-version`.
3. `docker run --rm hello-world` to confirm Docker works without sudo.

**Primer concepts:** none beyond "why two run modes": production-shaped code, but runnable for free.

**Check questions:** none. **Done when:** `terraform -version`, `uv run python -V` (3.11) and Docker
all work; decision written in README.

---

## Step 1 — Scaffold + synthetic data generator (3 h)

**Do:**
1. `uv init` (no package), `uv add apache-beam[gcp] dbt-core dbt-duckdb dbt-bigquery duckdb faker pyarrow`,
   `uv add --dev pytest ruff`. Add `[tool.ruff]` line-length 100.
2. `Makefile` with the targets listed in CLAUDE.md (stubs are fine for later steps).
3. `generator/generate.py` — CLI: `--start 2026-09-01 --days 60 --seed 42 --out data/landing`.
   Deterministic (seeded `random.Random` + `Faker.seed`). Produces:
   - `products.csv` — 200 SKUs: `sku` (e.g. `SKU-0001`), `name`, `category` (8 categories),
     `unit_price` (EUR, 2 decimals, 2.50–120.00).
   - `stores.csv` — 6 stores: 5 physical (`AMS-01`, `RTM-01`, `UTR-01`, `DHG-01`, `EIN-01`) + `WEB`.
   - `orders/date=YYYY-MM-DD/events.jsonl` — ~300–400 events/day. Event shape:
     ```json
     {"event_id":"uuid","event_type":"order_placed","event_ts":"2026-09-01T10:15:02Z",
      "order_id":"O-000123","customer_id":"C-0042","store_id":"WEB","currency":"EUR",
      "lines":[{"sku":"SKU-0007","qty":2,"unit_price":19.99}]}
     ```
     `event_type` is `order_placed` (≈97%) or `order_cancelled` (≈2%, refers to an earlier order,
     `lines` empty). Inject on purpose: ≈1% **malformed** events (missing `order_id`, `qty <= 0`, or
     unparsable `event_ts`) and ≈0.5% **exact duplicates** (same `event_id` written twice). Keep the
     injection rates as constants at the top of the file — they are teaching material.
   - `inventory/date=YYYY-MM-DD/snapshot.csv` — `snapshot_date, store_id, sku, on_hand_qty` for
     every store × a random 60 SKUs per store (keep the same 60 per store for all days).
4. `tests/test_generator.py`: same seed ⇒ identical output; malformed and duplicate counts are > 0.

**Primer concepts:** event data vs. snapshot data; why we *inject* bad data (to prove the pipeline
handles it); Hive-style `date=` partitioning (one folder per day → a day can be reprocessed alone).

**Demo (Hasan runs):** `make data && ls data/landing/orders | head && head -2 data/landing/orders/date=2026-09-01/events.jsonl`

**Check questions:**
1. Why partition landing files by date instead of one big file?
2. What's the difference between an event (order placed) and a snapshot (stock on hand)? Which one
   can you sum over time?

**If short on time:** drop products/stores CSVs into the generator as hard-coded lists; skip the test.

---

## Step 2 — Terraform module + dev env (4 h)

**Do:**
1. `infra/terraform/modules/domain_data_product/`:
   - variables: `project_id`, `location` (default `EU`), `domain` (string), `consumer_members`
     (list(string), default `[]`, e.g. `["group:analysts@example.com"]`).
   - resources:
     - `google_bigquery_dataset` `${domain}_raw` and `${domain}_products`
       (`delete_contents_on_destroy = true` — it's a demo; say so in a comment).
     - `google_service_account` `${domain}-pipeline` (account_id must be 6–30 chars, lowercase).
     - `google_bigquery_dataset_iam_member`: the SA gets `roles/bigquery.dataEditor` on both datasets;
       each `consumer_members` entry gets `roles/bigquery.dataViewer` on **`_products` only**
       (consumers see the product, never the raw data — this *is* the data-mesh point).
     - labels on everything: `domain = var.domain`, `managed_by = "terraform"`.
   - outputs: dataset ids, SA email.
2. `infra/terraform/envs/dev/`:
   - `versions.tf`: required_version `>= 1.6`, google provider pinned to current major.
   - `main.tf`: provider `google` (project, region `europe-west4` = Netherlands); one
     `google_storage_bucket` `${project_id}-landing` (uniform access, lifecycle delete after 30 days,
     `force_destroy = true`); `module "orders"` and `module "inventory"` from `../../modules/domain_data_product`;
     `google_bigquery_table` **external table** `inventory_raw.snapshots` over
     `gs://<bucket>/landing/inventory/*` (CSV, skip_leading_rows 1, explicit schema).
   - `terraform.tfvars.example` with `project_id = "your-project-id"`; real `terraform.tfvars` is gitignored.
   - Backend: local state (default). Add a commented `backend "gcs"` block with a 3-line note on the
     bootstrap problem (the state bucket can't be created by the config that stores state in it).
3. `make tf-check` = `terraform fmt -check -recursive` + `terraform -chdir=infra/terraform/envs/dev init -backend=false` + `validate`.

**Primer concepts:** declarative vs. imperative ("you describe the end state, Terraform computes the
diff" — like Django migrations computing ALTERs from models); **state** (Terraform's memory of what it
created); **plan → apply**; **modules** (a function: inputs → resources → outputs); IAM on
**datasets** rather than project-wide (least privilege).

**Demo:** `make tf-check`. If GCP mode: `terraform -chdir=infra/terraform/envs/dev plan` then `apply`,
then open BigQuery console and show the four datasets.

**Check questions:**
1. What happens if you delete `terraform.tfstate` and run `apply` again?
2. Why does a consumer get access to `orders_products` but not `orders_raw`?
3. Adding a third domain `customers` — what do you change? (Answer: one `module` block.)

**If short on time:** skip the external table (load inventory CSVs with `bq load` from the Makefile
in GCP mode instead).

---

## Step 3 — Beam ingestion pipeline (6 h)

**Do:**
1. `pipelines/orders_ingest/transforms.py` — **pure functions, no Beam imports**:
   - `parse_event(line: str) -> dict` raising `ValueError` with a reason for anything malformed
     (bad JSON, missing `order_id`/`event_id`, unparsable `event_ts`, `qty <= 0`, negative price).
   - `to_order_row(event) -> dict` (one row: event_id, event_type, event_ts, order_id, customer_id,
     store_id, currency, `ingest_date`, `ingested_at`).
   - `to_line_rows(event) -> list[dict]` (one row per line: event_id, order_id, line_no, sku, qty,
     unit_price, line_amount = qty × unit_price rounded to 2 dp).
2. `pipelines/orders_ingest/pipeline.py`:
   - args: `--date YYYY-MM-DD`, `--mode local|gcp`, `--landing` (path or `gs://`), `--project`,
     `--dataset orders_raw`, plus standard Beam `PipelineOptions` (so `--runner DataflowRunner`,
     `--region europe-west4`, `--temp_location gs://...` just work).
   - Graph: `ReadFromText(landing/orders/date={date}/*.jsonl)` → `ParDo(ParseFn)` with **tagged
     outputs** `valid` / `dead_letter` (dead letter keeps the raw line + reason + date) →
     from `valid`: `Map(to_order_row)` and `FlatMap(to_line_rows)`.
   - Sinks: local → `WriteToParquet` (or JSONL) under `data/raw/{orders,order_lines,dead_letter}/date={date}/`,
     overwriting that date's folder first (idempotent re-runs). gcp → `WriteToBigQuery` into
     `orders_raw.orders`, `orders_raw.order_lines`, `orders_raw.dead_letter` with explicit schemas,
     `write_disposition=WRITE_APPEND`, and *before* writing delete that date's rows… **simpler and
     preferred:** write to the date partition with `WRITE_TRUNCATE` using the partition decorator
     `table$YYYYMMDD` on day-partitioned tables. Explain why this makes re-runs idempotent.
   - Do **not** dedup in Beam — duplicates are deliberately handled in dbt (Step 4) so Hasan practises
     the SQL window-function pattern.
3. `tests/test_transforms.py` (plain pytest on the pure functions) and one `TestPipeline` test with
   `assert_that` checking 2 valid + 1 dead-letter from 3 input lines.
4. `make ingest` loops over all generated dates in local mode.

**Primer concepts:** `PCollection` (an immutable distributed dataset — like a lazy queryset that never
fits in memory); `ParDo`/`DoFn` (a per-element function, can emit 0..n outputs); **tagged outputs**
for dead letters; **runner** (same code on DirectRunner locally or on Dataflow managed workers);
batch vs. streaming (we do batch; windows/watermarks are concept-only — be ready to define them).

**Demo:** `make ingest && uv run python -c "import duckdb; print(duckdb.sql(\"select count(*) from 'data/raw/dead_letter/**/*.parquet'\"))"`.
GCP mode: run one date with `--runner DataflowRunner` and show the job graph in the Dataflow console.

**Check questions:**
1. Why are the parse/validate functions kept free of Beam imports?
2. What does a dead-letter output buy you vs. raising an exception?
3. Re-running ingestion for 2026-09-05 — why doesn't it create duplicates?
4. (Concept) What is a watermark in streaming, in one sentence?

**If short on time:** drop the `TestPipeline` test (keep the pure-function tests); local sink JSONL
instead of Parquet.

---

## Step 4 — dbt: orders domain (4 h)

**Do:**
1. `dbt/dbt_project.yml` (name `retail_mesh`), `profiles.example.yml` with two targets:
   `local` (type duckdb, path `../data/warehouse.duckdb`) and `bigquery` (method oauth, project from
   env var, location EU). `make dbt` uses `--profiles-dir` pointing at a gitignored `profiles.yml`
   copied from the example by `make setup`. `packages.yml` → `dbt_utils`.
2. Sources (`_orders__sources.yml`): for **local**, dbt-duckdb sources with
   `config: external_location: "read_parquet('../data/raw/orders/**/*.parquet', hive_partitioning=true)"`
   (same for order_lines); for **bigquery**, the `orders_raw` dataset. Switch with
   `{{ target.type }}` in the source YAML or two source definitions — pick the simplest that works,
   and explain it. Add `loaded_at_field: ingested_at` and `freshness: warn_after 26h, error_after 50h`.
3. Models (put `{{ config(group='orders') }}` / folder-level config in `dbt_project.yml`):
   - `stg_orders__orders` (view): **dedup** with
     `row_number() over (partition by event_id order by ingested_at desc) = 1`; cast types.
   - `stg_orders__order_lines` (view): dedup the same way on `(event_id, line_no)`.
   - `fct_orders` (**incremental**, `unique_key='order_id'`, `incremental_strategy='merge'` on BigQuery /
     `delete+insert` on DuckDB — use `{{ 'merge' if target.type == 'bigquery' else 'delete+insert' }}`):
     one row per order: placed_at, store_id, customer_id, `status` = `cancelled` if any cancel event
     else `placed`, `order_amount` = sum of line_amount. `is_incremental()` filter on `placed_at`
     with a 3-day lookback (late cancels).
   - `orders__daily_sales` (table, **`access: public`**, **contract enforced**): grain
     `sales_date, store_id, sku` → `units_sold`, `revenue`, `orders_count`; excludes cancelled orders.
4. Tests (`_orders__models.yml`): `unique` + `not_null` on keys, `accepted_values` on status,
   `relationships` lines→orders, `dbt_utils.unique_combination_of_columns` on the daily_sales grain;
   singular test `tests/assert_no_negative_revenue.sql`.
5. Use only cross-database-safe SQL (`{{ dbt.date_trunc(...) }}`, `{{ dbt.safe_cast(...) }}`, `cast(... as date)`)
   so the same models run on DuckDB and BigQuery.

**Primer concepts:** `source()` vs `ref()` (ref builds the DAG — like ForeignKeys between models);
materializations (view / table / incremental); **incremental models** (only process new rows, then
merge — like an upsert in a nightly Django management command); **tests are queries that must return
zero rows**; **model contracts** (the column names + types are an API promise — build fails if they drift).

**Demo:** `make dbt` (runs `dbt build --target local`), then `dbt docs generate && dbt docs serve`
and show the lineage graph.

**Check questions:**
1. Write from memory the dedup query for "latest row per event_id". (Interview staple — make him type it.)
2. What does `is_incremental()` change on the first run vs. later runs? Why the 3-day lookback?
3. Break the contract on purpose (rename `revenue` → `rev`): what fails, and why is that good?

**If short on time:** make `fct_orders` a plain table (keep the incremental version as a stretch);
skip docs serve.

---

## Step 5 — dbt: inventory domain + mesh rules (3 h)

**Do:**
1. `models/groups.yml`: groups `orders` (owner name "Orders team") and `inventory` ("Inventory team").
2. Default access `private` for staging, `protected` for intermediate; only the two `<domain>__*`
   products are `public`.
3. `stg_inventory__snapshots` (source: local CSV via `read_csv(...)` external location; gcp: the
   Terraform external table `inventory_raw.snapshots`).
4. `inventory__stock_cover` (table, public, contract): grain `snapshot_date, store_id, sku` →
   `on_hand_qty`, `avg_daily_units_28d` (window: `avg(units_sold) over (partition by store_id, sku
   order by date rows between 27 preceding and current row)` on a date spine joined to
   `ref('orders__daily_sales')` so missing days count as 0), `days_of_cover` =
   `on_hand_qty / nullif(avg_daily_units_28d, 0)`.
5. **Prove the mesh rule:** temporarily `ref('fct_orders')` (protected/private, other group) from an
   inventory model, run `dbt build`, show the access error, revert. Note it in LEARNING.md.
6. Schema routing: `+schema: orders_products` / `inventory_products` so on BigQuery each domain lands
   in its Terraform-created dataset (add the standard `generate_schema_name` override so the schema
   name is used verbatim).

**Primer concepts:** dbt **groups + access** (private/protected/public) as the code-level form of
domain ownership; why consumers depend only on public products; date spines (why missing days must
become zeros before a moving average).

**Demo:** `make dbt`, then query `inventory__stock_cover` for the 10 lowest `days_of_cover` in DuckDB.

**Check questions:**
1. Map each of the 4 data-mesh principles to a concrete file/folder in this repo.
2. Why is the 28-day average wrong without the date spine?
3. The inventory team wants a new column from orders — what's the process? (Orders adds it to its
   public product + contract; inventory never reads orders' internals.)

**If short on time:** drop the date spine (note the bug in README as a known limitation — it's still
a good interview talking point).

---

## Step 6 — Airflow DAG (4 h)

**Do:**
1. `airflow/Dockerfile`: `FROM apache/airflow:2.10.5-python3.11`; `pip install` the project's runtime
   deps (beam, dbt-core, dbt-duckdb, dbt-bigquery, faker, pyarrow) — use the Airflow constraints file
   only for Airflow itself; if dependency conflicts appear, install dbt into a separate venv inside the
   image (`python -m venv /opt/dbt-venv`) and call that binary from the DAG. Explain why.
2. `airflow/docker-compose.yml`: a single `airflow standalone` service (SQLite + SequentialExecutor
   is fine for a demo — say so), port 8080, mounting `dags/`, the repo (read-only code), and `data/`.
3. `dags/retail_data_mesh_daily.py`:
   - `schedule="@daily"`, `start_date=2026-09-01`, `catchup=False`, `max_active_runs=1`,
     `default_args = {retries: 2, retry_delay: 5 min}`, tags `["data-mesh"]`.
   - Tasks (BashOperator): `ingest_orders` (`python -m pipelines.orders_ingest.pipeline --date {{ ds }} --mode local`)
     → `dbt_source_freshness` → `dbt_build_orders` (`dbt build --select group:orders`) →
     `dbt_build_inventory` (`dbt build --select group:inventory`).
   - A `sla` (or, if the 2.10 SLA feature misbehaves, a final `check_freshness` task) on the last task.
4. Backfill demo: `airflow dags backfill -s 2026-09-01 -e 2026-09-07 retail_data_mesh_daily` inside
   the container.

**Primer concepts:** DAG / task / operator; `{{ ds }}` templating (the *logical* date, not "now");
**idempotency** (re-running a day gives the same result — Step 3's partition overwrite is what makes
this true); retries; catchup vs. backfill; why the DAG calls dbt instead of holding SQL.

**Demo:** `make airflow-up`, open http://localhost:8080, trigger one run, show the graph and logs;
then the backfill command.

**Check questions:**
1. What is `{{ ds }}` for a run triggered at 09:00 on 2026-11-04 for the 2026-11-03 interval?
2. Why must every task be idempotent for retries and backfills to be safe?
3. Why is it fine that Airflow only *orchestrates* (Beam and dbt do the work)?

**If short on time:** drop the SLA; skip the custom image and use `PythonVirtualenvOperator`-free
plain BashOperators that call `uv run` from the mounted repo.

---

## Step 7 — CI (2 h)

`.github/workflows/ci.yml`, on push + PR, three jobs:
1. **python**: setup uv, `uv sync`, `make lint`, `make test`.
2. **dbt-local**: `make data` (small: `--days 7`), `make ingest`, `make dbt` — the whole pipeline
   runs on DuckDB in CI. This is the strongest single proof in the repo; mention it in the README.
3. **terraform**: `hashicorp/setup-terraform`, `make tf-check`.

**Check question:** What would you add to CI before letting a team merge to main in a real client
project? (e.g. `dbt build --select state:modified+` slim CI, `terraform plan` comment on PRs.)

**If short on time:** single job running all three sequentially.

---

## Step 8 — README + optional GCP run (2–4 h)

README sections, in this order:
1. One-paragraph pitch (reuse the résumé sentence).
2. Architecture diagram — Mermaid `flowchart LR` mirroring the ASCII diagram above.
3. **Data mesh in this repo** — the 4-principle mapping table (from Step 5's check answer).
4. **Run modes** — what was executed where, with dates. Be literal.
5. Quickstart (local): `make setup data ingest dbt`, and `make airflow-up`.
6. GCP quickstart (only commands that were actually run, or clearly marked "not executed").
7. Data quality: list of tests, contracts, freshness SLAs, dead-letter handling.
8. Cost & teardown (GCP): BigQuery on-demand, Dataflow per-job; `terraform destroy`.
9. What I'd do next in a real engagement (3 bullets: streaming via Pub/Sub, slim CI, Dataplex/data
   catalog for discovery).

GCP run (only if Step 0 said yes): `terraform apply`, upload landing files with `gcloud storage cp`,
run one date on Dataflow, `dbt build --target bigquery`, screenshot the BigQuery datasets and
Dataflow graph into `docs/`, then `terraform destroy` to stop costs. Tag `v0.1.0`
(project 2 reuses the Terraform module via this tag).

---

## Step 9 — Explain-back + résumé sync (2 h)

1. Hasan walks through the repo top to bottom **out loud** (or in writing): every folder, why it
   exists, one design decision each. You ask "why?" once per folder. Anything he can't explain → fix
   the code or the understanding now; log the gap in LEARNING.md.
2. Compare the résumé sentence (top of this file) with reality. If GCP mode was never executed,
   propose exactly this wording change for `Txt/2-page2.tex` in the résumé repo
   (`~/Projects/LateX/Resume-2page-job-template`, follow that repo's CLAUDE.md — `% Hsn added:`
   comment, no build unless asked):
   - "Terraform-provisioned GCP platform" → "Terraform-defined GCP platform (validated in CI)"
   - "a Python Beam pipeline on Dataflow ingests" → "a Python Beam pipeline (Dataflow-ready) ingests"
3. Make sure the repo is public, CI is green, `v0.1.0` is tagged, and the URL on the résumé resolves.
4. Update `LEARNING.md` → "Gaps for interview prep" with the lowest-confidence topics. That list is
   the input to the interview-prep session after both projects.
