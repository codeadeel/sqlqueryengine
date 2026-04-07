# BIRD Benchmark Evaluation Pipeline

Evaluate our SQL Query Engine against the [BIRD benchmark](https://bird-bench.github.io/) — a collection of 500+ real-world NL-to-SQL questions across 11 databases (mini-dev) or 1,534 questions across 95 databases (full dev set).

The pipeline converts BIRD's SQLite databases to PostgreSQL at startup, runs our engine against all questions in a 3-config ablation (generation-only, single-shot, full self-healing), and produces Execution Accuracy (EX) metrics comparable to published baselines.


## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Download the BIRD Dataset](#download-the-bird-dataset)
3. [Configure the LLM API Key](#configure-the-llm-api-key)
4. [Run the Evaluation](#run-the-evaluation)
5. [Output Files](#output-files)
6. [Environment Variables Reference](#environment-variables-reference)
7. [Architecture Overview](#architecture-overview)
8. [SQLite to PostgreSQL Conversion](#sqlite-to-postgresql-conversion)
9. [Troubleshooting](#troubleshooting)


## Prerequisites

- Docker and Docker Compose v2+
- A Groq API key (or any OpenAI-compatible LLM endpoint)
- ~3 GB disk space for the BIRD dataset + Docker images


## Download the BIRD Dataset

The BIRD dataset consists of two parts:

1. **Questions JSON** — 500 NL-to-SQL question/answer pairs with difficulty labels
2. **SQLite databases** — 11 real-world databases (mini-dev) as `.sqlite` files

### Option A: Download from the Official Source (Recommended)

```bash
cd evaluation/bird/

# Create the data directory
mkdir -p bird_data && cd bird_data

# Download the questions JSON from HuggingFace
# Dataset page: https://huggingface.co/datasets/birdsql/bird_mini_dev
pip install datasets
python3 -c "
from datasets import load_dataset
import json

ds = load_dataset('birdsql/bird_mini_dev', split='mini_dev_sqlite')
rows = [{'question_id': r['question_id'], 'db_id': r['db_id'],
         'question': r['question'], 'evidence': r['evidence'],
         'SQL': r['SQL'], 'difficulty': r['difficulty']} for r in ds]

with open('mini_dev_sqlite.json', 'w') as f:
    json.dump(rows, f, indent=2)
print(f'Saved {len(rows)} questions')
"

# Download the SQLite databases (764 MB zip)
# Source: https://github.com/bird-bench/mini_dev → llm/mini_dev_data/README.md
wget -O minidev.zip "https://bird-bench.oss-cn-beijing.aliyuncs.com/minidev.zip"
unzip -q minidev.zip

# Move databases to expected location
mv minidev/MINIDEV/dev_databases .

# Clean up
rm -rf minidev minidev.zip

cd ..
```

### Option B: Google Drive Mirror

If the Alibaba mirror is slow, download from Google Drive:

```
https://drive.google.com/file/d/1UJyA6I6pTmmhYpwdn8iT9QKrcJqSQAcX/view?usp=sharing
```

Unzip and arrange files as described in Option A.

### Verify Your Data Directory

After downloading, your directory structure must look like this:

```
evaluation/bird/
  bird_data/
    mini_dev_sqlite.json              # 500 questions
    dev_databases/
      california_schools/
        california_schools.sqlite
      card_games/
        card_games.sqlite
      codebase_community/
        codebase_community.sqlite
      debit_card_specializing/
        debit_card_specializing.sqlite
      european_football_2/
        european_football_2.sqlite
      financial/
        financial.sqlite
      formula_1/
        formula_1.sqlite
      student_club/
        student_club.sqlite
      superhero/
        superhero.sqlite
      thrombosis_prediction/
        thrombosis_prediction.sqlite
      toxicology/
        toxicology.sqlite
```

The pipeline will fail if `mini_dev_sqlite.json` or any referenced `dev_databases/{db_id}/{db_id}.sqlite` file is missing.


## Configure the LLM API Key

Edit `docker-compose-bird-evaluation.yml` in the project root and set your Groq API key in the `bird-engine` service:

```yaml
bird-engine:
  environment:
    - LLM_API_KEY=gsk_your_actual_groq_key_here
    - LLM_MODEL=meta-llama/llama-4-scout-17b-16e-instruct
    - LLM_BASE_URL=https://api.groq.com/openai/v1
```

Also update the `LLM_MODEL` in the `bird-runner` service to match (used for report metadata only):

```yaml
bird-runner:
  environment:
    - LLM_MODEL=meta-llama/llama-4-scout-17b-16e-instruct
```

To use a different LLM provider, change `LLM_BASE_URL` and `LLM_MODEL` to any OpenAI-compatible endpoint.


## Run the Evaluation

From the project root (`sqlqueryengine/`):

```bash
# Build and run all 4 services
docker compose -f docker-compose-bird-evaluation.yml up --build

# Or run in background
docker compose -f docker-compose-bird-evaluation.yml up --build -d

# Follow logs
docker compose -f docker-compose-bird-evaluation.yml logs -f bird-runner
```

The pipeline runs these steps automatically:

1. **Wait for PostgreSQL** to accept connections
2. **Load BIRD dataset** — parse `mini_dev_sqlite.json`, convert gold SQL to PostgreSQL dialect
3. **Migrate SQLite to PostgreSQL** — create 11 `bird_*` databases, copy all tables and data
4. **Execute gold SQL** — run converted gold queries against PostgreSQL to capture expected results
5. **Wait for engine** to be ready (`/ping` endpoint)
6. **Run 3-config ablation** (Redis cache flushed between each):
   - **Config C** — Generation only: NL to SQL via `/inference/sqlQueryGeneration`, execute raw SQL
   - **Config B** — Single-shot: Full pipeline via `/inference/sqlQueryEngine` with `retryCount=1`
   - **Config A** — Full pipeline: Full pipeline with `retryCount=5` (self-healing enabled)
7. **Generate score report** — 5 formatted tables, summary JSON, official BIRD predictions file

Total runtime: ~2-4 hours for 500 questions x 3 configs = 1,500 engine calls (depends on LLM endpoint speed and rate limits).

### Tear Down

```bash
docker compose -f docker-compose-bird-evaluation.yml down -v
```


## Output Files

All results are written to `evaluation/bird/bird_results/` (volume-mounted from the container), organized by model under `bird_results/runs/{model_name}/`:

| File | Description |
|------|-------------|
| `summary.json` | All metrics: accuracy, latency percentiles, healing breakdown, resource metrics |
| `results_config_c.json` | Per-question results for Config C (generation only) |
| `results_config_b.json` | Per-question results for Config B (single-shot) |
| `results_config_a.json` | Per-question results for Config A (full self-healing pipeline) |
| `metrics_config_c.json` | Resource metrics for Config C (wall time, throughput, memory) |
| `metrics_config_b.json` | Resource metrics for Config B |
| `metrics_config_a.json` | Resource metrics for Config A |
| `predictions.txt` | Official BIRD submission format: `{SQL}\t----- bird -----\t{db_id}` per line |
| `conversion_report.json` | SQLite-to-PostgreSQL gold SQL conversion statistics |

The `questions.json` file (shared across models) is at `bird_results/questions.json`.

### Printed Tables (stdout)

The score report prints 6 tables:

1. **Overall EX Accuracy** — per config with evaluated/excluded counts
2. **Accuracy by Difficulty** — simple / moderate / challenging breakdown
3. **Accuracy by Database** — top-10 databases by question count
4. **Self-Healing Breakdown** — correct first attempt, fixed by healing, exhausted, regressions
5. **Comparison vs Published Baselines** — ChatGPT, GPT-4, DIN-SQL, CHASE-SQL, etc.
6. **Resource Metrics** — wall time, throughput (q/min), peak memory, latency p50/p90/p95


## Environment Variables Reference

All configuration is driven by environment variables set in `docker-compose-bird-evaluation.yml`.

### Engine Service (`bird-engine`)

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_BASE_URL` | `https://api.groq.com/openai/v1` | OpenAI-compatible LLM endpoint |
| `LLM_MODEL` | — | Model identifier (e.g., `meta-llama/llama-4-scout-17b-16e-instruct`) |
| `LLM_API_KEY` | — | API key for the LLM endpoint |
| `LLM_TEMPERATURE` | `0.1` | Generation temperature |
| `POSTGRES_HOST` | `birdpostgres` | PostgreSQL hostname |
| `POSTGRES_DB` | `bird_concert_singer` | Default DB (overridden per request) |

### Runner Service (`bird-runner`)

| Variable | Default | Description |
|----------|---------|-------------|
| `BIRD_DATA_DIR` | `/app/bird_data` | Mount point for BIRD dataset files |
| `RESULTS_DIR` | `/app/bird_results` | Mount point for output files |
| `BIRD_DATASET` | `mini` | Dataset variant: `mini` (500 questions) or `full` (1,534) |
| `BIRD_USE_EVIDENCE` | `true` | Prepend BIRD evidence hints to engine prompts |
| `EVAL_MAX_WORKERS` | `4` | Parallel engine requests (tune for rate limits) |
| `TIMEOUT_SECONDS` | `180` | Per-request timeout in seconds |
| `LLM_MODEL` | — | Used in `summary.json` metadata only |
| `LLM_TEMPERATURE` | `0.1` | Used in `summary.json` metadata only |

### Tuning Parallelism

The `EVAL_MAX_WORKERS` setting controls how many questions are sent to the engine concurrently. Set this based on your LLM provider's rate limits:

- **Groq free tier** (~30 req/min): use `3-4`
- **Groq paid tier** (~100+ req/min): use `6-8`
- **Self-hosted / no rate limits**: use `8-12`

Too many workers will trigger 429 rate-limit errors (handled via automatic exponential backoff, but slows total throughput).


## Architecture Overview

```
docker-compose-bird-evaluation.yml
  |
  +-- bird-postgres    PostgreSQL 16 (empty, databases created at runtime)
  +-- bird-redis       Redis (schema caching for the engine)
  +-- bird-engine      SQL Query Engine (FastAPI, built from Dockerfile)
  +-- bird-runner      BIRD evaluation pipeline (Dockerfile stage: birdevaluationrunner)
        |
        +-- birdEntrypoint.py     Orchestrator (subprocess steps)
        +-- birdDataLoader.py     Load questions + migrate SQLite -> PG
        +-- sqliteToPostgres.py   DDL conversion + bulk data insert
        +-- birdEvalRunner.py     3-config ablation via engine REST API
        +-- birdScoreReport.py    Metrics + tables + output files
```

The engine is treated as a **black box** — the runner communicates with it exclusively via REST API calls to `/inference/sqlQueryGeneration/{chatID}` and `/inference/sqlQueryEngine/{chatID}`.


## SQLite to PostgreSQL Conversion

BIRD databases are in SQLite format. Our engine runs against PostgreSQL. The pipeline handles this with two layers of conversion:

### Database Migration (`sqliteToPostgres.py`)

For each of the 11 BIRD databases:

1. Introspect SQLite schema via `PRAGMA table_info` and `PRAGMA foreign_key_list`
2. Map SQLite types to PostgreSQL types (INT -> BIGINT, REAL -> DOUBLE PRECISION, TEXT -> TEXT)
3. Lowercase all identifier names (PostgreSQL folds unquoted identifiers to lowercase)
4. Topologically sort tables by foreign key dependencies
5. Create PostgreSQL database `bird_{db_id}` and all tables
6. Bulk-insert all data in batches of 1,000 rows
7. Reset auto-increment sequences after bulk insert

### Gold SQL Conversion (`birdDataLoader._convertGoldSQL`)

Gold SQL queries are converted from SQLite dialect to PostgreSQL dialect:

| SQLite | PostgreSQL |
|--------|-----------|
| `` `identifier` `` | `"identifier"` |
| `IIF(cond, t, f)` | `CASE WHEN cond THEN t ELSE f END` |
| `STRFTIME('%Y', col)` | `EXTRACT(YEAR FROM col::TIMESTAMP)::TEXT` |
| `GROUP_CONCAT(x)` | `STRING_AGG(x, ',')` |
| `SUBSTR(...)` | `SUBSTRING(...)` |
| `IFNULL(...)` | `COALESCE(...)` |
| `LIKE` | `ILIKE` (case-insensitive) |
| `CAST(x AS FLOAT)` | `CAST(x AS DOUBLE PRECISION)` |
| `CAST(x AS REAL)` | `CAST(x AS NUMERIC)` |
| `LIMIT offset, count` | `LIMIT count OFFSET offset` |
| `INSTR(a, b)` | `POSITION(b IN a)` |
| `DATE('now')` | `CURRENT_DATE` |

### Conversion Error Rate

Some gold SQL queries cannot be auto-converted due to inherent SQLite/PostgreSQL semantic differences:

- Date arithmetic on TEXT-typed columns (`text - text` operator)
- Multi-word unquoted column names (e.g., `T2.First Date`)
- PostgreSQL's stricter GROUP BY requirements
- `SUM(boolean)` not supported in PostgreSQL

Expected error rate for mini-dev: **~12-13%** (63/500 questions). These questions are excluded from accuracy scoring and the exclusion count is reported in all output files.


## Troubleshooting

### "SQLite file not found" Error

Ensure the database files are in the correct location:
```
evaluation/bird/bird_data/dev_databases/{db_id}/{db_id}.sqlite
```

The `{db_id}` folder name must match the `.sqlite` filename exactly.

### Rate Limit Errors (429)

The pipeline automatically retries with exponential backoff (5s, 10s, 15s, 20s, 25s). If you see many 429s, reduce `EVAL_MAX_WORKERS` in the compose file.

### Engine Timeout

If the engine doesn't respond within 120 seconds at startup, the runner exits. Check that `bird-engine` started correctly:

```bash
docker compose -f docker-compose-bird-evaluation.yml logs bird-engine
```

### Large Database Migration is Slow

The `european_football_2` database has ~570 MB of data. Migration takes 1-2 minutes. This is normal. Progress is logged per-table.

### Re-running After a Failure

The pipeline is idempotent. Simply re-run `docker compose up` — databases are dropped and re-created, results are overwritten.

To preserve partial results and only re-run the evaluation (skip migration):

```bash
# Run just the eval runner manually inside the container
docker exec bird-runner python -u birdEvalRunner.py
docker exec bird-runner python -u birdScoreReport.py
```

### Running Without Evidence

To benchmark without BIRD evidence hints (zero-shot, no external knowledge):

```yaml
bird-runner:
  environment:
    - BIRD_USE_EVIDENCE=false
```

This is the fairer comparison against published zero-shot baselines. Run both with and without evidence for the paper.


## Benchmark Results (mini-dev, 437 evaluated, evidence=true)

| Model | Config C | Config B | Config A | Delta (A−C) | Healed | Regressions |
|---|---|---|---|---|---|---|
| **GPT-OSS 120B** | 44.4% | 42.6% | **49.0%** | **+4.6 pp** | 39 | 19 |
| Llama 3.3 70B | 43.7% | 38.0% | 46.5% | +2.8 pp | 35 | 23 |
| Llama 4 Scout 17B | 37.1% | 34.3% | 40.5% | +3.4 pp | 35 | 20 |
| GPT-OSS 20B | 43.5% | 39.8% | 43.2% | -0.3 pp | 26 | 24 |
| Qwen3 32B | 40.7% | 41.6% | 39.4% | -1.3 pp | 31 | 17 |

63 questions (12.6%) excluded due to SQLite-to-PostgreSQL gold SQL conversion errors.

### Comparison vs Published Baselines

| System | EX Accuracy |
|---|---|
| ChatGPT (zero-shot) | 40.1% |
| GPT-4 (zero-shot) | 46.4% |
| DIN-SQL + GPT-4 | 55.9% |
| DAIL-SQL + GPT-4 | 54.8% |
| CHASE-SQL (SOTA) | 73.0% |
| **Ours — GPT-OSS 120B (Config A)** | **49.0%** |
| **Ours — Llama 3.3 70B (Config A)** | **46.5%** |
