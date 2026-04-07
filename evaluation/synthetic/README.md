# Synthetic Evaluation Pipeline

Evaluate the SQL Query Engine against a controlled synthetic dataset — 120 gold NL-to-SQL questions (40 per domain) across 3 purpose-built databases seeded with Faker-generated data.

This evaluation tests the engine in a clean, reproducible environment where schemas and data are generated deterministically at runtime. By default, the Docker Compose config runs 75 of the 120 questions (25 per database) to keep runtimes manageable. Set `QUESTIONS_PER_DB=40` in the compose file to run all 120.


## Quick Start

From the project root (`sqlqueryengine/`):

```bash
# Set your Groq API key in docker-compose-synthetic-evaluation.yml
# Then build and run:
docker compose -f docker-compose-synthetic-evaluation.yml up --build
```

Results appear in `evaluation/synthetic/results/`.


## How It Works

The pipeline runs 4 steps automatically:

1. **Seed Databases** — creates 3 PostgreSQL databases (`eval_ecommerce`, `eval_university`, `eval_hospital`) with ~23 tables total, populated with realistic synthetic data via Faker
2. **Generate Questions** — executes 120 gold SQL queries to capture expected results
3. **Run 3-Config Ablation** — sends all questions to the engine under 3 configurations:
   - **Config C** — Generation only (NL to SQL, raw execution)
   - **Config B** — Full pipeline with `retryCount=1` (single-shot)
   - **Config A** — Full pipeline with `retryCount=5` (self-healing enabled)
4. **Generate Score Report** — prints 5 tables and saves `summary.json`


## Databases

| Database | Domain | Tables | Question Count |
|----------|--------|--------|---------------|
| `eval_ecommerce` | E-commerce (customers, orders, products, reviews) | 7 | 40 (25 used by default) |
| `eval_university` | Academic (students, courses, enrollments, faculty) | 8 | 40 (25 used by default) |
| `eval_hospital` | Medical (patients, doctors, appointments, prescriptions) | 8 | 40 (25 used by default) |


## Difficulty Tiers

Questions are labeled across 4 difficulty tiers:

| Tier | Description |
|------|-------------|
| **easy** | Single-table, simple aggregations |
| **medium** | 2-table joins, GROUP BY, HAVING |
| **hard** | 3+ table joins, subqueries, window functions |
| **extra_hard** | Complex nested queries, CTEs, multi-step reasoning |


## Output Files

| File | Description |
|------|-------------|
| `results/summary.json` | All metrics: accuracy, latency percentiles, healing breakdown, resource metrics |
| `results/results_config_c.json` | Per-question results for Config C |
| `results/results_config_b.json` | Per-question results for Config B |
| `results/results_config_a.json` | Per-question results for Config A |
| `results/metrics_config_*.json` | Resource metrics per config (wall time, throughput, memory) |
| `results/questions.json` | All 120 questions with gold results |


## Reported Metrics

### Accuracy Metrics
- **Execution Accuracy (EX)** — order-independent comparison of gold vs predicted result sets
- **Accuracy by difficulty** — breakdown across easy/medium/hard/extra_hard
- **Accuracy by database** — per-domain performance
- **Self-healing breakdown** — correct first attempt, fixed by healing, exhausted retries, regressions

### Resource Metrics
- **Wall-clock time** — total time per configuration
- **Throughput** — questions evaluated per minute
- **Peak memory** — peak RSS of the evaluation runner process
- **Latency percentiles** — p50, p90, p95, p99 per-question latency


## Configuration

All settings are controlled via environment variables in `docker-compose-synthetic-evaluation.yml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_MODEL` | — | Model identifier for the LLM endpoint |
| `LLM_API_KEY` | — | API key for the LLM endpoint |
| `LLM_BASE_URL` | `https://api.groq.com/openai/v1` | OpenAI-compatible endpoint |
| `EVAL_MAX_WORKERS` | `3` | Parallel engine requests |
| `TIMEOUT_SECONDS` | `180` | Per-request timeout |
| `QUESTIONS_PER_DB` | `40` (compose sets `25`) | Questions per database. 40 = all available, 25 = balanced subset |


## Switching Models

To evaluate a different model, change `LLM_MODEL` in both the `eval-engine` and `eval-runner` services, then re-run. Previous results are in `results/runs/{model_name}/`.


## Benchmark Results (75 questions, 25 per database)

| Model | Config C | Config B | Config A | Delta (A−C) | Healed | Regressions |
|---|---|---|---|---|---|---|
| **Llama 4 Scout 17B** | 48.0% | 50.7% | **57.3%** | **+9.3 pp** | 7 | 0 |
| GPT-OSS 20B | 45.3% | 45.3% | 53.3% | +8.0 pp | 6 | 0 |
| Llama 3.3 70B | 53.3% | 54.7% | 54.7% | +1.4 pp | 3 | 2 |
| GPT-OSS 120B | 50.7% | 49.3% | 48.0% | -2.7 pp | 2 | 4 |
| Qwen3 32B | 48.0% | 42.7% | 46.7% | -1.3 pp | 5 | 6 |

### Accuracy by Difficulty (Config A)

| Difficulty | Scout 17B | GPT-OSS 20B | Llama 70B | GPT-OSS 120B | Qwen3 32B |
|---|---|---|---|---|---|
| Easy | 100% | 95.2% | 100% | 100% | 95.2% |
| Medium | 77.8% | 66.7% | 77.8% | 55.6% | 61.1% |
| Hard | 44.4% | 44.4% | 27.8% | 27.8% | 22.2% |
| Extra Hard | 0% | 0% | 5.6% | 0% | 0% |
