# %%
# [ Stage 1 | SQL Query Engine @ PostgreSQL ]: NL-to-SQL inference service
FROM ubuntu@sha256:c35e29c9450151419d9448b0fd75374fec4fff364a27f176fb458d472dfc9e54 AS queryenginepostgresql
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /home/ubuntu

# %%
# System dependencies
RUN apt-get update && apt-get install --no-install-recommends -y python3-minimal python3-pip libpq-dev --fix-missing

# %%
# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt --break-system-packages --ignore-installed wheel

# %%
# Copy application source code
COPY run.py .
COPY sqlQueryEngine/ ./sqlQueryEngine/

# %%
# Start the server
RUN chmod +x ./run.py
ENTRYPOINT ["python3", "./run.py"]


# %%
# [ Stage 2 | Evaluation Runner ]: Ablation study pipeline
FROM python:3.12-slim AS evaluationrunner
WORKDIR /app

# %%
# Install evaluation dependencies
COPY evaluation/synthetic/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# %%
# Copy evaluation scripts, shared utilities, and question bank
COPY evaluation/shared/resultComparator.py evaluation/shared/resourceMetrics.py ./
COPY evaluation/synthetic/evalConfig.py evaluation/synthetic/schemaDefinitions.py ./
COPY evaluation/synthetic/seedData.py evaluation/synthetic/questionRunner.py evaluation/synthetic/evalRunner.py ./
COPY evaluation/synthetic/scoreReport.py evaluation/synthetic/entrypoint.py ./
COPY evaluation/synthetic/questions/ ./questions/

# %%
# Run the full evaluation pipeline
CMD ["python", "-u", "entrypoint.py"]


# %%
# [ Stage 3 | BIRD Evaluation Runner ]: SQLite → PostgreSQL migration + 3-config ablation
FROM python:3.12-slim AS birdevaluationrunner
WORKDIR /app

# %%
# System dependencies — libpq-dev for psycopg (PostgreSQL client library)
# sqlite3 is bundled with Python's stdlib; no extra system package needed
RUN apt-get update && apt-get install --no-install-recommends -y libpq-dev && rm -rf /var/lib/apt/lists/*

# %%
# Install BIRD evaluation dependencies
COPY evaluation/bird/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# %%
# Copy shared evaluation utilities
COPY evaluation/shared/resultComparator.py evaluation/shared/resourceMetrics.py ./

# %%
# Copy BIRD evaluation scripts into the flat /app working directory
COPY evaluation/bird/birdConfig.py evaluation/bird/birdDataLoader.py ./
COPY evaluation/bird/sqliteToPostgres.py evaluation/bird/birdEvalRunner.py ./
COPY evaluation/bird/birdScoreReport.py evaluation/bird/birdEntrypoint.py ./

# %%
# Data and results directories are volume-mounted at runtime:
#   ./evaluation/bird/bird_data    → /app/bird_data    (BIRD SQLite files)
#   ./evaluation/bird/bird_results → /app/bird_results  (output JSON + report)
CMD ["python", "-u", "birdEntrypoint.py"]
