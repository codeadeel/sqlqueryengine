#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import os
import subprocess
import sys
import time

import httpx
import psycopg

from birdConfig import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD,
    ENGINE_URL, RESULTS_DIR, RESULTS_BASE_DIR, BIRD_DATA_DIR,
)

# %%
# BIRD evaluation pipeline entrypoint — orchestrates all steps in sequence:
#   1. birdDataLoader.py  — download/load BIRD dataset, migrate SQLite → PostgreSQL,
#                           execute gold SQL, save questions.json
#   2. birdEvalRunner.py  — run the 3-config ablation study via engine REST API
#   3. birdScoreReport.py — generate 5 summary tables and output files


def waitForPostgres() -> bool:
    """Block until PostgreSQL accepts connections (up to 60s)."""
    print("Waiting for PostgreSQL...", flush=True)
    for i in range(60):
        try:
            psycopg.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT,
                user=POSTGRES_USER, password=POSTGRES_PASSWORD,
                dbname="postgres", autocommit=True,
            ).close()
            print(f"  PostgreSQL ready after {i + 1}s", flush=True)
            return True
        except Exception:
            time.sleep(1)
    print("  ERROR: PostgreSQL not ready after 60s", flush=True)
    return False


def waitForEngine() -> bool:
    """Block until the engine /ping endpoint responds (up to 120s)."""
    print(f"Waiting for engine at {ENGINE_URL}...", flush=True)
    for i in range(120):
        try:
            if httpx.get(f"{ENGINE_URL}/ping", timeout=5).status_code == 200:
                print(f"  Engine ready after {i + 1}s", flush=True)
                return True
        except Exception:
            pass
        time.sleep(1)
    print("  ERROR: Engine not ready after 120s", flush=True)
    return False


def runStep(name: str, script: str):
    """Run a Python script as a subprocess, abort on failure."""
    print(f"\n{'=' * 70}", flush=True)
    print(f"Step: {name}", flush=True)
    print(f"{'=' * 70}", flush=True)
    result = subprocess.run([sys.executable, "-u", script], cwd="/app")
    if result.returncode != 0:
        print(f"  FAILED: {name} (exit code {result.returncode})", flush=True)
        sys.exit(result.returncode)
    print(f"  Completed: {name}", flush=True)


# %%
# Execution
if __name__ == "__main__":
    os.makedirs(RESULTS_BASE_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(BIRD_DATA_DIR, exist_ok=True)

    if not waitForPostgres():
        sys.exit(1)

    # Step 1: Load BIRD dataset, migrate SQLite databases → PostgreSQL,
    #         execute gold SQL queries, save questions.json
    runStep("Load BIRD Data + Migrate to PostgreSQL", "birdDataLoader.py")

    if not waitForEngine():
        sys.exit(1)

    # Step 2: Run 3-config ablation (C / B / A)
    runStep("Run BIRD Evaluation (3 configs)", "birdEvalRunner.py")

    # Step 3: Generate score report and output files
    runStep("Generate BIRD Score Report", "birdScoreReport.py")

    print(f"\n{'=' * 70}", flush=True)
    print("BIRD evaluation pipeline complete!", flush=True)
    print(f"Results saved to {RESULTS_DIR}/", flush=True)
    print(f"{'=' * 70}", flush=True)
