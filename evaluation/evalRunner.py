#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
import psycopg
import redis

from evalConfig import (
    ENGINE_URL, ENGINE_PG_HOST, ENGINE_PG_PORT,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD,
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD,
    RESULTS_DIR, QUESTIONS_PATH, TIMEOUT_SECONDS,
)
from resultComparator import resultsMatch

logger = logging.getLogger(__name__)

# Maximum number of parallel requests to the engine.
# Tune based on LLM endpoint throughput and available resources.
MAX_WORKERS = int(os.environ.get("EVAL_MAX_WORKERS", "6"))


# %%
# API call helpers — thin wrappers around the engine REST endpoints
def _apiParams(database: str) -> dict:
    """Build query parameters to override the engine's target database."""
    return {
        "postgreHost": ENGINE_PG_HOST,
        "postgrePort": ENGINE_PG_PORT,
        "postgreUser": POSTGRES_USER,
        "postgrePassword": POSTGRES_PASSWORD,
        "postgreDBName": database,
    }


def _postWithRetry(url: str, params: dict, body: dict, maxRetries: int = 5) -> dict:
    """POST with automatic retry on 429 rate-limit errors."""
    for attempt in range(maxRetries):
        resp = httpx.post(url, params=params, json=body, timeout=TIMEOUT_SECONDS)
        data = resp.json()
        # Check if the engine forwarded a 429 from the LLM provider
        errMsg = str(data.get("error", ""))
        if resp.status_code == 429 or "429" in errMsg or "Rate limit" in errMsg:
            wait = 5 * (attempt + 1)
            logger.warning("Rate limited (attempt %d/%d), waiting %ds...", attempt + 1, maxRetries, wait)
            time.sleep(wait)
            continue
        return data
    return data  # return last response even if still rate-limited


def callGenerate(chatID: str, database: str, question: str) -> dict:
    """
    Call the generation-only endpoint (Stage 1).

    Returns the engine JSON response.
    """
    return _postWithRetry(
        f"{ENGINE_URL}/inference/sqlQueryGeneration/{chatID}",
        _apiParams(database),
        {"basePrompt": question},
    )


def callInference(chatID: str, database: str, question: str, retryCount: int) -> dict:
    """
    Call the full inference endpoint (Stage 1 + Stage 2 with healing).

    Returns the engine JSON response.
    """
    return _postWithRetry(
        f"{ENGINE_URL}/inference/sqlQueryEngine/{chatID}",
        _apiParams(database),
        {"basePrompt": question, "retryCount": retryCount},
    )


def executeRaw(database: str, sql: str):
    """
    Execute SQL directly against PostgreSQL (for Config C).

    Returns:
    --------
    tuple[list | None, str | None]
        (rows, error) — one will be None.
    """
    try:
        conn = psycopg.connect(
            dbname=database, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST, port=POSTGRES_PORT,
        )
        conn.autocommit = True
        rows = conn.cursor().execute(sql).fetchall()
        conn.close()
        return rows, None
    except Exception as e:
        return None, str(e)


# %%
# Redis flush — clears cached schemas between configurations
def flushRedis():
    """Flush the Redis database used by the engine for schema caching."""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=0)
    r.flushdb()
    logger.info("Redis cache flushed")


# %%
# Engine readiness check
def waitForEngine() -> bool:
    """
    Poll the engine ``/ping`` endpoint until it responds or timeout.

    Returns:
    --------
    bool
        True if the engine is reachable within 90 seconds.
    """
    logger.info("Waiting for engine at %s", ENGINE_URL)
    for i in range(90):
        try:
            if httpx.get(f"{ENGINE_URL}/ping", timeout=5).status_code == 200:
                logger.info("Engine ready after %ds", i + 1)
                return True
        except Exception:
            pass
        time.sleep(1)
    logger.error("Engine not ready after 90s")
    return False


# %%
# Configuration labels for structured log output
CONFIG_LABELS = {
    "C": "Generation Only",
    "B": "Single-shot (retryCount=1)",
    "A": "Full Pipeline (retryCount=5)",
}


# %%
# Execute a single question against the engine
def _runQuestion(configName: str, configFn: str, q: dict) -> dict:
    """
    Run a single question for one evaluation configuration.

    Arguments:
    ----------
    configName : str
        Short label (``C``, ``B``, or ``A``).
    configFn : str
        Internal identifier for the config mode.
    q : dict
        A single question record from ``questions.json``.

    Returns:
    --------
    dict
        Result record for this question.
    """
    db = q["database"]
    chatID = f"eval_{configName.lower()}_{db}"
    start = time.time()

    predictedSQL = None
    predRows = None
    err = None

    try:
        if configFn == "generate_only":
            resp = callGenerate(chatID, db, q["question"])
            if resp.get("code") == 200:
                predictedSQL = resp["agentResponse"]["generation"]["sqlQuery"]
                predRows, err = executeRaw(db, predictedSQL)
            else:
                err = resp.get("error") or resp.get("status", "generation failed")

        elif configFn == "run_retry1":
            resp = callInference(chatID, db, q["question"], retryCount=1)
            if resp.get("code") == 200:
                ev = resp["agentResponse"]["evaluation"]
                predictedSQL = ev["currentQuery"]
                predRows = ev["results"]
                if predictedSQL is None:
                    err = "exhausted retries"
            else:
                err = resp.get("error") or resp.get("status", "engine error")

        elif configFn == "run_retry5":
            resp = callInference(chatID, db, q["question"], retryCount=5)
            if resp.get("code") == 200:
                ev = resp["agentResponse"]["evaluation"]
                predictedSQL = ev["currentQuery"]
                predRows = ev["results"]
                if predictedSQL is None:
                    err = "exhausted retries"
            else:
                err = resp.get("error") or resp.get("status", "engine error")

    except httpx.TimeoutException:
        err = "timeout"
    except Exception as e:
        err = str(e)

    elapsed = time.time() - start
    match = resultsMatch(q["gold_result"], predRows)

    return {
        "id": q["id"], "database": db, "difficulty": q["difficulty"],
        "question": q["question"], "gold_query": q["gold_query"],
        "predicted_sql": predictedSQL, "match": match,
        "error": err, "latency_s": round(elapsed, 2), "config": configName,
    }


# %%
# Single-config evaluation loop with parallel execution
def runConfig(configName: str, configFn: str, questions: list) -> list:
    """
    Run one evaluation configuration across all questions in parallel.

    The first question per database is run sequentially to warm up the
    schema cache in the engine. Remaining questions are dispatched to a
    thread pool for parallel execution.

    Arguments:
    ----------
    configName : str
        Short label (``C``, ``B``, or ``A``).
    configFn : str
        Internal identifier for the config mode.
    questions : list
        The full question set loaded from ``questions.json``.

    Returns:
    --------
    list[dict]
        Per-question result records.
    """
    label = CONFIG_LABELS[configName]
    logger.info("=" * 70)
    logger.info("Starting %s | %s (max %d workers)", f"Config {configName}", label, MAX_WORKERS)
    logger.info("=" * 70)

    # Phase 1: Warm up schema cache — run the first question per database
    # sequentially so the engine generates and caches the schema description.
    warmedDBs = set()
    warmupQuestions = []
    remainingQuestions = []

    for q in questions:
        if q["database"] not in warmedDBs:
            warmupQuestions.append(q)
            warmedDBs.add(q["database"])
        else:
            remainingQuestions.append(q)

    resultsMap = {}
    logger.info("Phase 1: Warming schema cache for %d databases...", len(warmupQuestions))
    for q in warmupQuestions:
        result = _runQuestion(configName, configFn, q)
        resultsMap[result["id"]] = result
        status = "PASS" if result["match"] else "FAIL"
        logger.info(
            "Config %s | %s | #%-3d %-10s | %s | %6.1fs | %s (warmup)",
            configName, result["database"], result["id"], result["difficulty"].upper(),
            status, result["latency_s"], result["question"][:55],
        )

    # Phase 2: Run remaining questions in parallel
    logger.info("Phase 2: Running %d questions with %d parallel workers...", len(remainingQuestions), MAX_WORKERS)
    completed = 0
    total = len(remainingQuestions)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_runQuestion, configName, configFn, q): q
            for q in remainingQuestions
        }
        for future in as_completed(futures):
            result = future.result()
            resultsMap[result["id"]] = result
            completed += 1

            status = "PASS" if result["match"] else "FAIL"
            logger.info(
                "Config %s | %s | #%-3d %-10s | %s | %6.1fs | [%d/%d] %s",
                configName, result["database"], result["id"], result["difficulty"].upper(),
                status, result["latency_s"], completed, total, result["question"][:50],
            )
            if result["error"] and not result["match"]:
                logger.info("  Error: %s", str(result["error"])[:120])

            # Save incrementally
            currentResults = sorted(resultsMap.values(), key=lambda r: r["id"])
            path = os.path.join(RESULTS_DIR, f"results_config_{configName.lower()}.json")
            with open(path, "w") as f:
                json.dump(currentResults, f, indent=2, default=str)

    # Final sorted results
    results = sorted(resultsMap.values(), key=lambda r: r["id"])
    path = os.path.join(RESULTS_DIR, f"results_config_{configName.lower()}.json")
    with open(path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    correct = sum(1 for r in results if r["match"])
    logger.info("Config %s complete: %d/%d (%.1f%%)", configName, correct, len(results), correct / len(results) * 100)
    return results


# %%
# Main evaluation orchestrator
def runEvaluation():
    """
    Run the full 3-config ablation study.

    Loads questions from disk, waits for the engine, then runs
    Config C → B → A with a Redis flush between each.
    """
    os.makedirs(RESULTS_DIR, exist_ok=True)

    with open(QUESTIONS_PATH) as f:
        questions = json.load(f)

    logger.info("Loaded %d questions from %s", len(questions), QUESTIONS_PATH)
    logger.info("Engine: %s", ENGINE_URL)
    logger.info("PostgreSQL (direct): %s:%s", POSTGRES_HOST, POSTGRES_PORT)
    logger.info("Redis: %s:%s", REDIS_HOST, REDIS_PORT)

    if not waitForEngine():
        sys.exit(1)

    configs = [("C", "generate_only"), ("B", "run_retry1"), ("A", "run_retry5")]
    for configName, configFn in configs:
        flushRedis()
        runConfig(configName, configFn, questions)

    logger.info("=" * 70)
    logger.info("All configurations complete. Results in %s/", RESULTS_DIR)


# %%
# Execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    runEvaluation()
