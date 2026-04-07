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

from birdConfig import (
    ENGINE_URL, ENGINE_PG_HOST, ENGINE_PG_PORT,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD,
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD,
    RESULTS_DIR, RESULTS_BASE_DIR, TIMEOUT_SECONDS,
)
from resultComparator import resultsMatch
from resourceMetrics import WallTimer, getPeakMemoryMB

logger = logging.getLogger(__name__)

# Maximum number of parallel requests to the engine.
# Tune based on LLM endpoint throughput and available resources.
MAX_WORKERS = int(os.environ.get("EVAL_MAX_WORKERS", "4"))


# %%
# API call helpers — thin wrappers around the engine REST endpoints
def _apiParams(database: str) -> dict:
    """
    Build query parameters to override the engine's target database.

    Arguments:
    ----------
    database : str
        PostgreSQL database name (e.g. ``bird_concert_singer``).

    Returns:
    --------
    dict
        Query parameter dict consumed by the engine's connection dependency.
    """
    return {
        "postgreHost":     ENGINE_PG_HOST,
        "postgrePort":     ENGINE_PG_PORT,
        "postgreUser":     POSTGRES_USER,
        "postgrePassword": POSTGRES_PASSWORD,
        "postgreDBName":   database,
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
    Run a single BIRD question for one evaluation configuration.

    Questions marked ``gold_conversion_error`` are short-circuited and
    returned as excluded records without calling the engine.

    Arguments:
    ----------
    configName : str
        Short label (``C``, ``B``, or ``A``).
    configFn : str
        Internal identifier for the config mode.
    q : dict
        A single internal question record from ``questions.json``.

    Returns:
    --------
    dict
        Result record with all required fields including ``excluded``.
    """
    # Short-circuit for questions excluded due to gold conversion errors
    if q.get("gold_conversion_error"):
        return {
            "id":            q["id"],
            "database":      q["database"],
            "difficulty":    q["difficulty"],
            "question":      q["question"],
            "evidence":      q.get("evidence", ""),
            "gold_query":    q["gold_query"],
            "predicted_sql": None,
            "match":         False,
            "error":         "gold_conversion_error",
            "latency_s":     0.0,
            "config":        configName,
            "excluded":      True,
        }

    db     = q["database"]
    dbId   = q["db_id"]
    # Use original db_id in chatID to align schema cache with the BIRD database
    chatID = f"bird_{configName.lower()}_{dbId}"
    start  = time.time()

    predictedSQL = None
    predRows     = None
    err          = None

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
        "id":            q["id"],
        "database":      db,
        "difficulty":    q["difficulty"],
        "question":      q["question"],
        "evidence":      q.get("evidence", ""),
        "gold_query":    q["gold_query"],
        "predicted_sql": predictedSQL,
        "match":         match,
        "error":         err,
        "latency_s":     round(elapsed, 2),
        "config":        configName,
        "excluded":      False,
    }


# %%
# Single-config evaluation loop with parallel execution
def runConfig(configName: str, configFn: str, questions: list) -> list:
    """
    Run one BIRD evaluation configuration across all questions.

    Excluded questions (gold_conversion_error) are processed immediately
    without engine calls. Non-excluded questions follow the same
    warmup-then-parallel pattern as the base evalRunner.

    Arguments:
    ----------
    configName : str
        Short label (``C``, ``B``, or ``A``).
    configFn : str
        Internal identifier for the config mode.
    questions : list[dict]
        Full internal question set with gold results populated.

    Returns:
    --------
    list[dict]
        Per-question result records sorted by id.
    """
    label = CONFIG_LABELS[configName]
    configTimer = WallTimer()
    configTimer.start()
    logger.info("=" * 70)
    logger.info("Starting %s | %s (max %d workers)", f"Config {configName}", label, MAX_WORKERS)
    logger.info("=" * 70)

    # Separate excluded questions immediately — they need no engine calls or warmup
    excludedQuestions = [q for q in questions if q.get("gold_conversion_error")]
    activeQuestions   = [q for q in questions if not q.get("gold_conversion_error")]

    # Phase 1: Warm up schema cache — run the first question per db_id
    # sequentially so the engine generates and caches the schema description.
    warmedDBs         = set()
    warmupQuestions   = []
    remainingQuestions = []

    for q in activeQuestions:
        if q["db_id"] not in warmedDBs:
            warmupQuestions.append(q)
            warmedDBs.add(q["db_id"])
        else:
            remainingQuestions.append(q)

    resultsMap = {}

    # Record excluded questions directly
    for q in excludedQuestions:
        result = _runQuestion(configName, configFn, q)
        resultsMap[result["id"]] = result
        logger.info(
            "Config %s | %s | #%-3d %-12s | EXCL | gold_conversion_error",
            configName, result["database"], result["id"], result["difficulty"].upper(),
        )

    logger.info(
        "Phase 1: Warming schema cache for %d databases (%d excluded questions skipped)...",
        len(warmupQuestions), len(excludedQuestions),
    )
    for q in warmupQuestions:
        result = _runQuestion(configName, configFn, q)
        resultsMap[result["id"]] = result
        status = "PASS" if result["match"] else "FAIL"
        logger.info(
            "Config %s | %s | #%-3d %-12s | %s | %6.1fs | %s (warmup)",
            configName, result["database"], result["id"], result["difficulty"].upper(),
            status, result["latency_s"], result["question"][:55],
        )

    # Phase 2: Run remaining active questions in parallel
    logger.info(
        "Phase 2: Running %d questions with %d parallel workers...",
        len(remainingQuestions), MAX_WORKERS,
    )
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
                "Config %s | %s | #%-3d %-12s | %s | %6.1fs | [%d/%d] %s",
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

    configTimer.stop()
    nonExcluded = [r for r in results if not r.get("excluded")]
    correct = sum(1 for r in nonExcluded if r["match"])
    excludedCount = len(results) - len(nonExcluded)
    logger.info(
        "Config %s complete: %d/%d (%.1f%%) [%d excluded] in %.1fs | peak memory %.1f MB",
        configName, correct, len(nonExcluded),
        correct / len(nonExcluded) * 100 if nonExcluded else 0,
        excludedCount, configTimer.elapsed, getPeakMemoryMB(),
    )

    # Save resource metrics alongside results
    metricsPath = os.path.join(RESULTS_DIR, f"metrics_config_{configName.lower()}.json")
    latencies = [r["latency_s"] for r in nonExcluded]
    from resourceMetrics import latencyPercentiles, throughput
    metrics = {
        "config": configName,
        "wall_time_s": configTimer.elapsed,
        "peak_memory_mb": getPeakMemoryMB(),
        "questions_evaluated": len(nonExcluded),
        "questions_excluded": excludedCount,
        "throughput_qpm": throughput(len(nonExcluded), configTimer.elapsed),
        "latency_percentiles": latencyPercentiles(latencies),
    }
    with open(metricsPath, "w") as f:
        json.dump(metrics, f, indent=2)

    return results


# %%
# Main evaluation orchestrator
def runEvaluation(questionsPath: str):
    """
    Run the full 3-config BIRD ablation study.

    Loads questions from disk and runs Config C → B → A with a Redis
    flush between each configuration.

    Arguments:
    ----------
    questionsPath : str
        Path to the prepared questions JSON file with gold results
        populated by ``sqliteToPostgres.executeGoldSQL``.
    """
    os.makedirs(RESULTS_DIR, exist_ok=True)

    with open(questionsPath) as f:
        questions = json.load(f)

    total    = len(questions)
    excluded = sum(1 for q in questions if q.get("gold_conversion_error"))

    logger.info("Loaded %d questions from %s", total, questionsPath)
    logger.info("  Excluded (gold conversion errors): %d", excluded)
    logger.info("  Active for evaluation: %d", total - excluded)
    logger.info("Engine: %s", ENGINE_URL)
    logger.info("PostgreSQL (direct): %s:%s", POSTGRES_HOST, POSTGRES_PORT)
    logger.info("Redis: %s:%s", REDIS_HOST, REDIS_PORT)

    # Log difficulty distribution for active questions
    from collections import Counter
    diffs = Counter(q["difficulty"] for q in questions if not q.get("gold_conversion_error"))
    for diff in ["simple", "moderate", "challenging"]:
        if diff in diffs:
            logger.info("  %-12s %d active questions", diff, diffs[diff])

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
    questionsPath = os.path.join(RESULTS_BASE_DIR, "questions.json")
    runEvaluation(questionsPath)
