#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import json
import logging
import os
from collections import defaultdict

from tabulate import tabulate

from birdConfig import RESULTS_DIR, LLM_MODEL, LLM_TEMPERATURE
from resourceMetrics import latencyPercentiles, throughput, getPeakMemoryMB

logger = logging.getLogger(__name__)


# %%
# Published BIRD benchmark baselines for paper comparison (EX on mini-dev)
PUBLISHED_BASELINES = {
    "ChatGPT (zero-shot)":  40.08,
    "GPT-4 (zero-shot)":    46.35,
    "GPT-4 + evidence":     54.89,
    "DIN-SQL + GPT-4":      55.9,
    "DAIL-SQL + GPT-4":     54.76,
    "CHASE-SQL (SOTA)":     73.01,
}

DIFFICULTY_ORDER = ["simple", "moderate", "challenging"]


# %%
# Scoring helpers
def loadResults(filename: str) -> list:
    """Load a BIRD results JSON file from the results directory."""
    with open(os.path.join(RESULTS_DIR, filename)) as f:
        return json.load(f)


def scoreConfig(results: list) -> dict:
    """
    Compute BIRD execution accuracy metrics, excluding
    ``gold_conversion_error`` questions from all calculations.

    Arguments:
    ----------
    results : list[dict]
        Per-question result records from ``birdEvalRunner``.

    Returns:
    --------
    dict
        Keys: total, excluded, evaluated, correct, accuracy,
        avg_latency_s, by_difficulty, by_database.
    """
    excluded  = [r for r in results if r.get("excluded")]
    evaluated = [r for r in results if not r.get("excluded")]

    total    = len(results)
    correct  = sum(1 for r in evaluated if r["match"])
    avgLatency = (
        sum(r["latency_s"] for r in evaluated) / len(evaluated)
        if evaluated else 0
    )

    byDifficulty = defaultdict(lambda: {"total": 0, "correct": 0})
    byDatabase   = defaultdict(lambda: {"total": 0, "correct": 0})

    for r in evaluated:
        byDifficulty[r["difficulty"]]["total"]   += 1
        byDifficulty[r["difficulty"]]["correct"] += 1 if r["match"] else 0
        byDatabase[r["database"]]["total"]        += 1
        byDatabase[r["database"]]["correct"]      += 1 if r["match"] else 0

    return {
        "total":      total,
        "excluded":   len(excluded),
        "evaluated":  len(evaluated),
        "correct":    correct,
        "accuracy":   round(correct / len(evaluated) * 100, 1) if evaluated else 0,
        "avg_latency_s": round(avgLatency, 2),
        "by_difficulty": {
            k: round(v["correct"] / v["total"] * 100, 1) if v["total"] else 0
            for k, v in byDifficulty.items()
        },
        "by_database": {
            k: {
                "accuracy": round(v["correct"] / v["total"] * 100, 1) if v["total"] else 0,
                "total":    v["total"],
            }
            for k, v in byDatabase.items()
        },
    }


# %%
# Self-healing breakdown — compares Config C vs Config A
def healingBreakdown(resultsC: list, resultsA: list) -> dict:
    """
    Compare Config C (no healing) vs Config A (full healing) to
    quantify the self-healing loop's contribution.

    Excluded questions are filtered from both sides before comparison.

    Arguments:
    ----------
    resultsC : list[dict]
        Results from Config C (generation only).
    resultsA : list[dict]
        Results from Config A (full pipeline, retryCount=5).

    Returns:
    --------
    dict
        correct_first_attempt, fixed_by_healing, exhausted_retries,
        errors_or_timeouts, regressions.
    """
    cByID = {r["id"]: r["match"] for r in resultsC if not r.get("excluded")}
    aByID = {r["id"]: r for r in resultsA if not r.get("excluded")}

    correctFirst = fixedByHealing = exhausted = errorsTimeouts = regressions = 0

    for qid in cByID:
        cCorrect = cByID[qid]
        aEntry   = aByID.get(qid, {})
        aCorrect = aEntry.get("match", False)
        aError   = aEntry.get("error")

        if aError == "timeout" or (aError and aError != "exhausted retries"):
            errorsTimeouts += 1
        elif cCorrect and aCorrect:
            correctFirst += 1
        elif not cCorrect and aCorrect:
            fixedByHealing += 1
        elif cCorrect and not aCorrect:
            regressions += 1
        else:
            exhausted += 1

    return {
        "correct_first_attempt": correctFirst,
        "fixed_by_healing":      fixedByHealing,
        "exhausted_retries":     exhausted,
        "errors_or_timeouts":    errorsTimeouts,
        "regressions":           regressions,
    }


# %%
# Report generator
def generateReport():
    """
    Load all three config results, compute metrics, print 5 tables,
    and write summary.json, predictions.txt, and conversion_report.json.
    """
    resultsC = loadResults("results_config_c.json")
    resultsB = loadResults("results_config_b.json")
    resultsA = loadResults("results_config_a.json")

    scoreC    = scoreConfig(resultsC)
    scoreB    = scoreConfig(resultsB)
    scoreA    = scoreConfig(resultsA)
    breakdown = healingBreakdown(resultsC, resultsA)

    # ------------------------------------------------------------------
    # Table 1: Overall Execution Accuracy
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("TABLE 1: Overall BIRD Execution Accuracy (Ablation)")
    print("=" * 70)
    table1 = [
        [
            "C: Generation only",
            f"{scoreC['accuracy']}%",
            scoreC["evaluated"],
            scoreC["excluded"],
            f"{scoreC['avg_latency_s']}s",
        ],
        [
            "B: Single-shot (retryCount=1)",
            f"{scoreB['accuracy']}%",
            scoreB["evaluated"],
            scoreB["excluded"],
            f"{scoreB['avg_latency_s']}s",
        ],
        [
            "A: Full pipeline (retryCount=5)",
            f"{scoreA['accuracy']}%",
            scoreA["evaluated"],
            scoreA["excluded"],
            f"{scoreA['avg_latency_s']}s",
        ],
    ]
    print(tabulate(
        table1,
        headers=["Configuration", "EX Accuracy", "Evaluated", "Excluded", "Avg Latency"],
        tablefmt="github",
    ))

    # ------------------------------------------------------------------
    # Table 2: Accuracy by Difficulty
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("TABLE 2: Accuracy by Difficulty")
    print("=" * 70)
    table2 = []
    for d in DIFFICULTY_ORDER:
        table2.append([
            d,
            f"{scoreC['by_difficulty'].get(d, 0)}%",
            f"{scoreB['by_difficulty'].get(d, 0)}%",
            f"{scoreA['by_difficulty'].get(d, 0)}%",
        ])
    print(tabulate(table2, headers=["Difficulty", "Config C", "Config B", "Config A"], tablefmt="github"))

    # ------------------------------------------------------------------
    # Table 3: Accuracy by Database Domain (top-10 by question count)
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("TABLE 3: Accuracy by Database Domain (top-10)")
    print("=" * 70)

    # Build combined database stats from Config A results
    allDBs = sorted(
        scoreA["by_database"].items(),
        key=lambda kv: kv[1]["total"],
        reverse=True,
    )[:10]

    table3 = []
    for db, aStats in allDBs:
        cAcc  = scoreC["by_database"].get(db, {}).get("accuracy", 0)
        aAcc  = aStats["accuracy"]
        delta = round(aAcc - cAcc, 1)
        table3.append([
            db,
            aStats["total"],
            f"{cAcc}%",
            f"{aAcc}%",
            f"+{delta}%" if delta >= 0 else f"{delta}%",
        ])
    print(tabulate(
        table3,
        headers=["Database", "Questions", "Config C", "Config A", "Delta"],
        tablefmt="github",
    ))

    # ------------------------------------------------------------------
    # Table 4: Self-Healing Breakdown
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("TABLE 4: Self-Healing Breakdown (Config A only)")
    print("=" * 70)
    evaluated = scoreA["evaluated"]
    table4 = [
        ["Correct on 1st attempt (no healing needed)",    f"{breakdown['correct_first_attempt']} / {evaluated}"],
        ["Incorrect on 1st attempt, fixed by healing",    f"{breakdown['fixed_by_healing']} / {evaluated}"],
        ["Exhausted all retries (still incorrect)",       f"{breakdown['exhausted_retries']} / {evaluated}"],
        ["Errors / timeouts",                             f"{breakdown['errors_or_timeouts']} / {evaluated}"],
        ["Regressions (correct in C, incorrect in A)",    f"{breakdown['regressions']} / {evaluated}"],
    ]
    print(tabulate(table4, headers=["Metric", "Count"], tablefmt="github"))

    # ------------------------------------------------------------------
    # Table 5: Comparison vs Published Baselines
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("TABLE 5: Comparison vs Published BIRD Baselines")
    print("=" * 70)

    # Sort baselines ascending by accuracy, append "This System" rows at end
    baselineRows = sorted(PUBLISHED_BASELINES.items(), key=lambda kv: kv[1])
    table5 = [[name, f"{acc}%"] for name, acc in baselineRows]
    table5.append([f"This System (Config C)", f"{scoreC['accuracy']}%"])
    table5.append([f"This System (Config A)", f"{scoreA['accuracy']}%"])
    print(tabulate(table5, headers=["System", "EX Accuracy"], tablefmt="github"))

    # ------------------------------------------------------------------
    # Table 6: Resource Metrics
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("TABLE 6: Resource Metrics")
    print("=" * 70)
    table6 = []
    for configLabel, configResults, configName in [
        ("C: Generation only", resultsC, "c"),
        ("B: Single-shot (retryCount=1)", resultsB, "b"),
        ("A: Full pipeline (retryCount=5)", resultsA, "a"),
    ]:
        nonExcl = [r for r in configResults if not r.get("excluded")]
        latencies = [r["latency_s"] for r in nonExcl]
        pctls = latencyPercentiles(latencies)
        metricsFile = os.path.join(RESULTS_DIR, f"metrics_config_{configName}.json")
        wallTime = tput = peakMem = "-"
        if os.path.isfile(metricsFile):
            with open(metricsFile) as mf:
                m = json.load(mf)
                wallTime = f"{m.get('wall_time_s', 0):.1f}s"
                tput = f"{m.get('throughput_qpm', 0):.1f}"
                peakMem = f"{m.get('peak_memory_mb', 0):.1f} MB"
        table6.append([
            configLabel,
            wallTime, tput, peakMem,
            f"{pctls['median']}s", f"{pctls['p90']}s", f"{pctls['p95']}s",
        ])
    print(tabulate(
        table6,
        headers=["Configuration", "Wall Time", "Throughput (q/min)", "Peak Memory",
                 "Latency p50", "Latency p90", "Latency p95"],
        tablefmt="github",
    ))

    # Build resource metrics for summary
    resourceMetrics = {}
    for configName in ["c", "b", "a"]:
        metricsFile = os.path.join(RESULTS_DIR, f"metrics_config_{configName}.json")
        if os.path.isfile(metricsFile):
            with open(metricsFile) as mf:
                resourceMetrics[f"config_{configName}"] = json.load(mf)

    # ------------------------------------------------------------------
    # Write summary.json
    # ------------------------------------------------------------------
    summary = {
        "model":             LLM_MODEL,
        "llm_temperature":   LLM_TEMPERATURE,
        "total_questions":   scoreA["total"],
        "excluded":          scoreA["excluded"],
        "evaluated":         scoreA["evaluated"],
        "config_c": {
            "accuracy":      scoreC["accuracy"],
            "correct":       scoreC["correct"],
            "avg_latency_s": scoreC["avg_latency_s"],
            "by_difficulty": scoreC["by_difficulty"],
            "by_database":   {k: v["accuracy"] for k, v in scoreC["by_database"].items()},
            "latency_percentiles": latencyPercentiles([r["latency_s"] for r in resultsC if not r.get("excluded")]),
        },
        "config_b": {
            "accuracy":      scoreB["accuracy"],
            "correct":       scoreB["correct"],
            "avg_latency_s": scoreB["avg_latency_s"],
            "by_difficulty": scoreB["by_difficulty"],
            "by_database":   {k: v["accuracy"] for k, v in scoreB["by_database"].items()},
            "latency_percentiles": latencyPercentiles([r["latency_s"] for r in resultsB if not r.get("excluded")]),
        },
        "config_a": {
            "accuracy":      scoreA["accuracy"],
            "correct":       scoreA["correct"],
            "avg_latency_s": scoreA["avg_latency_s"],
            "by_difficulty": scoreA["by_difficulty"],
            "by_database":   {k: v["accuracy"] for k, v in scoreA["by_database"].items()},
            "latency_percentiles": latencyPercentiles([r["latency_s"] for r in resultsA if not r.get("excluded")]),
        },
        "healing_breakdown": breakdown,
        "resource_metrics": resourceMetrics,
    }
    summaryPath = os.path.join(RESULTS_DIR, "summary.json")
    with open(summaryPath, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary saved to {summaryPath}")

    # ------------------------------------------------------------------
    # Write predictions.txt — official BIRD submission format (Config A)
    # ------------------------------------------------------------------
    predictionsPath = os.path.join(RESULTS_DIR, "predictions.txt")
    sortedA = sorted(resultsA, key=lambda r: r["id"])
    with open(predictionsPath, "w") as f:
        for r in sortedA:
            if r.get("excluded"):
                continue
            sql   = r.get("predicted_sql") or ""
            # Sanitize: remove newlines from SQL for the one-line format
            sql   = sql.replace("\n", " ").replace("\r", " ").strip()
            db_id = r.get("database", "").removeprefix("bird_")
            f.write(f"{sql}\t----- bird -----\t{db_id}\n")
    print(f"Predictions saved to {predictionsPath}")

    # ------------------------------------------------------------------
    # Write conversion_report.json
    # ------------------------------------------------------------------
    convErrors = [r for r in resultsA if r.get("excluded")]
    errByDiff  = defaultdict(int)
    errByDB    = defaultdict(int)
    for r in convErrors:
        errByDiff[r["difficulty"]] += 1
        errByDB[r["database"]]     += 1

    convReport = {
        "total_questions":    scoreA["total"],
        "conversion_errors":  len(convErrors),
        "error_rate":         round(len(convErrors) / scoreA["total"] * 100, 1) if scoreA["total"] else 0,
        "error_by_difficulty": dict(errByDiff),
        "error_by_database":   dict(errByDB),
    }
    convPath = os.path.join(RESULTS_DIR, "conversion_report.json")
    with open(convPath, "w") as f:
        json.dump(convReport, f, indent=2)
    print(f"Conversion report saved to {convPath}")

    # ------------------------------------------------------------------
    # Key findings summary
    # ------------------------------------------------------------------
    delta = round(scoreA["accuracy"] - scoreC["accuracy"], 1)
    print(f"\nKey finding: Self-healing provides +{delta} pp improvement (Config A vs C)")
    if breakdown["fixed_by_healing"] > 0:
        print(f"The healing loop fixed {breakdown['fixed_by_healing']} initially-failing queries.")
    if scoreA["excluded"] > 0:
        print(
            f"Note: {scoreA['excluded']} questions excluded due to gold SQL conversion errors "
            f"({convReport['error_rate']}% of total)."
        )


# %%
# Execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    generateReport()
