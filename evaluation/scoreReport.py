#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import json
import logging
import os
from collections import defaultdict

from tabulate import tabulate

from evalConfig import RESULTS_DIR, EVAL_DATABASES, LLM_MODEL, LLM_TEMPERATURE

logger = logging.getLogger(__name__)


# %%
# Scoring helpers
def loadResults(filename: str) -> list:
    """Load a results JSON file from the results directory."""
    with open(os.path.join(RESULTS_DIR, filename)) as f:
        return json.load(f)


def scoreConfig(results: list) -> dict:
    """
    Compute accuracy metrics for a single configuration's results.

    Returns:
    --------
    dict
        Keys: total, correct, accuracy, avg_latency_s, by_difficulty, by_database.
    """
    total = len(results)
    correct = sum(1 for r in results if r["match"])
    avgLatency = sum(r["latency_s"] for r in results) / total if total else 0

    byDifficulty = defaultdict(lambda: {"total": 0, "correct": 0})
    byDatabase = defaultdict(lambda: {"total": 0, "correct": 0})

    for r in results:
        byDifficulty[r["difficulty"]]["total"] += 1
        byDifficulty[r["difficulty"]]["correct"] += 1 if r["match"] else 0
        byDatabase[r["database"]]["total"] += 1
        byDatabase[r["database"]]["correct"] += 1 if r["match"] else 0

    return {
        "total": total,
        "correct": correct,
        "accuracy": round(correct / total * 100, 1) if total else 0,
        "avg_latency_s": round(avgLatency, 2),
        "by_difficulty": {
            k: round(v["correct"] / v["total"] * 100, 1) if v["total"] else 0
            for k, v in byDifficulty.items()
        },
        "by_database": {
            k: round(v["correct"] / v["total"] * 100, 1) if v["total"] else 0
            for k, v in byDatabase.items()
        },
    }


# %%
# Self-healing breakdown — the most important table
def healingBreakdown(resultsC: list, resultsA: list) -> dict:
    """
    Compare Config C (no healing) vs Config A (full healing)
    to quantify the self-healing loop's value.

    Returns:
    --------
    dict
        correct_first_attempt, fixed_by_healing, exhausted_retries,
        errors_or_timeouts, regressions.
    """
    cByID = {r["id"]: r["match"] for r in resultsC}
    aByID = {r["id"]: r for r in resultsA}

    correctFirst = fixedByHealing = exhausted = errorsTimeouts = regressions = 0

    for qid in cByID:
        cCorrect = cByID[qid]
        aEntry = aByID.get(qid, {})
        aCorrect = aEntry.get("match", False)
        aError = aEntry.get("error")

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
        "fixed_by_healing": fixedByHealing,
        "exhausted_retries": exhausted,
        "errors_or_timeouts": errorsTimeouts,
        "regressions": regressions,
    }


# %%
# Report generator
def generateReport():
    """
    Load all three config results, compute metrics, print tables,
    and save ``summary.json``.
    """
    resultsC = loadResults("results_config_c.json")
    resultsB = loadResults("results_config_b.json")
    resultsA = loadResults("results_config_a.json")

    scoreC = scoreConfig(resultsC)
    scoreB = scoreConfig(resultsB)
    scoreA = scoreConfig(resultsA)
    breakdown = healingBreakdown(resultsC, resultsA)

    # Table 1: Overall Execution Accuracy
    print("\n" + "=" * 60)
    print("TABLE 1: Overall Execution Accuracy (Ablation)")
    print("=" * 60)
    table1 = [
        ["C: Generation only", f"{scoreC['accuracy']}%", f"{scoreC['avg_latency_s']}s"],
        ["B: Single-shot (retryCount=1)", f"{scoreB['accuracy']}%", f"{scoreB['avg_latency_s']}s"],
        ["A: Full pipeline (retryCount=5)", f"{scoreA['accuracy']}%", f"{scoreA['avg_latency_s']}s"],
    ]
    print(tabulate(table1, headers=["Configuration", "Accuracy", "Avg Latency"], tablefmt="github"))

    # Table 2: Accuracy by Difficulty
    print("\n" + "=" * 60)
    print("TABLE 2: Accuracy by Difficulty")
    print("=" * 60)
    table2 = []
    for d in ["easy", "medium", "hard", "extra_hard"]:
        table2.append([
            d,
            f"{scoreC['by_difficulty'].get(d, 0)}%",
            f"{scoreB['by_difficulty'].get(d, 0)}%",
            f"{scoreA['by_difficulty'].get(d, 0)}%",
        ])
    print(tabulate(table2, headers=["Difficulty", "Config C", "Config B", "Config A"], tablefmt="github"))

    # Table 3: Accuracy by Database Domain
    print("\n" + "=" * 60)
    print("TABLE 3: Accuracy by Database Domain")
    print("=" * 60)
    table3 = []
    for db in EVAL_DATABASES:
        cAcc = scoreC["by_database"].get(db, 0)
        aAcc = scoreA["by_database"].get(db, 0)
        delta = round(aAcc - cAcc, 1)
        table3.append([db, f"{cAcc}%", f"{aAcc}%", f"+{delta}%" if delta >= 0 else f"{delta}%"])
    print(tabulate(table3, headers=["Database", "Config C", "Config A", "Delta"], tablefmt="github"))

    # Table 4: Self-Healing Breakdown
    print("\n" + "=" * 60)
    print("TABLE 4: Self-Healing Breakdown (Config A only)")
    print("=" * 60)
    total = len(resultsA)
    table4 = [
        ["Correct on 1st attempt (no healing needed)", f"{breakdown['correct_first_attempt']} / {total}"],
        ["Incorrect on 1st attempt, fixed by healing", f"{breakdown['fixed_by_healing']} / {total}"],
        ["Exhausted all retries (still incorrect)", f"{breakdown['exhausted_retries']} / {total}"],
        ["Errors / timeouts", f"{breakdown['errors_or_timeouts']} / {total}"],
        ["Regressions (correct in C, incorrect in A)", f"{breakdown['regressions']} / {total}"],
    ]
    print(tabulate(table4, headers=["Metric", "Count"], tablefmt="github"))

    # Save summary.json
    summary = {
        "model": LLM_MODEL,
        "llm_temperature": LLM_TEMPERATURE,
        "total_questions": total,
        "databases": EVAL_DATABASES,
        "config_c": {"accuracy": scoreC["accuracy"], "correct": scoreC["correct"],
                     "avg_latency_s": scoreC["avg_latency_s"], "by_difficulty": scoreC["by_difficulty"],
                     "by_database": scoreC["by_database"]},
        "config_b": {"accuracy": scoreB["accuracy"], "correct": scoreB["correct"],
                     "avg_latency_s": scoreB["avg_latency_s"], "by_difficulty": scoreB["by_difficulty"],
                     "by_database": scoreB["by_database"]},
        "config_a": {"accuracy": scoreA["accuracy"], "correct": scoreA["correct"],
                     "avg_latency_s": scoreA["avg_latency_s"], "by_difficulty": scoreA["by_difficulty"],
                     "by_database": scoreA["by_database"]},
        "healing_breakdown": breakdown,
    }
    summaryPath = os.path.join(RESULTS_DIR, "summary.json")
    with open(summaryPath, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\nSummary saved to {summaryPath}")
    print(f"\nKey finding: Self-healing provides +{round(scoreA['accuracy'] - scoreC['accuracy'], 1)} pp improvement (Config A vs C)")
    if breakdown["fixed_by_healing"] > 0:
        print(f"The healing loop fixed {breakdown['fixed_by_healing']} initially-failing queries.")


# %%
# Execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    generateReport()
