#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import json
import logging
import os
from collections import Counter
from datetime import datetime, date
from decimal import Decimal

from evalConfig import dbConnect, RESULTS_DIR
from questions import QUESTION_BANK

logger = logging.getLogger(__name__)


# %%
# Gold query executor
def executeGold(dbname: str, sql: str) -> list:
    """
    Execute a gold SQL query and return the result as a list of lists.

    Arguments:
    ----------
    dbname : str
        Target database name.
    sql : str
        The gold SQL query to execute.

    Returns:
    --------
    list[list]
        Result rows with values converted to JSON-serializable types.
    """
    conn = dbConnect(dbname)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()

    serialized = []
    for row in rows:
        sRow = []
        for val in row:
            if isinstance(val, Decimal):
                sRow.append(float(val))
            elif isinstance(val, (datetime, date)):
                sRow.append(str(val))
            elif val is None:
                sRow.append(None)
            else:
                sRow.append(val)
        serialized.append(sRow)
    return serialized


# %%
# Question file generator
def generateQuestions():
    """
    Execute all gold queries and write ``questions.json`` to the results directory.

    Iterates over every question in the QUESTION_BANK, executes the gold query,
    captures the result, and writes the full question set with gold results.
    """
    allQuestions = []
    questionID = 1
    errors = []

    for dbName, questions in QUESTION_BANK.items():
        logger.info("Executing gold queries for %s (%d questions)", dbName, len(questions))

        for q in questions:
            try:
                goldResult = executeGold(dbName, q["gold_query"])
                status = "OK" if goldResult else "EMPTY"
            except Exception as e:
                goldResult = None
                status = f"ERROR: {e}"
                errors.append((questionID, q["question"], str(e)))

            allQuestions.append({
                "id": questionID,
                "database": dbName,
                "difficulty": q["difficulty"],
                "question": q["question"],
                "gold_query": q["gold_query"].strip(),
                "gold_result": goldResult,
            })
            logger.info("  #%-3d [%-10s] %s — %s", questionID, q["difficulty"], status, q["question"][:60])
            questionID += 1

    # Write output
    os.makedirs(RESULTS_DIR, exist_ok=True)
    outPath = os.path.join(RESULTS_DIR, "questions.json")
    with open(outPath, "w") as f:
        json.dump(allQuestions, f, indent=2, default=str)
    logger.info("Wrote %d questions to %s", len(allQuestions), outPath)

    # Summary
    byDiff = Counter(q["difficulty"] for q in allQuestions)
    byDB = Counter(q["database"] for q in allQuestions)
    empties = sum(1 for q in allQuestions if q["gold_result"] is not None and len(q["gold_result"]) == 0)

    logger.info("By difficulty: %s", dict(byDiff))
    logger.info("By database:   %s", dict(byDB))
    logger.info("Errors: %d | Empty results: %d", len(errors), empties)
    for qid, question, err in errors:
        logger.error("  ERROR #%d: %s — %s", qid, question[:60], err)


# %%
# Execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    generateQuestions()
