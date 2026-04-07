#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import json
import logging
import os
import re

from birdConfig import BIRD_DATA_DIR, BIRD_DATASET, BIRD_HF_DATASET, BIRD_USE_EVIDENCE, RESULTS_BASE_DIR

logger = logging.getLogger(__name__)


# %%
# Prompt construction — optionally prepends BIRD evidence to the question
def _buildPrompt(question: str, evidence: str) -> str:
    """
    Build the engine prompt, optionally prepending evidence.

    Arguments:
    ----------
    question : str
        The natural-language question from the BIRD dataset.
    evidence : str
        Domain-specific hints provided alongside the question.

    Returns:
    --------
    str
        Final prompt string to be sent to the engine as ``basePrompt``.
    """
    if BIRD_USE_EVIDENCE and evidence and evidence.strip():
        return f"Given the following information: {evidence}\n\nQuestion: {question}"
    return question


# %%
# SQLite → PostgreSQL dialect conversion for gold SQL queries
def _convertGoldSQL(sql: str) -> str:
    """
    Convert a SQLite-dialect gold SQL query to PostgreSQL dialect.

    Applies transformations in a fixed priority order to avoid
    double-substitutions. Conversion is best-effort; queries that
    still fail on PostgreSQL are marked ``gold_conversion_error`` by
    ``sqliteToPostgres.executeGoldSQL``.

    Arguments:
    ----------
    sql : str
        Gold SQL query in SQLite dialect from the BIRD dataset.

    Returns:
    --------
    str
        Equivalent SQL in PostgreSQL dialect.
    """
    # 1. Backtick identifiers → double-quoted identifiers
    sql = re.sub(r'`([^`]+)`', r'"\1"', sql)

    # 2. IIF(cond, true, false) → CASE WHEN cond THEN true ELSE false END
    #    Uses a balanced-paren scanner to handle nested function calls in args.
    def _replaceAllIIF(sql):
        """Iteratively replace all IIF(...) calls with CASE WHEN ... END."""
        result = sql
        while True:
            # Find the next IIF( occurrence (case-insensitive)
            match = re.search(r'(?i)\bIIF\s*\(', result)
            if not match:
                break
            start = match.start()
            parenStart = match.end() - 1  # position of the '('
            # Find the matching closing paren
            depth = 1
            pos = parenStart + 1
            while pos < len(result) and depth > 0:
                if result[pos] == '(':
                    depth += 1
                elif result[pos] == ')':
                    depth -= 1
                pos += 1
            if depth != 0:
                break  # unbalanced — give up
            inner = result[parenStart + 1 : pos - 1]
            # Split inner on top-level commas
            depth = 0
            parts = []
            current = []
            for ch in inner:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                elif ch == ',' and depth == 0:
                    parts.append(''.join(current).strip())
                    current = []
                    continue
                current.append(ch)
            parts.append(''.join(current).strip())
            if len(parts) == 3:
                replacement = f"CASE WHEN {parts[0]} THEN {parts[1]} ELSE {parts[2]} END"
            else:
                replacement = result[start:pos]  # leave unchanged
            result = result[:start] + replacement + result[pos:]
        return result

    sql = _replaceAllIIF(sql)

    # 3. STRFTIME('%Y', col) → EXTRACT(YEAR FROM col::TEXT::TIMESTAMP)
    #    STRFTIME('%m', col) → LPAD(EXTRACT(MONTH FROM col::TEXT::TIMESTAMP)::INTEGER::TEXT, 2, '0')
    _strftime_map = {
        '%Y': 'YEAR',
        '%m': 'MONTH',
        '%d': 'DAY',
        '%H': 'HOUR',
        '%M': 'MINUTE',
        '%S': 'SECOND',
    }

    def _replaceStrftime(m):
        fmt  = m.group(1)
        expr = m.group(2).strip()
        field = _strftime_map.get(fmt)
        if field:
            castExpr = f"CAST(EXTRACT({field} FROM ({expr})::TEXT::TIMESTAMP) AS INTEGER)"
            if fmt == '%m':
                return f"LPAD({castExpr}::TEXT, 2, '0')"
            return f"{castExpr}::TEXT"
        return m.group(0)  # unsupported format — leave unchanged

    sql = re.sub(
        r"(?i)\bSTRFTIME\s*\(\s*'([^']+)'\s*,\s*(.+?)\s*\)(?=\s|,|\)|$)",
        _replaceStrftime,
        sql,
    )

    # 4. JULIANDAY(x) - JULIANDAY(y) → (x::DATE - y::DATE)
    #    Best-effort: wrap JULIANDAY(expr) → (expr)::DATE as Julian day
    sql = re.sub(
        r"(?i)\bJULIANDAY\s*\(\s*(.+?)\s*\)(?=\s|,|\)|\-|\+|$)",
        r"((\1)::DATE - DATE '0001-01-01')",
        sql,
    )

    # 5. GROUP_CONCAT(x ORDER BY y) → STRING_AGG(x, ',' ORDER BY y)
    #    Must come before the simpler GROUP_CONCAT patterns.
    sql = re.sub(
        r'(?i)\bGROUP_CONCAT\s*\(([^)]+?)\s+ORDER\s+BY\s+([^)]+)\)',
        r"STRING_AGG(\1, ',' ORDER BY \2)",
        sql,
    )

    # 6. GROUP_CONCAT(x, sep) → STRING_AGG(x, sep)
    sql = re.sub(
        r'(?i)\bGROUP_CONCAT\s*\(([^,)]+),\s*([^)]+)\)',
        r'STRING_AGG(\1, \2)',
        sql,
    )

    # 7. GROUP_CONCAT(x) → STRING_AGG(x, ',')
    sql = re.sub(
        r'(?i)\bGROUP_CONCAT\s*\(([^)]+)\)',
        r"STRING_AGG(\1, ',')",
        sql,
    )

    # 8. SUBSTR( → SUBSTRING(
    sql = re.sub(r'(?i)\bSUBSTR\s*\(', 'SUBSTRING(', sql)

    # 9. IFNULL( → COALESCE(
    sql = re.sub(r'(?i)\bIFNULL\s*\(', 'COALESCE(', sql)

    # 10. INSTR(haystack, needle) → POSITION(needle IN haystack)
    sql = re.sub(
        r'(?i)\bINSTR\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)',
        r'POSITION(\2 IN \1)',
        sql,
    )

    # 11. LIKE → ILIKE
    #     SQLite LIKE is case-insensitive for ASCII; PostgreSQL LIKE is
    #     case-sensitive. Converting to ILIKE preserves SQLite semantics.
    sql = re.sub(r'(?i)\bLIKE\b', 'ILIKE', sql)

    # 12. DATE('now') → CURRENT_DATE
    sql = re.sub(r"(?i)DATE\s*\(\s*'now'\s*\)", 'CURRENT_DATE', sql)

    # 13. DATETIME('now') → NOW()
    sql = re.sub(r"(?i)DATETIME\s*\(\s*'now'\s*\)", 'NOW()', sql)

    # 14. CAST(... AS FLOAT) → CAST(... AS DOUBLE PRECISION)
    #     SQLite FLOAT is an alias; PostgreSQL requires explicit type.
    sql = re.sub(r'(?i)\bCAST\s*\((.+?)\s+AS\s+FLOAT\s*\)', r'CAST(\1 AS DOUBLE PRECISION)', sql)

    # 15. CAST(... AS REAL) → CAST(... AS NUMERIC)
    #     Use NUMERIC instead of DOUBLE PRECISION so ROUND(x, n) works.
    #     PostgreSQL ROUND() requires numeric, not double precision.
    sql = re.sub(r'(?i)\bCAST\s*\((.+?)\s+AS\s+REAL\s*\)', r'CAST(\1 AS NUMERIC)', sql)

    # 16. LIMIT offset, count → LIMIT count OFFSET offset
    #     SQLite supports LIMIT offset, count; PostgreSQL requires LIMIT count OFFSET offset.
    sql = re.sub(
        r'(?i)\bLIMIT\s+(\d+)\s*,\s*(\d+)',
        r'LIMIT \2 OFFSET \1',
        sql,
    )

    # 17. SUM(boolean_expr) → SUM(CAST(boolean_expr AS INTEGER))
    #     PostgreSQL does not support SUM on booleans directly.
    #     This is hard to fix generically — instead we note that IIF has been
    #     converted to CASE WHEN which returns integers, so SUM(CASE...) works.

    return sql


# %%
# HuggingFace dataset download (optional — fails gracefully)
def _tryHuggingFace(destDir: str) -> bool:
    """
    Attempt to download the BIRD dataset from HuggingFace Hub.

    The ``datasets`` package is an optional dependency; if it is absent
    this function returns False immediately without raising.

    Arguments:
    ----------
    destDir : str
        Local directory where downloaded JSON files should be placed.

    Returns:
    --------
    bool
        True if download and file placement succeeded, False otherwise.
    """
    try:
        import datasets as hf_datasets  # optional dependency
    except ImportError:
        logger.warning("'datasets' package not installed — skipping HuggingFace download")
        return False

    try:
        logger.info("Attempting HuggingFace download: %s", BIRD_HF_DATASET)
        split = "train" if BIRD_DATASET == "mini" else "validation"
        ds = hf_datasets.load_dataset(BIRD_HF_DATASET, split=split, trust_remote_code=True)

        os.makedirs(destDir, exist_ok=True)
        outPath = os.path.join(destDir, "questions.json")

        # Convert dataset rows to the raw BIRD JSON format
        rows = []
        for row in ds:
            rows.append({
                "question": row.get("question", ""),
                "evidence": row.get("evidence", ""),
                "SQL": row.get("SQL", row.get("query", "")),
                "db_id": row.get("db_id", ""),
                "difficulty": row.get("difficulty", "simple"),
            })

        with open(outPath, "w") as f:
            json.dump(rows, f, indent=2)

        logger.info("HuggingFace download complete: %d questions → %s", len(rows), outPath)
        return True

    except Exception as e:
        logger.warning("HuggingFace download failed: %s", str(e)[:120])
        return False


# %%
# Locate the BIRD questions JSON file in the data directory
def _findQuestionsFile(dataDir: str) -> str | None:
    """
    Locate the BIRD questions JSON file in the data directory.

    Searches candidate filenames in priority order across several
    subdirectories so the pipeline works regardless of whether the
    user unpacked mini_dev or full dev, or used HuggingFace download.

    Arguments:
    ----------
    dataDir : str
        Root of the bird_data volume mount.

    Returns:
    --------
    str or None
        Absolute path to the questions file, or None if not found.
    """
    candidates = ["mini_dev_sqlite.json", "dev.json", "questions.json"]
    subdirs = ["", "mini_dev", "dev", "data"]

    for subdir in subdirs:
        base = os.path.join(dataDir, subdir) if subdir else dataDir
        for name in candidates:
            path = os.path.join(base, name)
            if os.path.isfile(path):
                logger.info("Found questions file: %s", path)
                return path

    return None


# %%
# Main data loading function
def loadQuestions(dataDir: str) -> list:
    """
    Load and normalize BIRD questions into the internal pipeline format.

    Attempts HuggingFace download first; falls back to manual file
    placement in ``dataDir``. On success, returns a list of internal
    question records with ``gold_result`` and ``gold_conversion_error``
    as placeholders to be populated by ``sqliteToPostgres.executeGoldSQL``.

    Arguments:
    ----------
    dataDir : str
        Path to the bird_data volume mount.

    Returns:
    --------
    list[dict]
        Normalized question records. Each record includes:
        id, database, db_id, difficulty, question (engine prompt),
        evidence, gold_query, gold_query_pg, gold_result (None),
        gold_conversion_error (False).
    """
    # Try HuggingFace download if no file is present yet
    if _findQuestionsFile(dataDir) is None:
        logger.info("No questions file found — attempting HuggingFace download")
        _tryHuggingFace(dataDir)

    questionsPath = _findQuestionsFile(dataDir)
    if questionsPath is None:
        raise FileNotFoundError(
            f"BIRD questions file not found in {dataDir!r}. "
            "Place mini_dev_sqlite.json (or dev.json) under BIRD_DATA_DIR, "
            "or install the 'datasets' package for automatic download."
        )

    with open(questionsPath) as f:
        raw = json.load(f)

    # Handle both a top-level list and an object with a 'data' key
    if isinstance(raw, dict):
        raw = raw.get("data", raw.get("questions", list(raw.values())[0]))

    questions = []
    for idx, item in enumerate(raw, start=1):
        dbId    = item.get("db_id", "")
        pgName  = f"bird_{dbId}"
        goldSQL = item.get("SQL", item.get("query", ""))
        pgSQL   = _convertGoldSQL(goldSQL)
        prompt  = _buildPrompt(
            item.get("question", ""),
            item.get("evidence", ""),
        )

        questions.append({
            "id":                   idx,
            "database":             pgName,
            "db_id":                dbId,
            "difficulty":           item.get("difficulty", "simple"),
            "question":             prompt,
            "evidence":             item.get("evidence", ""),
            "gold_query":           goldSQL,
            "gold_query_pg":        pgSQL,
            "gold_result":          None,           # populated by executeGoldSQL
            "gold_conversion_error": False,
        })

    dbIds = {q["db_id"] for q in questions}
    logger.info(
        "Loaded %d questions across %d databases from %s",
        len(questions), len(dbIds), questionsPath,
    )

    # Log difficulty distribution
    from collections import Counter
    diffs = Counter(q["difficulty"] for q in questions)
    for diff, count in sorted(diffs.items()):
        logger.info("  %-12s %d questions", diff, count)

    return questions


# %%
# Execution — data preparation entry point called by birdEntrypoint.py
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    from sqliteToPostgres import migrateAll, executeGoldSQL

    questions = loadQuestions(BIRD_DATA_DIR)

    dbIds = list({q["db_id"] for q in questions})
    logger.info("Migrating %d SQLite databases to PostgreSQL...", len(dbIds))
    migrationResults = migrateAll(dbIds, BIRD_DATA_DIR)

    failedDBs = [k for k, v in migrationResults.items() if not v]
    if failedDBs:
        logger.warning("Migration failed for %d database(s): %s", len(failedDBs), failedDBs)

    logger.info("Executing gold SQL queries against PostgreSQL...")
    questions = executeGoldSQL(questions)

    errorCount = sum(1 for q in questions if q["gold_conversion_error"])
    logger.info(
        "Gold SQL: %d OK, %d conversion errors (%.1f%%)",
        len(questions) - errorCount,
        errorCount,
        errorCount / len(questions) * 100 if questions else 0,
    )

    import os as _os
    _os.makedirs(RESULTS_BASE_DIR, exist_ok=True)
    outPath = _os.path.join(RESULTS_BASE_DIR, "questions.json")
    with open(outPath, "w") as f:
        json.dump(questions, f, indent=2, default=str)
    logger.info("Questions saved to %s", outPath)
