#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import logging

logger = logging.getLogger(__name__)


# %%
# Value and row normalization for gold-vs-predicted comparison.
# Handles the type mismatch between psycopg tuple rows (gold queries)
# and the engine's dict rows (evaluation results).

def normalizeValue(v) -> str:
    """
    Normalize a single cell value to a canonical string representation.

    Strips whitespace and collapses numeric trailing zeros so that
    ``100.0`` and ``100`` compare as equal.

    Arguments:
    ----------
    v : any
        A cell value from a query result row.

    Returns:
    --------
    str
        Canonical string form of the value.
    """
    if v is None:
        return "None"
    s = str(v).strip()
    try:
        f = float(s)
        s = str(f).rstrip("0").rstrip(".")
    except (ValueError, OverflowError):
        pass
    return s


def normalizeRows(rows) -> list:
    """
    Convert a result set to a sorted list of normalized string tuples.

    Accepts both ``list[tuple]`` (from raw psycopg) and ``list[dict]``
    (from the engine's ``queryExecutor``).

    Arguments:
    ----------
    rows : list
        Result rows in either tuple or dict form.

    Returns:
    --------
    list[tuple[str, ...]]
        Sorted list of normalized row tuples.
    """
    if rows is None:
        return None
    out = []
    for r in rows:
        if isinstance(r, dict):
            out.append(tuple(normalizeValue(v) for v in r.values()))
        elif isinstance(r, (list, tuple)):
            out.append(tuple(normalizeValue(v) for v in r))
        else:
            out.append((normalizeValue(r),))
    return sorted(out)


def resultsMatch(gold, predicted) -> bool:
    """
    Order-independent comparison of two result sets.

    Arguments:
    ----------
    gold : list or None
        Expected result rows.
    predicted : list or None
        Actual result rows from the engine or direct execution.

    Returns:
    --------
    bool
        True if the normalized, sorted row sets are identical.
    """
    if gold is None or predicted is None:
        return gold is None and predicted is None
    if len(gold) == 0 and len(predicted) == 0:
        return True
    if len(gold) == 0 or len(predicted) == 0:
        return False
    try:
        return normalizeRows(gold) == normalizeRows(predicted)
    except Exception:
        return False
