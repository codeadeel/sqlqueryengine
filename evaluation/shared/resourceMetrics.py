#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import logging
import os
import resource
import time

logger = logging.getLogger(__name__)


# %%
# Peak memory tracking via the OS resource module
def getPeakMemoryMB() -> float:
    """
    Return the peak resident set size (RSS) of the current process in MB.

    Uses ``resource.getrusage`` which tracks the high-water mark of
    physical memory used by this process since it started.

    Returns:
    --------
    float
        Peak RSS in megabytes, rounded to 1 decimal place.
    """
    # ru_maxrss is in KB on Linux, bytes on macOS
    rusage = resource.getrusage(resource.RUSAGE_SELF)
    if os.uname().sysname == "Darwin":
        return round(rusage.ru_maxrss / (1024 * 1024), 1)
    return round(rusage.ru_maxrss / 1024, 1)


# %%
# Latency percentile calculations
def latencyPercentiles(latencies: list) -> dict:
    """
    Compute latency percentiles from a list of per-question latencies.

    Arguments:
    ----------
    latencies : list[float]
        Latency values in seconds.

    Returns:
    --------
    dict
        Keys: min, max, mean, median, p90, p95, p99 — all rounded to 2 decimals.
    """
    if not latencies:
        return {"min": 0, "max": 0, "mean": 0, "median": 0, "p90": 0, "p95": 0, "p99": 0}

    s = sorted(latencies)
    n = len(s)

    def _percentile(p):
        idx = int(p / 100 * (n - 1))
        return round(s[idx], 2)

    return {
        "min":    round(s[0], 2),
        "max":    round(s[-1], 2),
        "mean":   round(sum(s) / n, 2),
        "median": _percentile(50),
        "p90":    _percentile(90),
        "p95":    _percentile(95),
        "p99":    _percentile(99),
    }


# %%
# Throughput calculation
def throughput(questionCount: int, wallTimeSeconds: float) -> float:
    """
    Calculate throughput in questions per minute.

    Arguments:
    ----------
    questionCount : int
        Number of questions evaluated.
    wallTimeSeconds : float
        Total wall-clock time in seconds.

    Returns:
    --------
    float
        Questions per minute, rounded to 1 decimal.
    """
    if wallTimeSeconds <= 0:
        return 0.0
    return round(questionCount * 60 / wallTimeSeconds, 1)


# %%
# Response size estimation
def estimateTokens(text: str) -> int:
    """
    Rough token count estimate for a text string.

    Uses the ~4 characters per token heuristic which is reasonable
    for English text and SQL queries across most tokenizers.

    Arguments:
    ----------
    text : str
        Input text to estimate tokens for.

    Returns:
    --------
    int
        Estimated token count.
    """
    if not text:
        return 0
    return max(1, len(text) // 4)


# %%
# Wall-clock timer context manager
class WallTimer:
    """
    Simple wall-clock timer for measuring elapsed time of code blocks.

    Usage::

        timer = WallTimer()
        timer.start()
        # ... do work ...
        timer.stop()
        print(timer.elapsed)  # seconds as float
    """

    def __init__(self):
        self._start = None
        self._end = None

    def start(self):
        """Record the start time."""
        self._start = time.time()

    def stop(self):
        """Record the end time."""
        self._end = time.time()

    @property
    def elapsed(self) -> float:
        """Elapsed time in seconds, rounded to 2 decimals."""
        if self._start is None:
            return 0.0
        end = self._end if self._end is not None else time.time()
        return round(end - self._start, 2)
