#!/usr/bin/env python3

# %%
# Question Bank Package
# Each module exports a list of question dicts with keys:
#   difficulty, question, gold_query
import os
from .ecommerce import ECOMMERCE_QUESTIONS
from .university import UNIVERSITY_QUESTIONS
from .hospital import HOSPITAL_QUESTIONS


# %%
# Subset selector — takes the first N questions per difficulty tier
# to produce a balanced subset of a given size per database.
def _subset(questions: list, total: int) -> list:
    """Select a balanced subset of questions across difficulty tiers."""
    by_diff = {}
    for q in questions:
        by_diff.setdefault(q["difficulty"], []).append(q)

    tiers = list(by_diff.keys())
    n_tiers = len(tiers)
    per_tier = total // n_tiers
    remainder = total % n_tiers

    result = []
    for i, tier in enumerate(tiers):
        take = per_tier + (1 if i < remainder else 0)
        result.extend(by_diff[tier][:take])
    return result


# %%
# Number of questions per database (default 40 = all, set via env var)
QUESTIONS_PER_DB = int(os.environ.get("QUESTIONS_PER_DB", "40"))

# %%
# Registry mapping database name → question list
QUESTION_BANK = {
    "eval_ecommerce": _subset(ECOMMERCE_QUESTIONS, QUESTIONS_PER_DB) if QUESTIONS_PER_DB < 40 else ECOMMERCE_QUESTIONS,
    "eval_university": _subset(UNIVERSITY_QUESTIONS, QUESTIONS_PER_DB) if QUESTIONS_PER_DB < 40 else UNIVERSITY_QUESTIONS,
    "eval_hospital": _subset(HOSPITAL_QUESTIONS, QUESTIONS_PER_DB) if QUESTIONS_PER_DB < 40 else HOSPITAL_QUESTIONS,
}
