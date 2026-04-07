#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import os
import psycopg

# %%
# Environment-driven configuration for the BIRD evaluation pipeline.
# All values fall back to sensible defaults for local development;
# the docker-compose-bird-evaluation.yml compose file overrides them
# for container use.

POSTGRES_HOST     = os.environ.get("POSTGRES_HOST", "birdpostgres")
POSTGRES_PORT     = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER     = os.environ.get("POSTGRES_USER", "birduser")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "birdpass")

ENGINE_URL        = os.environ.get("ENGINE_URL", "http://birdengine:8080")
ENGINE_PG_HOST    = os.environ.get("ENGINE_PG_HOST", "birdpostgres")
ENGINE_PG_PORT    = int(os.environ.get("ENGINE_PG_PORT", "5432"))

REDIS_HOST        = os.environ.get("REDIS_HOST", "birdredis")
REDIS_PORT        = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD    = os.environ.get("REDIS_PASSWORD", "birdPass")

RESULTS_BASE_DIR  = os.environ.get("RESULTS_DIR", "bird_results")
BIRD_DATA_DIR     = os.environ.get("BIRD_DATA_DIR", "bird_data")
TIMEOUT_SECONDS   = int(os.environ.get("TIMEOUT_SECONDS", "180"))

LLM_MODEL         = os.environ.get("LLM_MODEL", "unknown")

# Derive a filesystem-safe model slug for the results subfolder.
# e.g. "meta-llama/llama-4-scout-17b-16e-instruct" → "llama-4-scout-17b-16e-instruct"
_MODEL_SLUG       = LLM_MODEL.rsplit("/", 1)[-1] if "/" in LLM_MODEL else LLM_MODEL
RESULTS_DIR       = os.path.join(RESULTS_BASE_DIR, "runs", _MODEL_SLUG)
LLM_TEMPERATURE   = float(os.environ.get("LLM_TEMPERATURE", "0.1"))

# Whether to prepend BIRD "evidence" hints to the engine prompt.
# Set to "true" for the upper-bound (evidence-assisted) evaluation.
BIRD_USE_EVIDENCE = os.environ.get("BIRD_USE_EVIDENCE", "true").lower() == "true"

# Dataset variant: "mini" (~500 questions, 11 databases) or "full" (1534 questions, 95 databases)
BIRD_DATASET      = os.environ.get("BIRD_DATASET", "mini")

# HuggingFace dataset identifier for automatic download
BIRD_HF_DATASET   = os.environ.get("BIRD_HF_DATASET", "birdsql/bird")


# %%
# Connection helpers
def adminConnect():
    """
    Connect to the default ``postgres`` database for admin operations
    such as CREATE DATABASE.

    Returns:
    --------
    psycopg.Connection
        An autocommit connection to the ``postgres`` database.
    """
    return psycopg.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        dbname="postgres", autocommit=True,
    )


def dbConnect(dbname: str):
    """
    Connect to a specific BIRD PostgreSQL database.

    Arguments:
    ----------
    dbname : str
        Target database name (e.g. ``bird_concert_singer``).

    Returns:
    --------
    psycopg.Connection
        An autocommit connection to the requested database.
    """
    return psycopg.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        dbname=dbname, autocommit=True,
    )
