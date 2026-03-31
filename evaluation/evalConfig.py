#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import os
import psycopg

# %%
# Environment-driven configuration for the evaluation pipeline.
# All values fall back to sensible defaults for local development;
# the evaluation.yml compose file overrides them for container use.

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "evalpostgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.environ.get("POSTGRES_USER", "evaluser")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "evalpass")

ENGINE_URL = os.environ.get("ENGINE_URL", "http://evalengine:8080")
ENGINE_PG_HOST = os.environ.get("ENGINE_PG_HOST", "evalpostgres")
ENGINE_PG_PORT = int(os.environ.get("ENGINE_PG_PORT", "5432"))

REDIS_HOST = os.environ.get("REDIS_HOST", "evalredis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "evalPass")

RESULTS_DIR = os.environ.get("RESULTS_DIR", "results")
QUESTIONS_PATH = os.environ.get("QUESTIONS_PATH", "results/questions.json")
TIMEOUT_SECONDS = int(os.environ.get("TIMEOUT_SECONDS", "180"))

LLM_MODEL = os.environ.get("LLM_MODEL", "unknown")
LLM_TEMPERATURE = float(os.environ.get("LLM_TEMPERATURE", "0.1"))

EVAL_DATABASES = ["eval_ecommerce", "eval_university", "eval_hospital"]


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
    Connect to a specific evaluation database.

    Arguments:
    ----------
    dbname : str
        Target database name (e.g. ``eval_ecommerce``).

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
