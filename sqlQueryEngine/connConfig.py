#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import os
from typing import Optional
from fastapi import Query

# %%
# Loading Environment Configuration
# Numeric env vars are read as None when absent so that 0 and 0.0 are not
# misidentified as "not configured" — they are valid values.
_llmTemperatureStr = os.environ.get("LLM_TEMPERATURE")
_postgrePortStr    = os.environ.get("POSTGRES_PORT")
_redisPortStr      = os.environ.get("REDIS_PORT")
_redisDBStr        = os.environ.get("REDIS_DB")

LLM_PARAMS = {
    "model":       os.environ.get("LLM_MODEL", ""),
    "temperature": float(_llmTemperatureStr) if _llmTemperatureStr is not None else None,
    "base_url":    os.environ.get("LLM_BASE_URL", ""),
    "api_key":     os.environ.get("LLM_API_KEY", "")
}

DB_PARAMS = {
    "host":     os.environ.get("POSTGRES_HOST", ""),
    "port":     int(_postgrePortStr) if _postgrePortStr is not None else None,
    "dbname":   os.environ.get("POSTGRES_DB", ""),
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD")   # None = not configured; "" = no-auth
}

REDIS_PARAMS = {
    "host":             os.environ.get("REDIS_HOST", ""),
    "port":             int(_redisPortStr) if _redisPortStr is not None else None,
    "password":         os.environ.get("REDIS_PASSWORD"),   # None = not configured; "" = no-auth
    "db":               int(_redisDBStr) if _redisDBStr is not None else None,
    "decode_responses": True
}

BOT_NAME         = os.environ.get("BOT_NAME", "SQLBot")
SPLIT_IDENTIFIER = os.environ.get("SPLIT_IDENTIFIER", "<|-/|-/>")

# %%
# Shared Connection Dependency
# All 13 LLM / PostgreSQL / Redis Query parameters in one reusable function.
# FastAPI resolves each parameter individually, so they appear as separate
# labelled inputs in the Swagger UI Parameters section for every route.
def connectionDependency(
    # LLM
    llmBaseURL: Optional[str] = Query(
        LLM_PARAMS["base_url"] if LLM_PARAMS["base_url"] else ...,
        description="OpenAI-compatible API base URL."
    ),
    llmAPIKey: Optional[str] = Query(
        LLM_PARAMS["api_key"] if LLM_PARAMS["api_key"] else ...,
        description="API key for the LLM endpoint."
    ),
    llmModel: Optional[str] = Query(
        LLM_PARAMS["model"] if LLM_PARAMS["model"] else ...,
        description="Model identifier for generation and evaluation."
    ),
    llmTemperature: Optional[float] = Query(
        LLM_PARAMS["temperature"] if LLM_PARAMS["temperature"] is not None else ...,
        description="Sampling temperature (0.0 = deterministic)."
    ),
    # PostgreSQL
    postgreHost: Optional[str] = Query(
        DB_PARAMS["host"] if DB_PARAMS["host"] else ...,
        description="PostgreSQL server hostname or IP."
    ),
    postgrePort: Optional[int] = Query(
        DB_PARAMS["port"] if DB_PARAMS["port"] is not None else ...,
        description="PostgreSQL server port."
    ),
    postgreDBName: Optional[str] = Query(
        DB_PARAMS["dbname"] if DB_PARAMS["dbname"] else ...,
        description="Database name."
    ),
    postgreUser: Optional[str] = Query(
        DB_PARAMS["user"] if DB_PARAMS["user"] else ...,
        description="Database user name."
    ),
    postgrePassword: Optional[str] = Query(
        DB_PARAMS["password"] if DB_PARAMS["password"] is not None else ...,
        description="Database password."
    ),
    # Redis
    redisHost: Optional[str] = Query(
        REDIS_PARAMS["host"] if REDIS_PARAMS["host"] else ...,
        description="Redis server hostname or IP."
    ),
    redisPort: Optional[int] = Query(
        REDIS_PARAMS["port"] if REDIS_PARAMS["port"] is not None else ...,
        description="Redis server port."
    ),
    redisPassword: Optional[str] = Query(
        REDIS_PARAMS["password"] if REDIS_PARAMS["password"] is not None else ...,
        description="Redis authentication password."
    ),
    redisDB: Optional[int] = Query(
        REDIS_PARAMS["db"] if REDIS_PARAMS["db"] is not None else ...,
        description="Redis logical database number (0–15)."
    )
) -> dict:
    return {
        "llm": {
            "model":       llmModel,
            "temperature": llmTemperature,
            "base_url":    llmBaseURL,
            "api_key":     llmAPIKey
        },
        "db": {
            "host":     postgreHost,
            "port":     postgrePort,
            "dbname":   postgreDBName,
            "user":     postgreUser,
            "password": postgrePassword
        },
        "redis": {
            "host":             redisHost,
            "port":             redisPort,
            "password":         redisPassword,
            "db":               redisDB,
            "decode_responses": True
        }
    }
