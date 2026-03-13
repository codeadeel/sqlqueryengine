#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import logging
from typing import Dict, Any, Optional
from fastapi import Depends, FastAPI, Path, Query, Request
from pydantic import BaseModel, Field
from .engine import SQLQueryEngine
from .connConfig import (
    LLM_PARAMS,
    DB_PARAMS,
    REDIS_PARAMS,
    BOT_NAME,
    SPLIT_IDENTIFIER,
    connectionDependency,
)
from .openaiCompat import router as openaiCompatRouter, OPENAI_API_KEYS, COMPLETIONS_MODEL_NAME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# %%
# Defining FastAPI Application
tagsMetadata = [
    {
        "name": "SQL Inference",
        "description": (
            "Run the full two-stage NL-to-SQL pipeline in a single request.\n\n"
            "1. **SQL Generation** — the LLM introspects the database schema (cached in Redis per `chatID`) "
            "and produces an initial SQL query.\n"
            "2. **SQL Evaluation** — the query is executed against PostgreSQL; "
            "on failure the LLM iteratively repairs it up to `retryCount` times, "
            "publishing progress to a Redis Pub/Sub channel keyed by `chatID`."
        )
    },
    {
        "name": "SQL Generation",
        "description": (
            "Run Stage 1 only — translate a natural language prompt into a SQL query.\n\n"
            "Introspects the database schema (cached in Redis per `chatID`) and returns "
            "the generated SQL query without executing it."
        )
    },
    {
        "name": "SQL Evaluation",
        "description": (
            "Run Stage 2 only — execute a provided SQL query against PostgreSQL and repair it if needed.\n\n"
            "Accepts an existing SQL query, executes it, and if it fails or returns empty results "
            "the LLM iteratively repairs it up to `retryCount` times, publishing progress to "
            "a Redis Pub/Sub channel keyed by `chatID`."
        )
    },
    {
        "name": "Ping",
        "description": "Health check — confirms the service is running and reachable."
    }
]

app = FastAPI(
    title="SQL Query Engine",
    description=(
        "End-to-end **natural language → SQL** inference service.\n\n"
        "### How it works\n"
        "1. Send a natural language `basePrompt` to the inference endpoint together with a `chatID`.\n"
        "2. On the first call for a `chatID` the engine introspects the connected PostgreSQL database, "
        "generates a detailed schema description via the LLM, and caches it in Redis.\n"
        "3. An initial SQL query is generated from the prompt and the cached schema context.\n"
        "4. The query is executed. If it fails or returns empty results the LLM repairs it "
        "up to `retryCount` times.\n"
        "5. Every repair attempt publishes structured progress messages to the Redis Pub/Sub "
        "channel `{chatID}` so clients can stream real-time feedback.\n\n"
        "### Connection parameters\n"
        "LLM, PostgreSQL, and Redis connection parameters are passed as **query parameters** "
        "so they are visible as individual inputs in the Swagger UI. "
        "Parameters are **optional** when the corresponding environment variable is configured "
        "and **required** when it is not.\n\n"
        "### Pub/Sub message format\n"
        "```\n"
        "</{component}:{event}>{SPLIT_IDENTIFIER}{content}\n"
        "```\n"
        "Example: `</SQLQueryEvaluator:QueryFixAttempt#1><|-/|-/>LLM Judgement : False`"
    ),
    version="1.0.0",
    openapi_tags=tagsMetadata
)

agentNamespace = "sqlQueryEngine"

# Include OpenAI-compatible routes
app.include_router(openaiCompatRouter)

if OPENAI_API_KEYS:
    logger.info(f"[ SQL Query Engine | Auth ]: API key authentication enabled ({len(OPENAI_API_KEYS)} key(s) configured)")
else:
    logger.info("[ SQL Query Engine | Auth ]: API key authentication disabled (set OPENAI_API_KEY to enable)")

logger.info(f"[ SQL Query Engine | OpenAI Compat ]: Model name '{COMPLETIONS_MODEL_NAME}' exposed at /v1/models")

# %%
# Request Models — pipeline parameters only
# Connection parameters (LLM / PostgreSQL / Redis) are query parameters on the route
# so they render as individual inputs in the Swagger UI Parameters section.
class queryEngineRequest(BaseModel):
    """
    Request body for the SQL Query Engine inference endpoint.

    Contains only pipeline control parameters. Connection parameters (LLM, PostgreSQL,
    Redis) are declared as query parameters on the route so they appear as individual
    inputs in the Swagger UI alongside `chatID`, rather than buried inside the JSON body.
    """

    basePrompt: str = Field(
        ...,
        title="Base Prompt",
        description="Natural language question or instruction to translate into SQL."
    )
    retryCount: Optional[int] = Field(
        5,
        title="Retry Count",
        description="Maximum number of LLM-driven repair attempts before returning a failure response."
    )
    schemaDescriptionKey: Optional[str] = Field(
        "dbSchemaDescription",
        title="Schema Description Key",
        description="Redis hash field key used to cache the generated schema description. Change this to force a fresh schema generation for the same `chatID`."
    )
    schemaExamples: Optional[int] = Field(
        5,
        title="Schema Examples",
        description="Number of sample rows per table to include in the schema context sent to the LLM."
    )
    feedbackExamples: Optional[int] = Field(
        3,
        title="Feedback Examples",
        description="Number of result rows fed back to the LLM evaluator to help it judge query correctness."
    )
    extraPayload: Optional[Dict[Any, Any]] = Field(
        None,
        title="Extra Payload",
        description="Any additional JSON data to pass through and receive back in the response unchanged."
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "basePrompt": "How many orders were placed in the last 30 days?",
                "retryCount": 5,
                "schemaExamples": 5,
                "feedbackExamples": 3,
                "schemaDescriptionKey": "dbSchemaDescription",
                "extraPayload": None
            }
        }
    }

class queryGenerationRequest(BaseModel):
    """
    Request body for the SQL Generation endpoint.

    Contains only pipeline control parameters for Stage 1 (NL → SQL).
    Connection parameters (LLM, PostgreSQL, Redis) are declared as query parameters
    on the route so they appear as individual inputs in the Swagger UI.
    """

    basePrompt: str = Field(
        ...,
        title="Base Prompt",
        description="Natural language question or instruction to translate into SQL."
    )
    schemaDescriptionKey: Optional[str] = Field(
        "dbSchemaDescription",
        title="Schema Description Key",
        description="Redis hash field key used to cache the generated schema description. Change this to force a fresh schema generation for the same `chatID`."
    )
    schemaExamples: Optional[int] = Field(
        5,
        title="Schema Examples",
        description="Number of sample rows per table to include in the schema context sent to the LLM."
    )
    extraPayload: Optional[Dict[Any, Any]] = Field(
        None,
        title="Extra Payload",
        description="Any additional JSON data to pass through and receive back in the response unchanged."
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "basePrompt": "How many orders were placed in the last 30 days?",
                "schemaExamples": 5,
                "schemaDescriptionKey": "dbSchemaDescription",
                "extraPayload": None
            }
        }
    }

class queryEvaluationRequest(BaseModel):
    """
    Request body for the SQL Evaluation endpoint.

    Contains only pipeline control parameters for Stage 2 (execute + repair loop).
    Connection parameters (LLM, PostgreSQL, Redis) are declared as query parameters
    on the route so they appear as individual inputs in the Swagger UI.
    """

    basePrompt: str = Field(
        ...,
        title="Base Prompt",
        description="Original natural language question — used as context for the LLM repair loop."
    )
    baseQuery: str = Field(
        ...,
        title="Base Query",
        description="SQL query to execute and evaluate."
    )
    baseDescription: str = Field(
        ...,
        title="Base Description",
        description="Human-readable description of what the query is intended to do."
    )
    retryCount: Optional[int] = Field(
        5,
        title="Retry Count",
        description="Maximum number of LLM-driven repair attempts before returning a failure response."
    )
    schemaDescriptionKey: Optional[str] = Field(
        "dbSchemaDescription",
        title="Schema Description Key",
        description="Redis hash field key used to read the cached schema description."
    )
    schemaExamples: Optional[int] = Field(
        5,
        title="Schema Examples",
        description="Number of sample rows per table to include in the schema context sent to the LLM."
    )
    feedbackExamples: Optional[int] = Field(
        3,
        title="Feedback Examples",
        description="Number of result rows fed back to the LLM evaluator to help it judge query correctness."
    )
    extraPayload: Optional[Dict[Any, Any]] = Field(
        None,
        title="Extra Payload",
        description="Any additional JSON data to pass through and receive back in the response unchanged."
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "basePrompt": "How many orders were placed in the last 30 days?",
                "baseQuery": "SELECT COUNT(*) FROM orders WHERE created_at >= NOW() - INTERVAL '30 days'",
                "baseDescription": "Count orders placed in the last 30 days.",
                "retryCount": 5,
                "schemaExamples": 5,
                "feedbackExamples": 3,
                "schemaDescriptionKey": "dbSchemaDescription",
                "extraPayload": None
            }
        }
    }

# %%
# Inference Endpoint
@app.post(
    "/inference/sqlQueryEngine/{chatID}",
    tags=["SQL Inference"],
    summary="Run NL-to-SQL inference",
    response_description="Generated SQL query, evaluation result, and optional extra payload."
)
async def sql_query_engine_inference(
    params: queryEngineRequest,
    chatID: str = Path(
        ...,
        description="Chat ID used to namespace Redis context. Reuse the same value across calls to share cached schema context."
    ),
    conn: dict = Depends(connectionDependency)
) -> dict:
    """
    Translate a natural language prompt into a validated, executed SQL query.

    ### Pipeline

    1. **SQL Generation** — the LLM produces an initial SQL query from the prompt
       and the schema context cached in Redis for this `chatID`.
       On the first call the schema is introspected from PostgreSQL and cached automatically.

    2. **SQL Evaluation** — the query is executed against PostgreSQL.
       If it fails or returns empty results, the LLM repairs it and retries up to `retryCount` times.

    ### Real-time feedback via Redis Pub/Sub

    Subscribe to the Redis channel `{chatID}` before sending the request to receive
    structured progress messages during evaluation. Each message has the format:

    ```
    </{component}:{event}>{SPLIT_IDENTIFIER}{content}
    ```

    **Before each LLM call** (execution visibility):
    - `Current Query` — the SQL being evaluated this attempt
    - `Current Observation` — context carried into this attempt
    - `Execution Errors` — PostgreSQL error output, or `No errors encountered.`

    **After each LLM call** (repair visibility):
    - `LLM Judgement` — `True` / `False`
    - `LLM Observation` — what the LLM found wrong
    - `Fixed Query` — the repaired SQL
    - `Modified Prompt` — adjusted user prompt

    ### Schema context caching

    Schema context is stored in Redis under `{chatID}:SQLQueryEngine`.
    Subsequent calls for the same `chatID` reuse the cached context, skipping introspection.
    To force regeneration, change `schemaDescriptionKey` or clear the Redis key.
    """
    effectiveLLM   = conn["llm"]
    effectiveDB    = conn["db"]
    effectiveRedis = conn["redis"]

    logger.info(f"[ {chatID} | LLM Model ] : {effectiveLLM['model']}")
    logger.info(f"[ {chatID} | LLM Base URL ] : {effectiveLLM['base_url']}")
    logger.info(f"[ {chatID} | PostgreSQL Host ] : {effectiveDB['host']} | DB : {effectiveDB['dbname']}")
    logger.info(f"[ {chatID} | Redis Host ] : {effectiveRedis['host']}:{effectiveRedis['port']}")

    engine = SQLQueryEngine(effectiveLLM, effectiveDB, effectiveRedis, botName=BOT_NAME, splitIdentifier=SPLIT_IDENTIFIER)
    result = engine.run(
        chatID=chatID,
        basePrompt=params.basePrompt,
        retryCount=params.retryCount,
        schemaExamples=params.schemaExamples,
        feedbackExamples=params.feedbackExamples,
        schemaDescriptionKey=params.schemaDescriptionKey
    )

    if result["code"] != 200:
        logger.error(f"[ {chatID} | SQL Query Engine ] : {result.get('error', 'Unknown error')}")
        return {
            "code": 500,
            "status": f"[ SQL Query Engine ]: {result.get('error', 'Inference failed.')}",
            "error": result.get("error", "")
        }

    logger.info(f"[ {chatID} | SQL Query Engine ] : Inference completed successfully")
    return {
        "code": 200,
        "status": f"[ {chatID} | SQL Query Engine ]: Inference executed successfully.",
        "chatID": chatID,
        "agentResponse": {
            "generation": result["generation"],
            "evaluation": result["evaluation"]
        },
        "extraPayload": params.extraPayload
    }

# %%
# SQL Generation Endpoint
@app.post(
    "/inference/sqlQueryGeneration/{chatID}",
    tags=["SQL Generation"],
    summary="Run SQL generation only (Stage 1)",
    response_description="Generated SQL query and its description."
)
async def sql_query_engine_generation(
    params: queryGenerationRequest,
    chatID: str = Path(
        ...,
        description="Chat ID used to namespace Redis context. Reuse the same value across calls to share cached schema context."
    ),
    conn: dict = Depends(connectionDependency)
) -> dict:
    """
    Translate a natural language prompt into a SQL query without executing it.

    ### What this does

    Introspects the database schema — or reads a cached description from Redis for the
    given `chatID` — then produces a structured response containing a natural language
    `queryDescription` and the `sqlQuery`.

    The generated query is **not executed** against PostgreSQL. Use the `/inference`
    endpoint to run the full pipeline, or pass the output to `/evaluation` to execute
    and repair it separately.

    ### Schema context caching

    Schema context is stored in Redis under `{chatID}:SQLQueryEngine`.
    Subsequent calls for the same `chatID` reuse the cached context, skipping introspection.
    To force regeneration, change `schemaDescriptionKey` or clear the Redis key.
    """
    effectiveLLM   = conn["llm"]
    effectiveDB    = conn["db"]
    effectiveRedis = conn["redis"]

    logger.info(f"[ {chatID} | LLM Model ] : {effectiveLLM['model']}")
    logger.info(f"[ {chatID} | LLM Base URL ] : {effectiveLLM['base_url']}")
    logger.info(f"[ {chatID} | PostgreSQL Host ] : {effectiveDB['host']} | DB : {effectiveDB['dbname']}")
    logger.info(f"[ {chatID} | Redis Host ] : {effectiveRedis['host']}:{effectiveRedis['port']}")

    engine = SQLQueryEngine(effectiveLLM, effectiveDB, effectiveRedis, botName=BOT_NAME, splitIdentifier=SPLIT_IDENTIFIER)
    result = engine.generate(
        chatID=chatID,
        basePrompt=params.basePrompt,
        schemaExamples=params.schemaExamples,
        schemaDescriptionKey=params.schemaDescriptionKey
    )

    if result["code"] != 200:
        logger.error(f"[ {chatID} | SQL Generation ] : {result.get('error', 'Unknown error')}")
        return {
            "code": 500,
            "status": f"[ SQL Generation ]: {result.get('error', 'Generation failed.')}",
            "error": result.get("error", "")
        }

    logger.info(f"[ {chatID} | SQL Generation ] : Generation completed successfully")
    return {
        "code": 200,
        "status": f"[ {chatID} | SQL Generation ]: Generation executed successfully.",
        "chatID": chatID,
        "agentResponse": {
            "generation": result["generation"]
        },
        "extraPayload": params.extraPayload
    }

# %%
# SQL Evaluation Endpoint
@app.post(
    "/inference/sqlQueryEvaluation/{chatID}",
    tags=["SQL Evaluation"],
    summary="Run SQL evaluation only (Stage 2)",
    response_description="Execution result of the provided SQL query after any LLM-driven repairs."
)
async def sql_query_engine_evaluation(
    params: queryEvaluationRequest,
    chatID: str = Path(
        ...,
        description="Chat ID used to namespace Redis context. Reuse the same value across calls to share cached schema context."
    ),
    conn: dict = Depends(connectionDependency)
) -> dict:
    """
    Execute a provided SQL query against PostgreSQL and iteratively repair it if needed.

    ### What this does

    Takes an existing SQL query (`baseQuery`) and runs it. If it fails or returns
    empty results the LLM repairs it and retries up to `retryCount` times, publishing
    structured progress events to the Redis Pub/Sub channel keyed by `chatID`.

    ### Real-time feedback via Redis Pub/Sub

    Subscribe to the Redis channel `{chatID}` before sending the request to receive
    structured progress messages. Each message has the format:

    ```
    </{component}:{event}>{SPLIT_IDENTIFIER}{content}
    ```

    **Before each LLM call** (execution visibility):
    - `Current Query` — the SQL being evaluated this attempt
    - `Current Observation` — context carried into this attempt
    - `Execution Errors` — PostgreSQL error output, or `No errors encountered.`

    **After each LLM call** (repair visibility):
    - `LLM Judgement` — `True` / `False`
    - `LLM Observation` — what the LLM found wrong
    - `Fixed Query` — the repaired SQL
    - `Modified Prompt` — adjusted user prompt
    """
    effectiveLLM   = conn["llm"]
    effectiveDB    = conn["db"]
    effectiveRedis = conn["redis"]

    logger.info(f"[ {chatID} | LLM Model ] : {effectiveLLM['model']}")
    logger.info(f"[ {chatID} | LLM Base URL ] : {effectiveLLM['base_url']}")
    logger.info(f"[ {chatID} | PostgreSQL Host ] : {effectiveDB['host']} | DB : {effectiveDB['dbname']}")
    logger.info(f"[ {chatID} | Redis Host ] : {effectiveRedis['host']}:{effectiveRedis['port']}")

    engine = SQLQueryEngine(effectiveLLM, effectiveDB, effectiveRedis, botName=BOT_NAME, splitIdentifier=SPLIT_IDENTIFIER)
    result = engine.evaluate(
        chatID=chatID,
        basePrompt=params.basePrompt,
        baseQuery=params.baseQuery,
        baseDescription=params.baseDescription,
        retryCount=params.retryCount,
        schemaExamples=params.schemaExamples,
        feedbackExamples=params.feedbackExamples,
        schemaDescriptionKey=params.schemaDescriptionKey
    )

    if result["code"] != 200:
        logger.error(f"[ {chatID} | SQL Evaluation ] : {result.get('error', 'Unknown error')}")
        return {
            "code": 500,
            "status": f"[ SQL Evaluation ]: {result.get('error', 'Evaluation failed.')}",
            "error": result.get("error", "")
        }

    logger.info(f"[ {chatID} | SQL Evaluation ] : Evaluation completed successfully")
    return {
        "code": 200,
        "status": f"[ {chatID} | SQL Evaluation ]: Evaluation executed successfully.",
        "chatID": chatID,
        "agentResponse": {
            "evaluation": result["evaluation"]
        },
        "extraPayload": params.extraPayload
    }


# %%
# Ping Endpoint
@app.get(
    "/ping",
    tags=["Ping"],
    summary="Health check",
    response_description="Service status and request metadata."
)
async def ping(request: Request) -> dict:
    """
    Confirm the SQL Query Engine is running and reachable.

    Returns basic request metadata (scheme, host, port, client address)
    useful for verifying connectivity through proxies or load balancers.
    """
    logger.info(f"[ SQL Query Engine ]: Ping received from {request.client.host}")
    try:
        currHost = request.headers.get("host", " : ").split(":")[0]
        currPort = request.headers.get("host", " : ").split(":")[1]
    except Exception:
        currHost = "localhost"
        currPort = ""
    return {
        "code": 200,
        "status": "[ SQL Query Engine ]: Agent is alive and reachable.",
        "agent": agentNamespace,
        "scheme": request.url.scheme,
        "host": currHost,
        "port": currPort,
        "clientHost": request.client.host,
        "clientPort": request.client.port
    }

# %%
# Execution
if __name__ == "__main__":
    print("[ SQL Query Engine ] : Use run.py to start the server.")
