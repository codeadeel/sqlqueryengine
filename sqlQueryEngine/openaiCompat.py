#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import asyncio
import hashlib
import json
import logging
import os
import uuid
from datetime import datetime
from typing import AsyncGenerator, List, Optional

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

from .connConfig import (
    BOT_NAME,
    SPLIT_IDENTIFIER,
    LLM_PARAMS,
    DB_PARAMS,
    REDIS_PARAMS,
)
from .engine import SQLQueryEngine

logger = logging.getLogger(__name__)

# %%
# Environment Configuration for OpenAI-Compatible API

# Authentication: comma-separated list of valid Bearer tokens.
# If empty, authentication is disabled.
OPENAI_API_KEYS: List[str] = [
    k.strip()
    for k in os.environ.get("OPENAI_API_KEY", "").split(",")
    if k.strip()
]

# Model name exposed via /v1/models and echoed in responses.
COMPLETIONS_MODEL_NAME: str = os.environ.get(
    "COMPLETIONS_MODEL_NAME",
    os.environ.get("BOT_NAME", "SQLBot")
)

# Pipeline defaults — read from env vars; fall back to sensible values so the
# completions routes (which have no query parameters) always have a valid config.
_defaultRetryStr     = os.environ.get("DEFAULT_RETRY_COUNT")
_defaultSchemaStr    = os.environ.get("DEFAULT_SCHEMA_EXAMPLES")
_defaultFeedbackStr  = os.environ.get("DEFAULT_FEEDBACK_EXAMPLES")

DEFAULT_RETRY_COUNT      : int = int(_defaultRetryStr)    if _defaultRetryStr    is not None else 5
DEFAULT_SCHEMA_EXAMPLES  : int = int(_defaultSchemaStr)   if _defaultSchemaStr   is not None else 5
DEFAULT_FEEDBACK_EXAMPLES: int = int(_defaultFeedbackStr) if _defaultFeedbackStr is not None else 3

# %%
# FastAPI Router

router = APIRouter(
    tags=["OpenAI Compatible"],
)

# %%
# Authentication

_httpBearer = HTTPBearer(auto_error=False)

async def verifyApiKey(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_httpBearer)
) -> None:
    """
    FastAPI dependency: validates Bearer token against OPENAI_API_KEY.
    No-op if no keys are configured (auth disabled).
    """
    if not OPENAI_API_KEYS:
        return  # Auth disabled — allow all requests

    if credentials is None or credentials.credentials not in OPENAI_API_KEYS:
        raise HTTPException(
            status_code=401,
            detail={
                "error": {
                    "message": "Invalid API key. Provide a valid key in 'Authorization: Bearer <key>'.",
                    "type": "invalid_request_error",
                    "param": None,
                    "code": "invalid_api_key"
                }
            }
        )

# %%
# Pydantic Request Models

class ChatMessage(BaseModel):
    """A single message in a conversation."""
    role: str = Field(..., description="Message role: 'system', 'user', or 'assistant'.")
    content: str = Field(..., description="Message content.")

class ChatCompletionRequest(BaseModel):
    """OpenAI-compatible chat completion request body."""
    model: str = Field(..., description="Model identifier (ignored; uses the configured SQL engine).")
    messages: List[ChatMessage] = Field(..., description="Conversation history. The last 'user' message is used as the SQL prompt.")
    stream: Optional[bool] = Field(True, description="Stream the response as SSE chunks.")
    temperature: Optional[float] = Field(None, description="Ignored — temperature is set via env var.")
    max_tokens: Optional[int] = Field(None, description="Ignored — not applicable to the SQL engine.")
    chat_id: Optional[str] = Field(
        None,
        description=(
            "Chat session identifier injected by OpenWebUI. When present it is used "
            "directly as the Redis namespace key, giving each OpenWebUI chat its own "
            "schema context. Falls back to a hash of all messages if absent."
        ),
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "model": "SQLBot",
                "messages": [
                    {"role": "user", "content": "How many orders were placed last month?"}
                ],
                "stream": True
            }
        }
    }

class CompletionRequest(BaseModel):
    """OpenAI-compatible text completion request body."""
    model: str = Field(..., description="Model identifier (ignored; uses the configured SQL engine).")
    prompt: str = Field(..., description="Natural language question to translate into SQL.")
    stream: Optional[bool] = Field(True, description="Stream the response as SSE chunks.")
    temperature: Optional[float] = Field(None, description="Ignored.")
    max_tokens: Optional[int] = Field(None, description="Ignored.")

    model_config = {
        "json_schema_extra": {
            "example": {
                "model": "SQLBot",
                "prompt": "How many active users are there?",
                "stream": True
            }
        }
    }

# %%
# Helper Functions

def _stableChatID(messages: List["ChatMessage"]) -> str:
    """
    Derive a stable, consistent chat ID from the first user message.

    OpenWebUI sends the full conversation history on every request, so the
    first user message is always present and never changes across turns in the
    same chat. Hashing it produces a consistent Redis namespace key for the
    entire conversation without requiring an explicit ``chat_id`` field.

    Falls back to a random UUID if no user message is found (should not happen
    in normal usage).
    """
    for msg in messages:
        if msg.role == "user":
            return hashlib.md5(msg.content.encode()).hexdigest()[:16]
    return uuid.uuid4().hex[:16]


def _formatSSEChunk(content: str, model: str, finish_reason: Optional[str] = None) -> str:
    """Format a content delta as an OpenAI-compatible SSE chunk."""
    chunk = {
        "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
        "object": "chat.completion.chunk",
        "created": int(datetime.now().timestamp()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": {"content": content} if content else {},
                "finish_reason": finish_reason,
            }
        ],
    }
    return f"data: {json.dumps(chunk)}\n\n"

def _formatSSEChunkRole(model: str) -> str:
    """First SSE chunk establishing role=assistant."""
    chunk = {
        "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
        "object": "chat.completion.chunk",
        "created": int(datetime.now().timestamp()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "delta": {"role": "assistant", "content": ""},
                "finish_reason": None,
            }
        ],
    }
    return f"data: {json.dumps(chunk)}\n\n"

def _formatFinalResult(result: dict) -> str:
    """
    Convert the engine result dict into a human-readable markdown string
    suitable for display in OpenWebUI or any OpenAI-compatible chat client.
    """
    if result.get("code") != 200:
        return f"**Error:** {result.get('error', 'The SQL query engine encountered an unexpected failure.')}"

    gen = result.get("generation", {})
    evl = result.get("evaluation", {})
    lines: List[str] = []

    if gen.get("queryDescription"):
        lines.append(f"**Query plan:** {gen['queryDescription']}")

    if gen.get("sqlQuery"):
        lines.append(f"\n```sql\n{gen['sqlQuery']}\n```")

    results = evl.get("results", [])
    if results:
        lines.append(f"\n**Results** — {len(results)} row(s) returned:\n")
        first = results[0]
        if isinstance(first, dict):
            headers = list(first.keys())
            lines.append("| " + " | ".join(str(h) for h in headers) + " |")
            lines.append("| " + " | ".join("---" for _ in headers) + " |")
            for row in results:
                lines.append("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |")
        else:
            lines.append(str(results))
    elif evl.get("currentQuery") is None:
        lines.append("\n**No results returned.** The engine exhausted all retry attempts without producing valid output.")
    else:
        lines.append("\n**Query executed successfully but returned no rows.**")

    return "\n".join(lines)

def _validateEnvConnParams() -> None:
    """
    Raise HTTPException 500 if any required connection env var is missing.

    Called at the start of each completions request so the error is surfaced
    immediately with a clear message rather than failing deep inside the engine.
    """
    missing: List[str] = []
    if not LLM_PARAMS.get("base_url"):
        missing.append("LLM_BASE_URL")
    if not LLM_PARAMS.get("model"):
        missing.append("LLM_MODEL")
    if LLM_PARAMS.get("temperature") is None:
        missing.append("LLM_TEMPERATURE")
    if not DB_PARAMS.get("host"):
        missing.append("POSTGRES_HOST")
    if DB_PARAMS.get("port") is None:
        missing.append("POSTGRES_PORT")
    if not DB_PARAMS.get("dbname"):
        missing.append("POSTGRES_DB")
    if not DB_PARAMS.get("user"):
        missing.append("POSTGRES_USER")
    if DB_PARAMS.get("password") is None:
        missing.append("POSTGRES_PASSWORD")
    if not REDIS_PARAMS.get("host"):
        missing.append("REDIS_HOST")
    if REDIS_PARAMS.get("port") is None:
        missing.append("REDIS_PORT")
    if REDIS_PARAMS.get("db") is None:
        missing.append("REDIS_DB")
    if missing:
        raise HTTPException(
            status_code=500,
            detail={
                "error": {
                    "message": (
                        f"Required environment variables not configured: {', '.join(missing)}. "
                        "Set them in docker-compose.yml or the host environment."
                    ),
                    "type": "server_error",
                    "code": "missing_configuration",
                }
            },
        )

# %%
# Core Streaming Generator

async def _streamSQLQueryEngine(
    chatID: str,
    prompt: str,
    llmParams: dict,
    dbParams: dict,
    redisParams: dict,
    botName: str,
    splitIdentifier: str,
    retryCount: int,
    schemaExamples: int,
    feedbackExamples: int,
    model: str,
) -> AsyncGenerator[str, None]:
    """
    Async generator that drives the SQL query engine and streams progress.

    Order of operations:
    1. Subscribe to the Redis pub/sub channel BEFORE starting the engine so no
       early messages are missed.
    2. Launch engine.run() in a thread-pool executor (non-blocking).
    3. Stream pub/sub messages as <think> content while the engine processes.
       Internal engine tags (e.g. </SQLQueryGenerator:schemaDescriptionChat>)
       are stripped — only the content after the split identifier is yielded.
    4. Drain residual messages after the engine completes.
    5. Close the <think> block and yield the final formatted result.

    Each piece of content yielded via SSE is also published to the Redis
    pub/sub channel ``{chatID}:stream`` so external subscribers receive the
    same clean stream without needing an SSE connection.
    """
    asyncRedisParams = {
        "host":             redisParams["host"],
        "port":             redisParams["port"],
        "password":         redisParams.get("password"),
        "db":               redisParams.get("db", 0),
        "decode_responses": True,
    }

    # subscriber: put into pub/sub mode — cannot issue PUBLISH on this connection
    subscriber = aioredis.Redis(**asyncRedisParams)
    pubsub = subscriber.pubsub()
    await pubsub.subscribe(chatID)

    # publisher: separate connection used solely for PUBLISH
    publisher = aioredis.Redis(**asyncRedisParams)

    logger.info(f"[ {chatID} | OpenAI Compat ] : Subscribed to Redis pub/sub channel '{chatID}'")

    result_store: dict = {}
    done_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    def _run_engine() -> None:
        try:
            engine = SQLQueryEngine(
                llmParams,
                dbParams,
                redisParams,
                botName=botName,
                splitIdentifier=splitIdentifier,
            )
            result_store["result"] = engine.run(
                chatID=chatID,
                basePrompt=prompt,
                retryCount=retryCount,
                schemaExamples=schemaExamples,
                feedbackExamples=feedbackExamples,
            )
        except Exception as exc:
            logger.error(f"[ {chatID} | OpenAI Compat ] : Engine error: {exc}", exc_info=True)
            result_store["result"] = {"code": 500, "error": str(exc)}
        finally:
            done_event.set()

    def _extractContent(raw: str) -> str:
        """Strip the engine event tag, return only the content after the split identifier."""
        if splitIdentifier in raw:
            parts = raw.split(splitIdentifier, 1)
            return parts[1] if len(parts) > 1 else ""
        return raw

    async def _yieldAndPublish(text: str, finish_reason=None):
        """Yield one SSE chunk and mirror the same text to the {chatID}:stream pub/sub channel."""
        await publisher.publish(f"{chatID}:stream", text)
        return _formatSSEChunk(text, model, finish_reason)

    # Submit engine to thread pool — do not await yet so pub/sub can stream concurrently
    engine_future = loop.run_in_executor(None, _run_engine)

    # ── SSE opening ──────────────────────────────────────────────────────────
    yield _formatSSEChunkRole(model)
    yield await _yieldAndPublish("<think>\n")

    # ── Stream pub/sub messages while engine runs ────────────────────────────
    while not done_event.is_set():
        try:
            msg = await asyncio.wait_for(
                pubsub.get_message(ignore_subscribe_messages=True),
                timeout=0.05,
            )
            if msg and msg.get("data"):
                content = _extractContent(msg["data"])
                if content:
                    yield await _yieldAndPublish(content)
        except asyncio.TimeoutError:
            pass
        await asyncio.sleep(0.01)

    # ── Drain any in-flight messages after engine completes ──────────────────
    for _ in range(100):
        try:
            msg = await asyncio.wait_for(
                pubsub.get_message(ignore_subscribe_messages=True),
                timeout=0.1,
            )
            if not msg:
                break
            if msg.get("data"):
                content = _extractContent(msg["data"])
                if content:
                    yield await _yieldAndPublish(content)
        except asyncio.TimeoutError:
            break

    await pubsub.unsubscribe(chatID)
    await subscriber.aclose()

    # Ensure the engine future is fully awaited to surface any exceptions
    await engine_future

    # ── Close think block and yield final result ─────────────────────────────
    yield await _yieldAndPublish("\n</think>\n\n")

    final_text = _formatFinalResult(result_store.get("result", {"code": 500, "error": "No result"}))
    yield await _yieldAndPublish(final_text)
    yield await _yieldAndPublish("", finish_reason="stop")

    await publisher.aclose()
    yield "data: [DONE]\n\n"
    logger.info(f"[ {chatID} | OpenAI Compat ] : Streaming complete")

# %%
# Non-streaming helper — collects the full SSE stream and returns one response

async def _collectFullResponse(
    chatID: str,
    prompt: str,
    llmParams: dict,
    dbParams: dict,
    redisParams: dict,
    botName: str,
    splitIdentifier: str,
    retryCount: int,
    schemaExamples: int,
    feedbackExamples: int,
    model: str,
) -> dict:
    """
    Run the streaming generator to completion and return a single OpenAI-format
    chat completion response (non-streaming).
    """
    think_parts: List[str] = []
    final_parts: List[str] = []
    in_think = False

    async for chunk in _streamSQLQueryEngine(
        chatID, prompt, llmParams, dbParams, redisParams,
        botName, splitIdentifier, retryCount, schemaExamples, feedbackExamples, model
    ):
        if chunk.startswith("data: [DONE]") or not chunk.startswith("data: "):
            continue
        try:
            payload = json.loads(chunk[6:])
            delta_content = payload["choices"][0].get("delta", {}).get("content", "")
            if not delta_content:
                continue
            if "<think>" in delta_content:
                in_think = True
            if in_think:
                think_parts.append(delta_content)
            else:
                final_parts.append(delta_content)
            if "</think>" in delta_content:
                in_think = False
        except (json.JSONDecodeError, KeyError, IndexError):
            continue

    full_content = "".join(think_parts) + "".join(final_parts)
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
        "object": "chat.completion",
        "created": int(datetime.now().timestamp()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": full_content},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }

# %%
# Routes

@router.get(
    "/v1/models",
    summary="List available models",
    response_description="OpenAI-compatible model list.",
    dependencies=[Depends(verifyApiKey)],
)
async def list_models() -> dict:
    """
    Return the list of models available through this SQL query engine.

    The model name is controlled by the `COMPLETIONS_MODEL_NAME` environment
    variable (default: value of `BOT_NAME`, falling back to `SQLBot`).
    """
    return {
        "object": "list",
        "data": [
            {
                "id":       COMPLETIONS_MODEL_NAME,
                "object":   "model",
                "created":  int(datetime.now().timestamp()),
                "owned_by": "sql-query-engine",
            }
        ],
    }

@router.post(
    "/v1/chat/completions",
    summary="OpenAI-compatible chat completions (SQL query engine)",
    response_description=(
        "SSE stream of chat completion chunks (stream=true) or a single "
        "chat completion response (stream=false)."
    ),
    dependencies=[Depends(verifyApiKey)],
)
async def chat_completions(
    request: ChatCompletionRequest,
):
    """
    Translate a natural language question into SQL using the two-stage engine.

    ### Streaming behavior (`stream=true`, default)

    Returns a `text/event-stream` response where:

    - Redis pub/sub progress messages are wrapped in `<think>…</think>` tags
      so that reasoning-capable clients (e.g. OpenWebUI) display them as chain-
      of-thought while the query is being built and validated.
    - The final formatted result (SQL query + markdown table) follows after the
      closing `</think>` tag.

    ### Chat ID

    When OpenWebUI (or any compliant client) injects a ``chat_id`` field in
    the request body, that value is used directly as the Redis namespace key,
    so every message in the same OpenWebUI chat shares the same schema context.

    When ``chat_id`` is absent, a stable fallback is derived by MD5-hashing the
    **first user message** in the conversation.  Because OpenWebUI always sends
    the full conversation history on every request, the first message is always
    present and never changes across turns — giving a consistent key for the
    entire conversation without needing an explicit session identifier.

    ### Connection parameters

    All LLM, PostgreSQL, and Redis parameters are read exclusively from
    environment variables. Set them in docker-compose.yml (or the host
    environment) before starting the service. A 500 error is returned immediately
    if any required variable is missing.
    """
    _validateEnvConnParams()

    # Extract prompt from the last user message
    prompt: Optional[str] = None
    for msg in reversed(request.messages):
        if msg.role == "user":
            prompt = msg.content
            break

    if prompt is None:
        raise HTTPException(status_code=422, detail="No 'user' message found in the request messages.")

    if request.chat_id:
        chatID = request.chat_id
        logger.info(f"[ {request.chat_id} | OpenAI Compat ] : using chat_id from OpenWebUI request")
    else:
        chatID = _stableChatID(request.messages)
        logger.warning(f"[ {chatID} | OpenAI Compat ] : chat_id not in request — derived stable ID from first user message (OpenWebUI may not be sending chat_id)")
    model  = COMPLETIONS_MODEL_NAME

    logger.info(f"[ {chatID} | OpenAI Compat ] : chat/completions — prompt='{prompt[:80]}...' stream={request.stream}")

    if request.stream is not False:
        return StreamingResponse(
            _streamSQLQueryEngine(
                chatID=chatID,
                prompt=prompt,
                llmParams=LLM_PARAMS,
                dbParams=DB_PARAMS,
                redisParams=REDIS_PARAMS,
                botName=BOT_NAME,
                splitIdentifier=SPLIT_IDENTIFIER,
                retryCount=DEFAULT_RETRY_COUNT,
                schemaExamples=DEFAULT_SCHEMA_EXAMPLES,
                feedbackExamples=DEFAULT_FEEDBACK_EXAMPLES,
                model=model,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    # Non-streaming path
    return await _collectFullResponse(
        chatID=chatID,
        prompt=prompt,
        llmParams=LLM_PARAMS,
        dbParams=DB_PARAMS,
        redisParams=REDIS_PARAMS,
        botName=BOT_NAME,
        splitIdentifier=SPLIT_IDENTIFIER,
        retryCount=DEFAULT_RETRY_COUNT,
        schemaExamples=DEFAULT_SCHEMA_EXAMPLES,
        feedbackExamples=DEFAULT_FEEDBACK_EXAMPLES,
        model=model,
    )

@router.post(
    "/v1/completions",
    summary="OpenAI-compatible text completions (SQL query engine)",
    response_description=(
        "SSE stream of completion chunks (stream=true) or a single "
        "completion response (stream=false)."
    ),
    dependencies=[Depends(verifyApiKey)],
)
async def completions(
    request: CompletionRequest,
):
    """
    Text-completions variant of the SQL query engine.

    Accepts a single `prompt` string instead of a message list. The chat ID
    is derived from the MD5 hash of the prompt string, so identical prompts
    share a Redis schema context while different prompts get their own.

    All connection parameters are read from environment variables.
    Streaming and non-streaming behavior is identical to `/v1/chat/completions`.
    """
    _validateEnvConnParams()

    chatID = uuid.uuid4().hex[:16]
    model  = COMPLETIONS_MODEL_NAME

    logger.info(f"[ {chatID} | OpenAI Compat ] : completions — prompt='{request.prompt[:80]}...' stream={request.stream}")

    if request.stream is not False:
        return StreamingResponse(
            _streamSQLQueryEngine(
                chatID=chatID,
                prompt=request.prompt,
                llmParams=LLM_PARAMS,
                dbParams=DB_PARAMS,
                redisParams=REDIS_PARAMS,
                botName=BOT_NAME,
                splitIdentifier=SPLIT_IDENTIFIER,
                retryCount=DEFAULT_RETRY_COUNT,
                schemaExamples=DEFAULT_SCHEMA_EXAMPLES,
                feedbackExamples=DEFAULT_FEEDBACK_EXAMPLES,
                model=model,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    # Non-streaming path
    response = await _collectFullResponse(
        chatID=chatID,
        prompt=request.prompt,
        llmParams=LLM_PARAMS,
        dbParams=DB_PARAMS,
        redisParams=REDIS_PARAMS,
        botName=BOT_NAME,
        splitIdentifier=SPLIT_IDENTIFIER,
        retryCount=DEFAULT_RETRY_COUNT,
        schemaExamples=DEFAULT_SCHEMA_EXAMPLES,
        feedbackExamples=DEFAULT_FEEDBACK_EXAMPLES,
        model=model,
    )
    # Convert to legacy completions response shape
    return {
        "id":      response["id"].replace("chatcmpl-", "cmpl-"),
        "object":  "text_completion",
        "created": response["created"],
        "model":   response["model"],
        "choices": [
            {
                "text":         response["choices"][0]["message"]["content"],
                "index":        0,
                "logprobs":     None,
                "finish_reason": "stop",
            }
        ],
        "usage": response["usage"],
    }
