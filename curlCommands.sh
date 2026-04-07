#!/usr/bin/env bash
# =============================================================================
# SQL Query Engine — curl command reference
# =============================================================================
# All commands target the Docker host port (5181).
# Set the variables below to match your environment, then copy/paste any block.
#
# Assumptions for the /inference/* commands:
#   - env vars (LLM_*, POSTGRES_*, REDIS_*) are already configured on the server
#     so query parameters are NOT repeated in these examples.
#   - If env vars are NOT set, append the full query string shown in the
#     "Explicit connection params" section at the bottom of each group.
# =============================================================================

BASE_URL="http://localhost:5181"
CHAT_ID="test-session-001"          # any string; namespaces Redis context
API_KEY="change-me"                  # only needed when OPENAI_API_KEY is set on server


# ─────────────────────────────────────────────────────────────────────────────
# 1. HEALTH CHECK — GET /ping
# ─────────────────────────────────────────────────────────────────────────────

curl -s -X GET "${BASE_URL}/ping" | jq .


# ─────────────────────────────────────────────────────────────────────────────
# 2. FULL PIPELINE — POST /inference/sqlQueryEngine/{chatID}
#    Stage 1 (NL → SQL) + Stage 2 (execute + repair loop)
# ─────────────────────────────────────────────────────────────────────────────

# Minimal request — env vars must be configured on the server
curl -s -X POST "${BASE_URL}/inference/sqlQueryEngine/${CHAT_ID}" \
  -H "Content-Type: application/json" \
  -d '{
    "basePrompt": "How many orders were placed in the last 30 days?"
  }' | jq .

# Full request — all optional pipeline params spelled out
curl -s -X POST "${BASE_URL}/inference/sqlQueryEngine/${CHAT_ID}" \
  -H "Content-Type: application/json" \
  -d '{
    "basePrompt": "How many orders were placed in the last 30 days?",
    "retryCount": 5,
    "schemaExamples": 5,
    "feedbackExamples": 3,
    "schemaDescriptionKey": "dbSchemaDescription",
    "extraPayload": {"source": "curl-test"}
  }' | jq .

# Force fresh schema generation by changing schemaDescriptionKey
curl -s -X POST "${BASE_URL}/inference/sqlQueryEngine/${CHAT_ID}" \
  -H "Content-Type: application/json" \
  -d '{
    "basePrompt": "Show me the top 10 customers by total order value.",
    "schemaDescriptionKey": "dbSchemaDescription_v2"
  }' | jq .

# Multi-turn: same chatID reuses cached schema context on second call
curl -s -X POST "${BASE_URL}/inference/sqlQueryEngine/${CHAT_ID}" \
  -H "Content-Type: application/json" \
  -d '{
    "basePrompt": "Now break that down by country."
  }' | jq .


# ─────────────────────────────────────────────────────────────────────────────
# 3. STAGE 1 ONLY — POST /inference/sqlQueryGeneration/{chatID}
#    Translates NL to SQL, does NOT execute the query
# ─────────────────────────────────────────────────────────────────────────────

curl -s -X POST "${BASE_URL}/inference/sqlQueryGeneration/${CHAT_ID}" \
  -H "Content-Type: application/json" \
  -d '{
    "basePrompt": "List all active users who signed up this year."
  }' | jq .

# With all optional params
curl -s -X POST "${BASE_URL}/inference/sqlQueryGeneration/${CHAT_ID}" \
  -H "Content-Type: application/json" \
  -d '{
    "basePrompt": "List all active users who signed up this year.",
    "schemaExamples": 10,
    "schemaDescriptionKey": "dbSchemaDescription",
    "extraPayload": null
  }' | jq .


# ─────────────────────────────────────────────────────────────────────────────
# 4. STAGE 2 ONLY — POST /inference/sqlQueryEvaluation/{chatID}
#    Executes a query you supply and repairs it if it fails
# ─────────────────────────────────────────────────────────────────────────────

curl -s -X POST "${BASE_URL}/inference/sqlQueryEvaluation/${CHAT_ID}" \
  -H "Content-Type: application/json" \
  -d '{
    "basePrompt": "How many orders were placed in the last 30 days?",
    "baseQuery": "SELECT COUNT(*) FROM orders WHERE created_at >= NOW() - INTERVAL '\''30 days'\''",
    "baseDescription": "Count orders placed in the last 30 days."
  }' | jq .

# Full evaluation request with all optional params
curl -s -X POST "${BASE_URL}/inference/sqlQueryEvaluation/${CHAT_ID}" \
  -H "Content-Type: application/json" \
  -d '{
    "basePrompt": "How many orders were placed in the last 30 days?",
    "baseQuery": "SELECT COUNT(*) AS order_count FROM orders WHERE created_at >= NOW() - INTERVAL '\''30 days'\''",
    "baseDescription": "Count orders placed in the last 30 days.",
    "retryCount": 3,
    "schemaExamples": 5,
    "feedbackExamples": 3,
    "schemaDescriptionKey": "dbSchemaDescription",
    "extraPayload": null
  }' | jq .

# Deliberately broken query — lets the repair loop demonstrate itself
curl -s -X POST "${BASE_URL}/inference/sqlQueryEvaluation/${CHAT_ID}" \
  -H "Content-Type: application/json" \
  -d '{
    "basePrompt": "Show me customer names and their total spend.",
    "baseQuery": "SELECT customer_name, SUM(total) FROM nonexistent_table GROUP BY customer_name",
    "baseDescription": "Get customer names and total spend from orders.",
    "retryCount": 3
  }' | jq .


# ─────────────────────────────────────────────────────────────────────────────
# 5. OPENAI-COMPATIBLE — GET /v1/models
#    Returns the model name exposed by this engine
# ─────────────────────────────────────────────────────────────────────────────

curl -s -X GET "${BASE_URL}/v1/models" \
  -H "Authorization: Bearer ${API_KEY}" | jq .


# ─────────────────────────────────────────────────────────────────────────────
# 6. OPENAI-COMPATIBLE CHAT — POST /v1/chat/completions
#    Use this with any OpenAI SDK, LangChain, curl, or OpenWebUI
# ─────────────────────────────────────────────────────────────────────────────

# Streaming (default) — raw SSE output
curl -s -X POST "${BASE_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "model": "SQLBot",
    "messages": [
      {"role": "user", "content": "How many orders were placed in the last 30 days?"}
    ],
    "stream": true
  }'

# Streaming with auth
curl -s -X POST "${BASE_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "model": "SQLBot",
    "messages": [
      {"role": "user", "content": "Show me the top 5 customers by revenue."}
    ],
    "stream": true
  }'

# Non-streaming — returns a single JSON response
curl -s -X POST "${BASE_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "model": "SQLBot",
    "messages": [
      {"role": "user", "content": "How many active users registered this month?"}
    ],
    "stream": false
  }' | jq .

# ── chat_id: context preservation across separate requests ───────────────────
#
# chat_id is a field in the request body (injected automatically by OpenWebUI;
# supply it manually from any other client).
#
# What it does:
#   - The engine stores schema context in Redis under the key {chat_id}:SQLQueryEngine.
#   - Turn 1: schema is introspected from PostgreSQL, described by the LLM, and cached.
#   - Turn 2+: the same chat_id means the cached schema is reused — no re-introspection.
#
# Without chat_id: the engine derives a stable ID from MD5(first user message).
#   - Stable only if the first message never changes across requests (fine for most clients).
#   - Explicitly providing chat_id is more reliable for multi-turn sessions.

# Turn 1 — first request with this chat_id (schema is generated and cached in Redis)
curl -s -X POST "${BASE_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "model": "SQLBot",
    "chat_id": "my-session-001",
    "messages": [
      {"role": "user", "content": "How many orders were placed last month?"}
    ],
    "stream": false
  }' | jq .

# Turn 2 — same chat_id: schema context is reused from Redis, no re-introspection
curl -s -X POST "${BASE_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "model": "SQLBot",
    "chat_id": "my-session-001",
    "messages": [
      {"role": "user",      "content": "How many orders were placed last month?"},
      {"role": "assistant", "content": "There were 842 orders placed last month."},
      {"role": "user",      "content": "Break that down by product category."}
    ],
    "stream": false
  }' | jq .

# Turn 3 — follow-up in the same session
curl -s -X POST "${BASE_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "model": "SQLBot",
    "chat_id": "my-session-001",
    "messages": [
      {"role": "user",      "content": "How many orders were placed last month?"},
      {"role": "assistant", "content": "There were 842 orders placed last month."},
      {"role": "user",      "content": "Break that down by product category."},
      {"role": "assistant", "content": "Here is the breakdown by category..."},
      {"role": "user",      "content": "Which category had the highest growth vs the previous month?"}
    ],
    "stream": false
  }' | jq .

# Pretty-print only the final assistant content from a non-streaming response
curl -s -X POST "${BASE_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "model": "SQLBot",
    "messages": [
      {"role": "user", "content": "What are the top 3 selling products?"}
    ],
    "stream": false
  }' | jq -r '.choices[0].message.content'


# ─────────────────────────────────────────────────────────────────────────────
# 7. OPENAI-COMPATIBLE TEXT COMPLETIONS — POST /v1/completions
#    Legacy single-prompt shape; each request gets a fresh chatID
# ─────────────────────────────────────────────────────────────────────────────

# Streaming (default)
curl -s -X POST "${BASE_URL}/v1/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "model": "SQLBot",
    "prompt": "How many users signed up in the last 7 days?",
    "stream": true
  }'

# Non-streaming — returns legacy text_completion shape
curl -s -X POST "${BASE_URL}/v1/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "model": "SQLBot",
    "prompt": "List all tables and row counts in the database.",
    "stream": false
  }' | jq .

# Extract just the text from a non-streaming completions response
curl -s -X POST "${BASE_URL}/v1/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${API_KEY}" \
  -d '{
    "model": "SQLBot",
    "prompt": "What is the average order value this year?",
    "stream": false
  }' | jq -r '.choices[0].text'


# ─────────────────────────────────────────────────────────────────────────────
# 8. INFERENCE WITH EXPLICIT CONNECTION PARAMS (override env vars)
#    Use when the server has no env vars set, or to target a different backend
#    Replace all placeholder values with your actual connection details
# ─────────────────────────────────────────────────────────────────────────────

LLM_BASE_URL="http://localhost:11434/v1"
LLM_MODEL="qwen2.5-coder:7b"
LLM_API_KEY="ollama"
LLM_TEMPERATURE="0.1"
PG_HOST="localhost"
PG_PORT="5432"
PG_DB="mydb"
PG_USER="postgres"
PG_PASS="secret"
REDIS_HOST="localhost"
REDIS_PORT="6379"
REDIS_PASS=""
REDIS_DB="0"

curl -s -X POST "${BASE_URL}/inference/sqlQueryEngine/${CHAT_ID}?llmBaseURL=${LLM_BASE_URL}&llmAPIKey=${LLM_API_KEY}&llmModel=${LLM_MODEL}&llmTemperature=${LLM_TEMPERATURE}&postgreHost=${PG_HOST}&postgrePort=${PG_PORT}&postgreDBName=${PG_DB}&postgreUser=${PG_USER}&postgrePassword=${PG_PASS}&redisHost=${REDIS_HOST}&redisPort=${REDIS_PORT}&redisPassword=${REDIS_PASS}&redisDB=${REDIS_DB}" \
  -H "Content-Type: application/json" \
  -d '{
    "basePrompt": "How many records are in each table?"
  }' | jq .

# Same pattern works for /inference/sqlQueryGeneration and /inference/sqlQueryEvaluation
# — just replace the path segment and body fields accordingly.


# ─────────────────────────────────────────────────────────────────────────────
# 9. SWAGGER UI / OPENAPI DOCS
# ─────────────────────────────────────────────────────────────────────────────

# Open in browser
# xdg-open http://localhost:5181/docs

# Download the OpenAPI schema
curl -s "${BASE_URL}/openapi.json" | jq .
