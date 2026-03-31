#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import logging
import json
import re
import psycopg
import traceback
import warnings
from pydantic import BaseModel, Field, model_validator
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

# LangChain's with_structured_output() stores the parsed Pydantic model in
# AIMessage.parsed. Pydantic v2's serializer emits a harmless UserWarning when
# it encounters a non-None value in that field — suppress it.
warnings.filterwarnings(
    "ignore",
    message=".*Pydantic serializer warnings.*",
    category=UserWarning,
    module=r"pydantic.*"
)

from .dbHandler import PostgresDB
from .sessionManager import SessionManager
from .promptTemplates import postgreSchemaDescriptionPrompt, queryEvaluatorFixerPrompt
from .sqlGuidelines import postgreManualData, postgreManualDataEval

logger = logging.getLogger(__name__)

# %%
# Structured output schema for SQL evaluation — the LLM produces this
class QueryEvaluationSchema(BaseModel):
    """Schema for SQL query evaluation and repair."""
    isValid: bool = Field(default=False, description="Always false. The system verifies by executing.")
    modifiedUserPrompt: str = Field(default="", description="The original user prompt, optionally modified.")
    observation: str = Field(default="", description="What was wrong and how it was fixed.")
    fixedQuery: str = Field(default="", description="The corrected SQL query.")
    fixed_query: str = Field(default="", description="Alias for fixedQuery.")
    sql: str = Field(default="", description="Alias for fixedQuery.")
    query: str = Field(default="", description="Alias for fixedQuery.")

    @model_validator(mode="after")
    def normalize_query_field(self):
        """Accept various field names for the fixed query."""
        if not self.fixedQuery:
            self.fixedQuery = self.fixed_query or self.sql or self.query
        return self

# %%
# SQL query evaluator — runs the generated query against PostgreSQL and uses
# an LLM to iteratively repair it if execution fails or returns empty results
class QueryEvaluator:
    def __init__(
        self,
        llmParams: dict,
        dbParams: dict,
        redisParams: dict,
        botName: str = "SQLBot",
        agentName: str = "SQLQueryEngine",
        splitIdentifier: str = "<|-/|-/>"
    ) -> None:
        """
        Initialize the SQL query evaluator with LLM, database, and Redis parameters.

        Arguments:
        ----------
        llmParams : dict
            LLM connection parameters.
            model : str
                The model name to use for evaluation.
            temperature : float
                Controls randomness in LLM responses.
            base_url : str
                Base URL for the OpenAI-compatible API endpoint.
            api_key : str
                API key for authenticating with the LLM service.

        dbParams : dict
            PostgreSQL connection parameters.
            host : str
                Database host address.
            port : int
                Database port number.
            dbname : str
                Database name.
            user : str
                Database user name.
            password : str
                Database user password.

        redisParams : dict
            Redis connection parameters.
            host : str
                Redis server host.
            port : int
                Redis server port.
            password : str
                Redis authentication password.
            db : int
                Redis database number.
            decode_responses : bool
                Whether to decode responses as strings.

        botName : str
            Display name for the assistant in prompts.
            Default : SQLBot
        agentName : str
            Namespace used for Redis key isolation per agent.
            Default : SQLQueryEngine
        splitIdentifier : str
            Delimiter used in Redis Pub/Sub messages to separate the event tag from content.
            Default : <|-/|-/>
        """
        self.llmParams = llmParams
        self.dbParams = dbParams
        self.redisParams = redisParams
        self.botName = botName
        self.agentName = agentName
        self.splitIdentifier = splitIdentifier

        self.llm = ChatOpenAI(**self.llmParams)

        self.postgreDB = PostgresDB(**self.dbParams)
        self.chatInstance = SessionManager(self.redisParams, self.agentName)

    @staticmethod
    def _parseEvalResponse(content: str) -> "QueryEvaluationSchema":
        """Extract fixed query from LLM response. Tries JSON, then code blocks, then SELECT."""
        # Strip <think> tags first
        cleaned = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()

        # Try JSON parse
        try:
            data = json.loads(cleaned)
            return QueryEvaluationSchema(**data)
        except (json.JSONDecodeError, Exception):
            pass

        # Try extracting JSON object from text
        jsonMatch = re.search(r'\{[^{}]*"(?:fixedQuery|fixed_query|sql|query)"[^{}]*\}', cleaned, re.DOTALL)
        if jsonMatch:
            try:
                data = json.loads(jsonMatch.group())
                return QueryEvaluationSchema(**data)
            except (json.JSONDecodeError, Exception):
                pass

        # Extract SQL from code blocks
        codeBlock = re.search(r'```(?:sql)?\s*\n?(.*?)\n?```', cleaned, re.DOTALL | re.IGNORECASE)
        if codeBlock:
            return QueryEvaluationSchema(fixedQuery=codeBlock.group(1).strip(), observation=cleaned[:200])

        # Find SELECT statement
        selectMatch = re.search(r'(SELECT\s+.+?;)', cleaned, re.DOTALL | re.IGNORECASE)
        if selectMatch:
            return QueryEvaluationSchema(fixedQuery=selectMatch.group(1).strip(), observation=cleaned[:200])

        # Last resort
        return QueryEvaluationSchema(fixedQuery=cleaned, observation="Could not parse response")

    def _buildFromPayload(self, generatorContextKey: str, localPayload: dict) -> dict:
        """
        Build schema context from a locally provided payload dict.

        Arguments:
        ----------
        generatorContextKey : str
            The key identifying the schema description in the payload.
        localPayload : dict
            The payload returned by the QueryGenerator containing chat histories.

        Returns:
        --------
        dict
            Status, baseKey, rawData, and parsed LangChain message list.
        """
        try:
            for i in list(localPayload['data'].keys()):
                if generatorContextKey in i:
                    break
            schemaDB = list()
            for j in localPayload['data'][i]:
                if j['role'] == 'system':
                    schemaDB.append(SystemMessage(content=j['content']))
                elif j['role'] == 'user':
                    schemaDB.append(HumanMessage(content=j['content']))
                elif j['role'] == 'assistant':
                    schemaDB.append(AIMessage(content=j['content']))

            return {
                'status': 200,
                'baseKey': i,
                'rawData': localPayload['data'][i],
                'data':  schemaDB
            }
        except Exception as e:
            logger.error(f"[ _buildFromPayload | {self.agentName} ] : Failed to build context from payload: {str(e)}", exc_info=True)
            return {
                'status': 500,
                'baseKey': None,
                'rawData': None,
                'data': None
            }

    def _buildFromScratch(self, chatID: str, generatorContextKey: str, schemaExamples: int) -> dict:
        """
        Generate a fresh schema description by querying the database and invoking the LLM.

        Arguments:
        ----------
        chatID : str
            The user ID for Redis namespacing and Pub/Sub streaming.
        generatorContextKey : str
            The Redis field key to store the schema description under.
        schemaExamples : int
            Number of sample rows per table to include in the schema context.

        Returns:
        --------
        dict
            Status, baseKey, rawData, and parsed LangChain message list.
        """
        logger.info(f"[ {chatID} | {self.agentName} ] : Fetching and parsing database schema")
        schemaDump, schemaParsed = self.postgreDB.getParsedSchemaDump(expLen=schemaExamples)
        logger.info(f"[ {chatID} | {self.agentName} ] : Generating schema description from scratch")
        schemaDescriptionChat = ChatPromptTemplate.from_messages([postgreSchemaDescriptionPrompt]).format_messages(
            botName=self.botName,
            botGoal="""
            Generate a detailed schema description that includes:
            - **Table Overview**: Describe the purpose and role of each table.
            - **Column Details**: List all columns with data types, constraints, defaults, and descriptions.
            - **Relationships**: Identify foreign key relationships and join conditions with cardinality.
            - **Sample Data Insights**: Analyze sample data to infer typical values, ranges, and patterns.
            - **Constraints and Notes**: Highlight indexes, JSONB structures, or schema-specific considerations.
            - **Validation**: Cross-reference schema details with PostgreSQL documentation for accuracy.
            """,
            dataContext=schemaParsed,
            postgreManual=postgreManualData
        ) + [HumanMessage(content=f"Based on the current database schema, generate a detailed schema description:\n\n{schemaParsed}")]

        dbDescription = ""
        for chunk in self.llm.stream(schemaDescriptionChat):
            dbDescription += chunk.content
            self.chatInstance.redisClient.publish(
                f"{chatID}",
                f"</SQLQueryEvaluator:schemaDescriptionChat>{self.splitIdentifier}{chunk.content}"
            )
        dbDescription = AIMessage(content=dbDescription)
        logger.info(f"[ {chatID} | {self.agentName} ] : Schema description generated")
        schemaDescriptionChat += [dbDescription]
        self.chatInstance.postUserChatContext(chatID, generatorContextKey, schemaDescriptionChat)

        # Convert to JSON-serializable format
        newSchemaDesc = list()
        for i in schemaDescriptionChat:
            if isinstance(i, SystemMessage):
                newSchemaDesc.append({"role": "system", "content": i.content})
            elif isinstance(i, HumanMessage):
                newSchemaDesc.append({"role": "user", "content": i.content})
            elif isinstance(i, AIMessage):
                newSchemaDesc.append({"role": "assistant", "content": i.content})

        return {
            'status': 200,
            'baseKey': generatorContextKey,
            'rawData': newSchemaDesc,
            'data': schemaDescriptionChat
        }

    def _buildFromRedis(self, chatID: str, generatorContextKey: str) -> dict:
        """
        Fetch an existing schema description from Redis.

        Arguments:
        ----------
        chatID : str
            The user ID for Redis namespacing.
        generatorContextKey : str
            The Redis field key holding the cached schema description.

        Returns:
        --------
        dict
            Status, baseKey, rawData, and parsed LangChain message list.
        """
        try:
            storedContext, jsonStoredContext = self.chatInstance.getUserChatContext(chatID, generatorContextKey)
            return {
                'status': 200,
                'baseKey': generatorContextKey,
                'rawData': jsonStoredContext,
                'data': storedContext
            }
        except Exception as e:
            logger.error(f"[ {chatID} | {self.agentName} ] : Failed to load context from Redis: {str(e)}", exc_info=True)
            return {
                'status': 500,
                'baseKey': None,
                'rawData': None,
                'data': None
            }

    def process(
        self,
        chatID: str,
        basePrompt: str,
        baseQuery: str,
        baseDescription: str,
        retryCount: int = 3,
        generatorContextKey: str = "dbSchemaDescription",
        schemaExamples: int = 5,
        feedbackExamples: int = 5,
        localPayload: None | dict = None,
        hardLimit: int = 50
    ) -> dict:
        """
        Run the SQL evaluation and iterative repair loop.

        The method resolves schema context (from payload, Redis, or fresh generation),
        then loops up to retryCount times: executing the query, feeding errors and results
        to the LLM evaluator, and updating the query with the LLM's fix until the query
        succeeds or the retry limit is reached.

        Arguments:
        ----------
        chatID : str
            Unique identifier for the user; used to namespace Redis keys and Pub/Sub channels.
        basePrompt : str
            The original natural language question.
        baseQuery : str
            The initial SQL query to evaluate (typically from QueryGenerator).
        baseDescription : str
            The description of the initial SQL query.
        retryCount : int
            Maximum number of LLM-driven repair attempts.
            Default : 3
        generatorContextKey : str
            Redis field key for the schema description context.
            Default : dbSchemaDescription
        schemaExamples : int
            Number of sample rows per table to include in schema context.
            Default : 5
        feedbackExamples : int
            Number of result rows to feed back to the LLM during evaluation.
            Default : 5
        localPayload : dict or None
            Payload from a prior QueryGenerator call to reuse schema context.
            Default : None
        hardLimit : int
            Maximum number of result rows to return in the final response.
            Default : 50

        Returns:
        --------
        dict
            A response dictionary containing:
            - code : int — 200 on success or exhausted retries.
            - data : dict — Full evaluation history and final results.
            - response : dict — Slim response with currentQuery, observation, and results.
        """
        # Resolve schema description context from payload, Redis, or scratch
        if localPayload is not None and len(localPayload) > 0:
            logger.info(f"[ {chatID} | {self.agentName} @ Context ] : Building context from local payload")
            retter = self._buildFromPayload(generatorContextKey, localPayload)
            if retter['status'] == 200:
                logger.info(f"[ {chatID} | {self.agentName} @ Context ] : Context loaded from payload successfully")
                self.chatInstance.redisClient.hset(f"{chatID}:{self.agentName}", generatorContextKey, json.dumps(retter['rawData']))
                schemaDesc = retter['data']
            else:
                logger.info(f"[ {chatID} | {self.agentName} @ Context ] : Payload context failed, trying Redis")
                try:
                    redisRetter = self._buildFromRedis(chatID, generatorContextKey)
                    if redisRetter['status'] == 200:
                        logger.info(f"[ {chatID} | {self.agentName} @ Context ] : Context loaded from Redis")
                        schemaDesc = redisRetter['data']
                    else:
                        logger.info(f"[ {chatID} | {self.agentName} @ Context ] : No Redis context, generating from scratch")
                        schemaDesc = self._buildFromScratch(chatID, generatorContextKey, schemaExamples)['data']
                except Exception as e:
                    logger.warning(f"[ {chatID} | {self.agentName} @ Context ] : Redis error, generating from scratch: {str(e)}")
                    schemaDesc = self._buildFromScratch(chatID, generatorContextKey, schemaExamples)['data']
        else:
            try:
                redisRetter = self._buildFromRedis(chatID, generatorContextKey)
                if redisRetter['status'] == 200:
                    logger.info(f"[ {chatID} | {self.agentName} @ Context ] : Context loaded from Redis")
                    schemaDesc = redisRetter['data']
                else:
                    logger.info(f"[ {chatID} | {self.agentName} @ Context ] : No Redis context, generating from scratch")
                    schemaDesc = self._buildFromScratch(chatID, generatorContextKey, schemaExamples)['data']
            except Exception as e:
                logger.warning(f"[ {chatID} | {self.agentName} @ Context ] : Redis error, generating from scratch: {str(e)}")
                schemaDesc = self._buildFromScratch(chatID, generatorContextKey, schemaExamples)['data']

        # Begin the iterative SQL evaluation and repair loop
        logger.info(f"[ {chatID} | {self.agentName} ] : Starting SQL evaluation loop")
        schemaDump, schemaParsed = self.postgreDB.getParsedSchemaDump(expLen=schemaExamples)
        validatorChat = ChatPromptTemplate.from_messages([queryEvaluatorFixerPrompt]).format_messages(
            botName=self.botName,
            botGoal=f"You are a helpful SQL assistant named {self.botName}. Evaluate and fix SQL queries. All queries must be read-only (SELECT only). Only fix queries that have execution errors or return empty results. Use ROUND(..., 2) for decimals. Never use bind parameters — use literal values. Only add LIMIT if the original query had one. Return ONLY the fixed SQL query.",
            postgreManual=postgreManualDataEval
        )

        currentQuery = baseQuery
        currentObservation = baseDescription
        currentUserPrompt = basePrompt
        validator = False
        retryCounter = 0

        # Track the best result seen across all attempts — if we exhaust retries,
        # return the best we got instead of nothing.
        bestQuery = baseQuery
        bestResults = list()
        bestObservation = baseDescription

        while ((not validator) and (retryCounter < retryCount)):
            errorLines = list()
            errorStr = "No errors encountered."
            retter = list()

            try:
                retter = self.postgreDB.queryExecutor(currentQuery)
            except psycopg.Error as e:
                errorLines.append("Psycopg Error caught:")
                errorLines.append(f"Error type: {type(e).__name__}")
                errorLines.append(f"Error message: {e}")
                errorLines.append(f"SQLSTATE code: {getattr(e, 'pgcode', None)}")
                errorLines.append(f"PostgreSQL error message: {getattr(e, 'pgerror', None)}")

                diag = getattr(e, "diag", None)
                if diag is not None:
                    errorLines.append(f"PostgreSQL diag.sqlstate: {getattr(diag, 'sqlstate', None)}")
                    errorLines.append(f"PostgreSQL diag.message_primary: {getattr(diag, 'message_primary', None)}")
                    errorLines.append(f"PostgreSQL diag.message_detail: {getattr(diag, 'message_detail', None)}")
                    errorLines.append(f"PostgreSQL diag.message_hint: {getattr(diag, 'message_hint', None)}")

                fullTb = traceback.format_exc()
                errorLines.append("Full Python Traceback:")
                errorLines.append(fullTb)
                errorStr = "\n".join(str(line) for line in errorLines)
                self.postgreDB.conn.rollback()

            except Exception as e:
                errorLines.append("Unexpected Error caught:")
                errorLines.append(f"Error type: {type(e).__name__}")
                errorLines.append(f"Error message: {str(e)}")
                errorLines.append(f"Exception repr: {repr(e)}")

                fullTb = traceback.format_exc()
                errorLines.append("Full Python Traceback:")
                errorLines.append(fullTb)
                errorStr = "\n".join(str(line) for line in errorLines)
                self.postgreDB.conn.rollback()

            # Track best result — prefer the attempt with the most rows
            if len(errorLines) == 0 and len(retter) > len(bestResults):
                bestQuery = currentQuery
                bestResults = retter
                bestObservation = currentObservation

            # If the query executed successfully and returned rows, accept it immediately
            # without invoking the LLM evaluator — this prevents regressions where
            # the LLM rewrites a working query into a broken one.
            if len(errorLines) == 0 and len(retter) > 0:
                logger.info(f"[ {chatID} | {self.agentName} # {retryCounter+1} ] : Query executed successfully with {len(retter)} rows — accepting without LLM evaluation")
                self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Query executed successfully with {len(retter)} rows")
                currentObservation = f"Query executed successfully and returned {len(retter)} rows."
                validator = True
                retryCounter += 1
                continue

            # Prepare a focused evaluation prompt — only include schema on first attempt
            # to avoid context bloat on subsequent retries.
            if retryCounter == 0:
                schemaSection = f"""
            Database Schema:
            {schemaParsed}

            Schema Description (summary):
            {schemaDesc[-1].content[:3000]}
            """
            else:
                schemaSection = "(Schema provided in previous message — refer to it above.)"

            validatorChat.append(HumanMessage(content=f"""Fix this SQL query. It has {"execution errors" if len(errorLines) > 0 else "returned empty results"}.

            RULES: Only SELECT columns the user asked for. Use ROUND(..., 2) for decimals. No bind parameters. Use PostgreSQL syntax. Make minimal changes.

            User Question: {currentUserPrompt}

            Failed Query:
            {currentQuery}

            Error:
            {errorStr}

            Results: {str(retter[:feedbackExamples]) if retter else "EMPTY — 0 rows returned"}

            {schemaSection}

            Return isValid=false and provide the fixed query in fixedQuery.
            """))

            # Publish current query + execution errors before invoking the LLM
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Current Query : {currentQuery}")
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Current Observation : {currentObservation}")
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Execution Errors : {errorStr}")

            # Invoke the LLM evaluator to get a fixed query
            rawEvalResp = self.llm.invoke(validatorChat)
            evalQuery = self._parseEvalResponse(rawEvalResp.content)

            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}LLM Observation : {evalQuery.observation}")
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Fixed Query : {evalQuery.fixedQuery}")

            validatorChat.append(AIMessage(content=f"Observation: {evalQuery.observation}\nFixed Query: {evalQuery.fixedQuery}"))

            # Update state with the LLM's fix — the NEXT iteration will execute
            # and verify it. We never trust isValid from the LLM; we verify ourselves.
            currentQuery = evalQuery.fixedQuery
            currentObservation = evalQuery.observation
            currentUserPrompt = evalQuery.modifiedUserPrompt

            logger.info(f"[ {chatID} | {self.agentName} # {retryCounter+1} ] : LLM produced fix — will verify on next iteration")
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Will verify fixed query on next iteration")

            retryCounter += 1

        # Persist the full evaluation chat history in Redis
        chatCounter = self.chatInstance.updateUsageToken(chatID)
        self.chatInstance.postUserChatContext(chatID, f"validatorChat:{chatCounter}", validatorChat)
        logger.info(f"[ {chatID} | {self.agentName} @ {chatCounter} ] : Evaluation chat stored in Redis")

        # Serialize the evaluation chat for the response payload
        tempValidatorChat = list()
        for i in validatorChat:
            if isinstance(i, SystemMessage):
                tempValidatorChat.append({"role": "system", "content": i.content})
            elif isinstance(i, HumanMessage):
                tempValidatorChat.append({"role": "user", "content": i.content})
            elif isinstance(i, AIMessage):
                tempValidatorChat.append({"role": "assistant", "content": i.content})

        # Determine the final query and results to return.
        # If validation succeeded, use the current state.
        # If retries exhausted, return the best result we saw instead of nothing.
        if validator:
            finalQuery = currentQuery.replace("\n", " ")
            finalObservation = currentObservation
            finalResults = retter
        elif len(bestResults) > 0:
            logger.info(f"[ {chatID} | {self.agentName} ] : Retries exhausted — returning best result ({len(bestResults)} rows)")
            finalQuery = bestQuery.replace("\n", " ")
            finalObservation = f"Retries exhausted. Returning best result with {len(bestResults)} rows. {bestObservation}"
            finalResults = bestResults
        else:
            finalQuery = None
            finalObservation = None
            finalResults = list()

        return {
            "code": 200,
            "chatID": chatID,
            "schemaExamples": schemaExamples,
            "feedbackExamples": feedbackExamples,
            "basePrompt": basePrompt,
            "baseQuery": baseQuery,
            "baseDescription": baseDescription,
            "retryCount": retryCount,
            "contextKey": generatorContextKey,
            "data": {
                "queryEvaluationHistory": tempValidatorChat,
                "currentQuery": finalQuery,
                "currentObservation": finalObservation,
                "results": finalResults
            },
            "response": {
                "currentQuery": finalQuery,
                "currentObservation": finalObservation,
                "results": finalResults[:hardLimit]
            }
        }


# %%
# Execution
if __name__ == "__main__":
    print("[ Query Evaluator ] : This module is intended to be imported, not run directly.")
