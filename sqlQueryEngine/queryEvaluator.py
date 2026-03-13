#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import logging
import json
import psycopg
import traceback
import warnings
from pydantic import BaseModel, Field
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
    isValid: bool = Field(..., description="True if the query executed successfully and returned relevant results. False on errors or empty results.")
    modifiedUserPrompt: str = Field(..., description="The original user prompt, optionally modified to better align with the schema if the query failed.")
    observation: str = Field(..., description="Detailed observation about query validity, errors encountered, and what was changed.")
    fixedQuery: str = Field(..., description="The corrected SQL query. Use the provided schema description to fix schema mismatch errors.")

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
        self.queryEvaluationInstance = self.llm.with_structured_output(QueryEvaluationSchema)

        self.postgreDB = PostgresDB(**self.dbParams)
        self.chatInstance = SessionManager(self.redisParams, self.agentName)

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
            botGoal=f"You are a helpful SQL assistant named {self.botName}. Evaluate and fix SQL queries. All queries must be read-only and output LIMIT <= 100.",
            postgreManual=postgreManualDataEval
        )

        currentQuery = baseQuery
        currentObservation = baseDescription
        currentUserPrompt = basePrompt
        validator = False
        retryCounter = 0

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

            # Prepare the evaluation prompt with execution results and error details
            validatorChat.append(HumanMessage(content=f"""
            Check and validate the following SQL query based on the database schema description, schema, user prompt, observations, and execution results provided below. If there are errors in the execution or if the results are empty, fix the query accordingly. Provide a detailed observation of what was wrong and how it was fixed. Also, modify the user prompt if necessary to better align with the database schema and requirements.

            Current Database Schema Description:
            ---------------------------------
            {schemaDesc[-1].content}

            Current Database Schema:
            ---------------------------
            {schemaParsed}

            User Prompt:
            -------------
            {currentUserPrompt}

            CURRENTLY EXECUTED QUERY USED TO GENERATE RESULTS:
            ---------------------------------------------------
            {currentQuery}

            Retrieved Observations after Above Query Execution:
            ---------------------------------------------------
            {currentObservation}

            Execution Result or Outputs from the above query execution:
            [Fix the executed query based on the data below, if there are errors or empty results]:
            NOTE: MAKE SURE TO RETURN isValid=False IF THERE ARE ERRORS OR EMPTY RESULTS
            ---------------------------------------------------------------------------
            {str(retter[:feedbackExamples])}

            Error Messages from the above query execution:
            [Fix the executed query based on the errors below, if there are errors]:
            NOTE: MAKE SURE TO RETURN isValid=False IF THERE ARE ERRORS
            -------------------------------------------------------------------------
            {errorStr}
            """))

            # Publish current query + execution errors before invoking the LLM
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Current Query : {currentQuery}")
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Current Observation : {currentObservation}")
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Execution Errors : {errorStr}")

            # Invoke the LLM evaluator and update the loop state
            evalQuery = self.queryEvaluationInstance.invoke(validatorChat)
            currentObservation = evalQuery.observation
            currentQuery = evalQuery.fixedQuery
            currentUserPrompt = evalQuery.modifiedUserPrompt

            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}LLM Judgement : {evalQuery.isValid}")
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}LLM Observation : {evalQuery.observation}")
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Fixed Query : {evalQuery.fixedQuery}")
            self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Modified Prompt : {evalQuery.modifiedUserPrompt}")

            validatorChat.append(AIMessage(content=f"""
            Query Evaluation
            ----------------
            Is Valid: {evalQuery.isValid}

            Observation
            -----------
            {evalQuery.observation}

            Fixed Query
            -----------
            {evalQuery.fixedQuery}

            Modified User Prompt
            --------------------
            {evalQuery.modifiedUserPrompt}
            """))

            if evalQuery.isValid:
                validator = True
                if len(retter) == 0:
                    logger.info(f"[ {chatID} | {self.agentName} # {retryCounter+1} ] : Query valid but returned empty results, retrying")
                    self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Query valid but empty results, retrying")
                    currentObservation = f"[ {currentQuery} ] executed successfully but returned empty results. Based on {currentObservation}"
                    validator = False
                else:
                    logger.info(f"[ {chatID} | {self.agentName} # {retryCounter+1} ] : Query validated successfully")
                    self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Query validated successfully")
            else:
                logger.info(f"[ {chatID} | {self.agentName} # {retryCounter+1} ] : Query validation failed, retrying")
                self.chatInstance.redisClient.publish(f"{chatID}", f"</SQLQueryEvaluator:QueryFixAttempt#{retryCounter+1}>{self.splitIdentifier}Query validation failed, retrying")
                currentObservation = f"[ {currentQuery} ] resulted in errors or empty data. Needs fixing. Based on {currentObservation}"
                validator = False

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

        if validator:
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
                    "currentQuery": currentQuery.replace("\n", " "),
                    "currentObservation": currentObservation,
                    "results": retter
                },
                "response": {
                    "currentQuery": currentQuery.replace("\n", " "),
                    "currentObservation": currentObservation,
                    "results": retter[:hardLimit]
                }
            }
        else:
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
                    "currentQuery": None,
                    "currentObservation": None,
                    "results": list()
                },
                "response": {
                    "currentQuery": None,
                    "currentObservation": None,
                    "results": list()
                }
            }


# %%
# Execution
if __name__ == "__main__":
    print("[ Query Evaluator ] : This module is intended to be imported, not run directly.")
