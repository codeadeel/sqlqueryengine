#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import logging
from .queryGenerator import QueryGenerator
from .queryEvaluator import QueryEvaluator

logger = logging.getLogger(__name__)

# %%
# SQLQueryEngine — unified two-stage pipeline for natural language to SQL
class SQLQueryEngine:
    def __init__(
        self,
        llmParams: dict,
        dbParams: dict,
        redisParams: dict,
        botName: str = "SQLBot",
        splitIdentifier: str = "<|-/|-/>"
    ) -> None:
        """
        Initialize the SQL Query Engine with connection parameters.

        Instantiate this class directly to use the engine as a Python module,
        bypassing the FastAPI layer entirely.

        Arguments:
        ----------
        llmParams : dict
            LLM connection parameters.
            model : str
                The model name (e.g. "qwen2.5-coder:7b").
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
        splitIdentifier : str
            Delimiter used in Redis Pub/Sub messages to separate the event tag from content.
            Default : <|-/|-/>

        Example usage (module mode):
        -----------------------------
        from sqlQueryEngine import SQLQueryEngine

        engine = SQLQueryEngine(
            llmParams={
                "model": "qwen2.5-coder:7b",
                "temperature": 0.1,
                "base_url": "http://localhost:11434/v1",
                "api_key": "ollama"
            },
            dbParams={
                "host": "localhost",
                "port": 5432,
                "dbname": "mydb",
                "user": "postgres",
                "password": "secret"
            },
            redisParams={
                "host": "localhost",
                "port": 6379,
                "password": "",
                "db": 0,
                "decode_responses": True
            }
        )

        result = engine.run(
            chatID="user123",
            basePrompt="How many orders were placed last month?"
        )
        print(result["generation"]["sqlQuery"])
        print(result["evaluation"]["results"])
        """
        self.llmParams = llmParams
        self.dbParams = dbParams
        self.redisParams = redisParams
        self.botName = botName
        self.splitIdentifier = splitIdentifier

    def run(
        self,
        chatID: str,
        basePrompt: str,
        retryCount: int = 5,
        schemaExamples: int = 5,
        feedbackExamples: int = 3,
        schemaDescriptionKey: str = "dbSchemaDescription"
    ) -> dict:
        """
        Run the full NL-to-SQL pipeline for a given prompt.

        Executes Stage 1 (SQL generation) followed by Stage 2 (SQL evaluation
        and iterative repair) and returns combined results.

        Arguments:
        ----------
        chatID : str
            Unique identifier for the user; used to namespace Redis keys and
            Pub/Sub channels. Reusing the same chatID across calls shares the
            cached schema context.
        basePrompt : str
            Natural language question or instruction to translate into SQL.
        retryCount : int
            Maximum LLM-driven repair attempts before returning a failure response.
            Default : 5
        schemaExamples : int
            Number of sample rows per table to include in schema context.
            Default : 5
        feedbackExamples : int
            Number of result rows to feed back to the LLM during evaluation.
            Default : 3
        schemaDescriptionKey : str
            Redis hash field key for the cached schema description.
            Default : dbSchemaDescription

        Returns:
        --------
        dict
            code : int
                200 on success, 500 on stage failure.
            chatID : str
                Echo of the input chatID.
            generation : dict
                queryDescription : str — Human-readable description of the query.
                sqlQuery : str — The generated SQL query.
            evaluation : dict
                currentQuery : str or None — The final executed query, None on failure.
                currentObservation : str or None — LLM observation on the final query.
                results : list — Query result rows (up to hardLimit).
            error : str
                Present only on failure; contains the exception message.
        """
        logger.info(f"[ {chatID} | SQL Query Engine ] : Starting inference")

        # Stage 1 — SQL generation
        try:
            genInst = QueryGenerator(
                self.llmParams,
                self.dbParams,
                self.redisParams,
                botName=self.botName,
                splitIdentifier=self.splitIdentifier
            )
            respGen = genInst.process(
                chatID=chatID,
                schemaExamples=schemaExamples,
                basePrompt=basePrompt
            )
        except Exception as e:
            logger.error(f"[ {chatID} | SQL Query Engine ] : SQL generation failed. Error: {str(e)}", exc_info=True)
            return {
                "code": 500,
                "chatID": chatID,
                "error": f"SQL generation failed: {str(e)}"
            }

        # Stage 2 — SQL evaluation and repair
        try:
            evalInst = QueryEvaluator(
                self.llmParams,
                self.dbParams,
                self.redisParams,
                botName=self.botName,
                splitIdentifier=self.splitIdentifier
            )
            respEval = evalInst.process(
                chatID=chatID,
                basePrompt=basePrompt,
                baseQuery=respGen['data']['sqlQuery'],
                baseDescription=respGen['data']['queryDescription'],
                retryCount=retryCount,
                generatorContextKey=schemaDescriptionKey,
                schemaExamples=schemaExamples,
                feedbackExamples=feedbackExamples,
                localPayload=respGen
            )
        except Exception as e:
            logger.error(f"[ {chatID} | SQL Query Engine ] : SQL evaluation failed. Error: {str(e)}", exc_info=True)
            return {
                "code": 500,
                "chatID": chatID,
                "error": f"SQL evaluation failed: {str(e)}"
            }

        logger.info(f"[ {chatID} | SQL Query Engine ] : Inference completed successfully")
        return {
            "code": 200,
            "chatID": chatID,
            "generation": respGen['response'],
            "evaluation": respEval['response']
        }

    def generate(
        self,
        chatID: str,
        basePrompt: str,
        schemaExamples: int = 5,
        schemaDescriptionKey: str = "dbSchemaDescription"
    ) -> dict:
        """
        Run Stage 1 only — generate a SQL query from a natural language prompt.

        Introspects the database schema (cached in Redis per chatID) and produces
        an initial SQL query without executing or evaluating it.

        Arguments:
        ----------
        chatID : str
            Unique identifier for the user; namespaces Redis keys and Pub/Sub channels.
        basePrompt : str
            Natural language question or instruction to translate into SQL.
        schemaExamples : int
            Number of sample rows per table to include in schema context.
            Default : 5
        schemaDescriptionKey : str
            Redis hash field key for the cached schema description.
            Default : dbSchemaDescription

        Returns:
        --------
        dict
            code : int
                200 on success, 500 on failure.
            chatID : str
                Echo of the input chatID.
            generation : dict
                queryDescription : str — Human-readable description of the query.
                sqlQuery : str — The generated SQL query.
            error : str
                Present only on failure; contains the exception message.
        """
        logger.info(f"[ {chatID} | SQL Query Engine ] : Starting SQL generation")

        try:
            genInst = QueryGenerator(
                self.llmParams,
                self.dbParams,
                self.redisParams,
                botName=self.botName,
                splitIdentifier=self.splitIdentifier
            )
            respGen = genInst.process(
                chatID=chatID,
                schemaExamples=schemaExamples,
                basePrompt=basePrompt
            )
        except Exception as e:
            logger.error(f"[ {chatID} | SQL Query Engine ] : SQL generation failed. Error: {str(e)}", exc_info=True)
            return {
                "code": 500,
                "chatID": chatID,
                "error": f"SQL generation failed: {str(e)}"
            }

        logger.info(f"[ {chatID} | SQL Query Engine ] : SQL generation completed successfully")
        return {
            "code": 200,
            "chatID": chatID,
            "generation": respGen['response']
        }

    def evaluate(
        self,
        chatID: str,
        basePrompt: str,
        baseQuery: str,
        baseDescription: str,
        retryCount: int = 5,
        schemaExamples: int = 5,
        feedbackExamples: int = 3,
        schemaDescriptionKey: str = "dbSchemaDescription"
    ) -> dict:
        """
        Run Stage 2 only — execute a SQL query and iteratively repair it if needed.

        Takes an existing SQL query and runs it against PostgreSQL. On failure or
        empty results the LLM repairs it up to retryCount times, publishing
        progress events to the Redis Pub/Sub channel keyed by chatID.

        Arguments:
        ----------
        chatID : str
            Unique identifier for the user; namespaces Redis keys and Pub/Sub channels.
        basePrompt : str
            Original natural language question (used as context for the LLM repair loop).
        baseQuery : str
            SQL query to execute and evaluate.
        baseDescription : str
            Human-readable description of what the query is intended to do.
        retryCount : int
            Maximum LLM-driven repair attempts before returning a failure response.
            Default : 5
        schemaExamples : int
            Number of sample rows per table to include in schema context.
            Default : 5
        feedbackExamples : int
            Number of result rows to feed back to the LLM during evaluation.
            Default : 3
        schemaDescriptionKey : str
            Redis hash field key for the cached schema description.
            Default : dbSchemaDescription

        Returns:
        --------
        dict
            code : int
                200 on success, 500 on failure.
            chatID : str
                Echo of the input chatID.
            evaluation : dict
                currentQuery : str or None — The final executed query, None on failure.
                currentObservation : str or None — LLM observation on the final query.
                results : list — Query result rows (up to hardLimit).
            error : str
                Present only on failure; contains the exception message.
        """
        logger.info(f"[ {chatID} | SQL Query Engine ] : Starting SQL evaluation")

        try:
            evalInst = QueryEvaluator(
                self.llmParams,
                self.dbParams,
                self.redisParams,
                botName=self.botName,
                splitIdentifier=self.splitIdentifier
            )
            respEval = evalInst.process(
                chatID=chatID,
                basePrompt=basePrompt,
                baseQuery=baseQuery,
                baseDescription=baseDescription,
                retryCount=retryCount,
                generatorContextKey=schemaDescriptionKey,
                schemaExamples=schemaExamples,
                feedbackExamples=feedbackExamples,
                localPayload=None
            )
        except Exception as e:
            logger.error(f"[ {chatID} | SQL Query Engine ] : SQL evaluation failed. Error: {str(e)}", exc_info=True)
            return {
                "code": 500,
                "chatID": chatID,
                "error": f"SQL evaluation failed: {str(e)}"
            }

        logger.info(f"[ {chatID} | SQL Query Engine ] : SQL evaluation completed successfully")
        return {
            "code": 200,
            "chatID": chatID,
            "evaluation": respEval['response']
        }


# %%
# Execution
if __name__ == "__main__":
    print("[ SQL Query Engine ] : This module is intended to be imported, not run directly.")
