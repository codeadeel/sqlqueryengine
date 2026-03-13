#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import logging
import warnings
from pydantic import BaseModel, Field
from langchain_core.messages import HumanMessage, AIMessage
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
from .sqlGuidelines import postgreManualData
from .promptTemplates import postgreSchemaDescriptionPrompt, queryGeneratorPrompt

logger = logging.getLogger(__name__)

# %%
# Structured output schema for SQL generation — the LLM produces this
class AutomatedQuerySchema(BaseModel):
    """Schema for automated SQL query generation."""
    description: str = Field(..., description="A description of the query and what it retrieves.")
    query: str = Field(..., description="The SQL query to execute against the database.")

# %%
# Natural language to SQL generator
class QueryGenerator:
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
        Initialize the SQL query generator with LLM, database, and Redis parameters.

        Arguments:
        ----------
        llmParams : dict
            LLM connection parameters.
            model : str
                The model name to use for generation.
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
        self.queryGeneratorInstance = self.llm.with_structured_output(AutomatedQuerySchema)

        self.postgreDB = PostgresDB(**self.dbParams)
        self.chatInstance = SessionManager(self.redisParams, self.agentName)

    def process(self, chatID: str, schemaExamples: int, basePrompt: str) -> dict:
        """
        Generate a SQL query from a natural language prompt.

        On first call for a user, the method introspects the database schema,
        generates a human-readable schema description via the LLM (streamed to
        Redis Pub/Sub), and caches both the description and query context in Redis.
        Subsequent calls for the same user reuse the cached context.

        Arguments:
        ----------
        chatID : str
            Unique identifier for the user; used to namespace Redis keys.
        schemaExamples : int
            Number of sample rows per table to include in the schema context.
        basePrompt : str
            The natural language question or instruction to translate into SQL.

        Returns:
        --------
        dict
            A response dictionary containing:
            - code : int — HTTP-style status code (200 on success).
            - chatID : str — Echo of the input chatID.
            - schemaExamples : int — Echo of the input schemaExamples.
            - basePrompt : str — Echo of the input basePrompt.
            - data : dict — Full results including chat histories.
            - response : dict — Slim response with queryDescription and sqlQuery.
        """
        if not self.chatInstance.redisClient.exists(f"{chatID}:{self.agentName}"):
            logger.info(f"[ {chatID} | {self.agentName} @ Redis ] : Creating new user session")
            schemaDump, schemaParsed = self.postgreDB.getParsedSchemaDump(expLen=schemaExamples)

            # Generate a detailed schema description via the LLM
            logger.info(f"[ {chatID} | {self.agentName} ] : Generating database schema description")
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
                    f"</SQLQueryGenerator:schemaDescriptionChat>{self.splitIdentifier}{chunk.content}"
                )
            dbDescription = AIMessage(content=dbDescription)

            self.chatInstance.postUserChatContext(chatID, "dbSchemaDescription", schemaDescriptionChat + [dbDescription])
            logger.info(f"[ {chatID} | {self.agentName} ] : Schema description generated and cached")

            # Build the query generator context with the schema description
            logger.info(f"[ {chatID} | {self.agentName} ] : Building query generator context")
            chatParser = ChatPromptTemplate.from_messages([queryGeneratorPrompt]).format_messages(
                botName=self.botName,
                botGoal=f"You are a helpful SQL assistant named {self.botName} that helps users build PostgreSQL queries based on their requirements. Use the provided database schema and context. Ensure all queries are syntactically correct, read-only, and do not delete data. Output LIMIT <= 100.",
                dataDescription=dbDescription.content,
                dataContext=schemaParsed,
                postgreManual=postgreManualData
            )

            logger.info(f"[ {chatID} | {self.agentName} @ Redis ] : Storing query generator context")
            self.chatInstance.postUserChatContext(chatID, "dbQueryGenerator", chatParser)
        else:
            logger.info(f"[ {chatID} | {self.agentName} @ Redis ] : Loading existing query generator context")
            chatParser, _ = self.chatInstance.getUserChatContext(chatID, "dbQueryGenerator")

        # Generate the SQL query from the user prompt
        schemaDump, schemaParsed = self.postgreDB.getParsedSchemaDump(expLen=schemaExamples)
        logger.info(f"[ {chatID} | {self.agentName} ] : Generating SQL query for prompt")
        resp = self.queryGeneratorInstance.invoke(chatParser + [HumanMessage(content=f"{basePrompt}")])
        chatParser.append(HumanMessage(content=basePrompt))
        self.chatInstance.postUserChatContext(chatID, "dbQueryGenerator", chatParser + [AIMessage(content=str(resp))])

        # Fetch updated histories for the response payload
        _, dbSchemaDescriptionHistory = self.chatInstance.getUserChatContext(chatID, "dbSchemaDescription")
        _, dbQueryGeneratorHistory = self.chatInstance.getUserChatContext(chatID, "dbQueryGenerator")

        return {
            "code": 200,
            "chatID": chatID,
            "schemaExamples": schemaExamples,
            "basePrompt": basePrompt,
            "data": {
                "queryDescription": resp.description,
                "sqlQuery": resp.query.replace("\n", " "),
                "dbSchemaDescriptionHistory": dbSchemaDescriptionHistory,
                "dbQueryGeneratorHistory": dbQueryGeneratorHistory
            },
            "response": {
                "queryDescription": resp.description,
                "sqlQuery": resp.query.replace("\n", " ")
            }
        }


# %%
# Execution
if __name__ == "__main__":
    print("[ Query Generator ] : This module is intended to be imported, not run directly.")
