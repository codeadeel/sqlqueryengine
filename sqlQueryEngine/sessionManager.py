#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import json
import logging
import redis
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage

logger = logging.getLogger(__name__)

# %%
# Redis-backed session manager — stores and retrieves per-user conversation
# context, enabling multi-turn SQL generation and evaluation workflows
class SessionManager:
    def __init__(self, redisParams: dict, agentName: str = "SQLQueryEngine") -> None:
        """
        Initialize the session manager with Redis connection parameters.

        Arguments:
        ----------
        redisParams : dict
            A dictionary containing Redis connection parameters.
            host : str
                The Redis server host.
            port : int
                The Redis server port.
            password : str
                The password for Redis authentication.
            db : int
                The Redis database number to connect to.
            decode_responses : bool
                Flag to decode responses as strings.

        agentName : str
            Namespace prefix for all Redis keys belonging to this agent.
            Default : SQLQueryEngine
        """
        self.redisParams = redisParams
        self.agentName = agentName
        self.redisClient = redis.Redis(**self.redisParams)

    def getRawUserData(self, chatID: str, retrievalKey: str) -> str:
        """
        Retrieve raw user data from Redis for a given user and key.

        Arguments:
        ----------
        chatID : str
            The unique identifier for the user.
        retrievalKey : str
            The hash field key for the specific data.

        Returns:
        --------
        str
            Raw string data stored at the given key.
        """
        rawData = self.redisClient.hgetall(f"{chatID}:{self.agentName}")
        return rawData[retrievalKey]

    def postRawUserData(self, chatID: str, retrievalKey: str, data: dict) -> None:
        """
        Store raw user data in Redis.

        Arguments:
        ----------
        chatID : str
            The unique identifier for the user.
        retrievalKey : str
            The hash field key to store data under.
        data : dict
            The data to serialize and store.

        Returns:
        --------
        None
        """
        self.redisClient.hset(f"{chatID}:{self.agentName}", retrievalKey, json.dumps(data))

    def getUserChatContext(self, chatID: str, retrievalKey: str) -> tuple:
        """
        Retrieve conversation history from Redis and parse into LangChain message objects.

        Arguments:
        ----------
        chatID : str
            The unique identifier for the user.
        retrievalKey : str
            The hash field key for the chat history.

        Returns:
        --------
        tuple
            A tuple containing:
            - list: LangChain message objects (SystemMessage, HumanMessage, AIMessage).
            - list: Raw JSON-serializable message dicts (role + content).
        """
        retData = self.redisClient.hgetall(f"{chatID}:{self.agentName}")
        chatParser = list()
        jsonDat = json.loads(retData[retrievalKey])
        for c in jsonDat:
            if c["role"] == "system":
                chatParser.append(SystemMessage(content=c["content"]))
            elif c["role"] == "user":
                chatParser.append(HumanMessage(content=c["content"]))
            elif c["role"] == "assistant":
                chatParser.append(AIMessage(content=c["content"]))
        return chatParser, jsonDat

    def postUserChatContext(self, chatID: str, retrievalKey: str, chatParser: list) -> None:
        """
        Serialize and store a conversation history in Redis.

        Arguments:
        ----------
        chatID : str
            The unique identifier for the user.
        retrievalKey : str
            The hash field key to store the chat history under.
        chatParser : list
            A list of LangChain message objects to serialize.

        Returns:
        --------
        None
        """
        parseBack = list()
        for c in chatParser:
            if isinstance(c, SystemMessage):
                parseBack.append({"role": "system", "content": c.content})
            elif isinstance(c, HumanMessage):
                parseBack.append({"role": "user", "content": c.content})
            elif isinstance(c, AIMessage):
                parseBack.append({"role": "assistant", "content": c.content})
        self.redisClient.hset(f"{chatID}:{self.agentName}", retrievalKey, json.dumps(parseBack))

    def updateUsageToken(self, chatID: str, currentCounter: int = -1, retrievalKey: str = "historyCounterManager") -> int:
        """
        Increment and return the user's evaluation history counter in Redis.

        Used to create uniquely keyed evaluation chat histories like
        validatorChat:1, validatorChat:2, etc., preventing overwrites across calls.

        Arguments:
        ----------
        chatID : str
            The unique identifier for the user.
        currentCounter : int
            A specific counter value to set. Pass -1 to auto-increment.
            Default : -1
        retrievalKey : str
            The hash field key for storing the counter value.
            Default : historyCounterManager

        Returns:
        --------
        int
            The updated counter value.
        """
        if currentCounter >= 0:
            countVal = currentCounter
        else:
            countRet = self.redisClient.hget(f"{chatID}:{self.agentName}", retrievalKey)
            if countRet is None:
                countVal = 0
            else:
                countVal = int(countRet)
            countVal += 1
        self.redisClient.hset(f"{chatID}:{self.agentName}", retrievalKey, str(countVal))
        return countVal


# %%
# Execution
if __name__ == "__main__":
    print("[ Session Manager ] : This module is intended to be imported, not run directly.")
