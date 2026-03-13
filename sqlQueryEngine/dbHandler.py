#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import logging
import psycopg

logger = logging.getLogger(__name__)

# %%
# PostgreSQL database handler — provides read-only access to schema introspection
# and safe query execution against any PostgreSQL database
class PostgresDB:
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str) -> None:
        """
        Initialize the database connection.

        Arguments:
        ----------
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
        """
        self.conn = psycopg.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        self.conn.set_read_only(True)
        self.cur = self.conn.cursor()

    def listTables(self) -> list:
        """
        Retrieve a list of all tables in the public schema.

        Returns:
        --------
        list
            A list of table name strings.
        """
        self.cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        return [row[0] for row in self.cur.fetchall()]

    def getTableSchema(self, table_name: str) -> list:
        """
        Retrieve the schema of a specific table.

        Arguments:
        ----------
        table_name : str
            Name of the table to retrieve the schema for.

        Returns:
        --------
        list
            A list of (column_name, data_type) tuples.
        """
        self.cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """)
        return self.cur.fetchall()

    def getFullTableDump(self, table_name: str) -> list:
        """
        Retrieve all records from a specific table.

        Arguments:
        ----------
        table_name : str
            Name of the table to retrieve records from.

        Returns:
        --------
        list
            A list of row tuples.
        """
        self.cur.execute(f"SELECT * FROM {table_name}")
        return self.cur.fetchall()

    def getSchemaDump(self, expLen: int = 5) -> dict:
        """
        Retrieve the schema and sample data for all tables in the database.

        Arguments:
        ----------
        expLen : int
            Number of sample records to retrieve from each table.
            Default : 5

        Returns:
        --------
        dict
            A dictionary keyed by table name, each containing schema and sampleData lists.
        """
        schemaDump = dict()
        tables = self.listTables()
        for table in tables:
            schemaDump[table] = {
                "schema": self.getTableSchema(table),
                "sampleData": self.getFullTableDump(table)[:expLen]
            }
        return schemaDump

    def getParsedSchemaDump(self, expLen: int = 5) -> tuple:
        """
        Retrieve the schema and sample data for all tables, parsed into a human-readable string.

        Arguments:
        ----------
        expLen : int
            Number of sample records to retrieve from each table.
            Default : 5

        Returns:
        --------
        dict
            Raw schema dump as a dictionary.
        str
            Parsed schema dump as a formatted string ready for use in LLM prompts.
        """
        schemaDump = self.getSchemaDump(expLen=expLen)
        parsedDump = ""
        for table in list(schemaDump.keys()):
            parsedDump += f"[ Table Schema Format ] : {table}\n---------------\n"
            for i in schemaDump[table]['schema']:
                parsedDump += f"{i[0]} : {i[1]}\n"
            if expLen > 0:
                parsedDump += f"\n\n[ Table Sample Data ] : First {expLen} Rows @ {table}\n"
                for row in schemaDump[table]['sampleData']:
                    parsedDump += f"{row}\n"
            parsedDump += "\n"
        return schemaDump, str(parsedDump)

    def queryExecutor(self, query: str) -> list:
        """
        Execute a SQL query and return results as a list of dictionaries.

        Arguments:
        ----------
        query : str
            The SQL query to execute.

        Returns:
        --------
        list
            A list of row dictionaries with column names as keys.
        """
        self.cur.execute(query)
        res = self.cur.fetchall()
        rows = list()
        cols = [desc.name for desc in self.cur.description]
        for row in res:
            rowDict = dict()
            for idx, col in enumerate(cols):
                rowDict[col] = str(row[idx])
            rows.append(rowDict)
        return rows

    def close(self) -> None:
        """
        Close the database cursor and connection.
        """
        self.cur.close()
        self.conn.close()


# %%
# Execution
if __name__ == "__main__":
    print("[ DB Handler ] : This module is intended to be imported, not run directly.")
