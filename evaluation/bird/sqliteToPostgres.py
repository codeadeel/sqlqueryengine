#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import logging
import os
import re
import sqlite3

import psycopg
from psycopg import sql as pgsql

from birdConfig import (
    BIRD_DATA_DIR,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD,
    adminConnect, dbConnect,
)

logger = logging.getLogger(__name__)

# Batch size for bulk INSERT operations
_INSERT_BATCH = 1000


# %%
# SQLite type affinity → PostgreSQL type mapping
def _pgType(sqliteType: str) -> str:
    """
    Map a SQLite declared column type to a PostgreSQL type using
    SQLite's type affinity rules.

    Arguments:
    ----------
    sqliteType : str
        The raw declared type string from ``PRAGMA table_info``.

    Returns:
    --------
    str
        PostgreSQL type name.
    """
    t = sqliteType.upper().strip()

    # Strip parenthesised precision/scale for matching (e.g. VARCHAR(255))
    t_bare = re.sub(r'\([^)]*\)', '', t).strip()

    if "INT" in t_bare:
        return "BIGINT"           # Use BIGINT — safer than INTEGER for large IDs
    if "CHAR" in t_bare or "CLOB" in t_bare or "TEXT" in t_bare:
        return "TEXT"
    if "REAL" in t_bare or "FLOA" in t_bare or "DOUB" in t_bare:
        return "DOUBLE PRECISION"
    if "BLOB" in t_bare or t_bare == "":
        return "TEXT"             # BLOB → TEXT (simpler than BYTEA)
    if "NUMERIC" in t_bare or "DECIMAL" in t_bare or "NUMBER" in t_bare:
        return "NUMERIC"
    if "BOOL" in t_bare:
        return "BIGINT"           # SQLite stores booleans as 0/1
    if "DATE" in t_bare and "TIME" not in t_bare:
        return "TEXT"             # Keep as TEXT to avoid parse errors on messy BIRD data
    if "TIME" in t_bare:
        return "TEXT"             # Same reasoning
    return "TEXT"                 # Catch-all


# %%
# Value coercion — SQLite Python values → PostgreSQL-safe values
def _coerceValue(v, pgType: str):
    """
    Coerce a SQLite Python value to be safe for the target PostgreSQL type.

    Arguments:
    ----------
    v : any
        Value from sqlite3 cursor (may be int, float, str, bytes, None).
    pgType : str
        Target PostgreSQL type (e.g. ``TEXT``, ``BIGINT``).

    Returns:
    --------
    any
        Value safe to pass as a psycopg parameter.
    """
    if v is None:
        return None
    if pgType == "TEXT":
        if isinstance(v, bytes):
            try:
                return v.decode("utf-8")
            except UnicodeDecodeError:
                return v.decode("latin-1", errors="replace")
        return str(v)
    if pgType == "BIGINT":
        try:
            return int(v)
        except (ValueError, TypeError):
            return None
    if pgType == "DOUBLE PRECISION":
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
    if pgType == "NUMERIC":
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
    # Fallback: stringify
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return v.decode("latin-1", errors="replace")
    return str(v)


# %%
# SQLite schema introspection
def _introspectSQLite(sqliteConn: sqlite3.Connection, dbId: str) -> list:
    """
    Introspect all user tables from a SQLite database.

    Arguments:
    ----------
    sqliteConn : sqlite3.Connection
        Open connection to the source SQLite file.
    dbId : str
        Database identifier (used only for log messages).

    Returns:
    --------
    list[dict]
        One dict per table with keys: tableName, columns (list[dict]),
        fkConstraints (list[dict]).
        Column dict keys: name, pgType, notNull, pk, defaultVal, sqliteType.
        FK dict keys: col, refTable, refCol.
    """
    cur = sqliteConn.cursor()

    # All user tables, excluding SQLite internal tables
    cur.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    )
    tableNames = [row[0] for row in cur.fetchall()]

    tables = []
    for tableName in tableNames:
        # PRAGMA table_info: (cid, name, type, notnull, dflt_value, pk)
        cur.execute(f'PRAGMA table_info("{tableName}")')
        colRows = cur.fetchall()

        columns = []
        for row in colRows:
            columns.append({
                # Lowercase all identifiers so unquoted SQL references work in PG.
                # PostgreSQL folds unquoted identifiers to lowercase, so if we create
                # tables/columns with their original mixed-case and double-quote them,
                # gold SQL like "FROM Patient AS T1" would fail (PG looks for "patient").
                # Also replace % with pct to avoid psycopg format-specifier conflicts.
                "name":       row[1].lower().replace("%", "pct"),
                "origName":   row[1],             # preserve for SQLite SELECT
                "sqliteType": row[2] or "",
                "pgType":     _pgType(row[2] or ""),
                "notNull":    bool(row[3]),
                "pk":         int(row[5]),      # 0 = not PK, 1/2/... = PK order
                "defaultVal": row[4],
            })

        # PRAGMA foreign_key_list: (id, seq, table, from, to, on_update, on_delete, match)
        cur.execute(f'PRAGMA foreign_key_list("{tableName}")')
        fkRows = cur.fetchall()
        fkConstraints = []
        for fk in fkRows:
            # fk[4] (refCol) can be None when the FK implicitly references
            # the primary key of the target table. Skip such FKs — they
            # cannot be expressed as explicit FK constraints in PostgreSQL
            # without resolving the target PK, and they are non-critical
            # for BIRD evaluation purposes.
            if fk[3] is None or fk[4] is None:
                continue
            fkConstraints.append({
                "col":      fk[3].lower(),
                "refTable": fk[2].lower(),
                "refCol":   fk[4].lower(),
            })

        tables.append({
            "tableName":     tableName.lower(),
            "origTableName": tableName,           # preserve for SQLite SELECT
            "columns":       columns,
            "fkConstraints": fkConstraints,
        })

    logger.info("  Introspected %d tables in %s", len(tables), dbId)
    return tables


# %%
# Topological sort — creates tables in FK dependency order
def _topoSort(tables: list) -> list:
    """
    Sort tables topologically by foreign-key dependency so that
    referenced tables are created before referencing tables.

    Arguments:
    ----------
    tables : list[dict]
        Output from ``_introspectSQLite``.

    Returns:
    --------
    list[dict]
        Tables in creation order (dependencies first).
        Falls back to original order if a cycle is detected.
    """
    nameToTable = {t["tableName"]: t for t in tables}
    visited = set()
    result = []

    def visit(name: str, stack: set):
        if name in visited:
            return
        if name in stack:
            # Cycle detected — skip to avoid infinite recursion
            return
        stack.add(name)
        table = nameToTable.get(name)
        if table:
            for fk in table["fkConstraints"]:
                if fk["refTable"] != name:  # skip self-references
                    visit(fk["refTable"], stack)
        stack.discard(name)
        visited.add(name)
        if name in nameToTable:
            result.append(nameToTable[name])

    hasCycle = False
    for t in tables:
        try:
            visit(t["tableName"], set())
        except RecursionError:
            hasCycle = True
            break

    if hasCycle or len(result) != len(tables):
        logger.warning("FK cycle detected — using original table order")
        return tables

    return result


# %%
# DDL generation — SQLite schema → PostgreSQL CREATE TABLE
def _buildCreateTable(tableName: str, columns: list, fkConstraints: list, dropFKs: bool = False) -> str:
    """
    Generate a ``CREATE TABLE IF NOT EXISTS`` DDL statement.

    Arguments:
    ----------
    tableName : str
        The table name.
    columns : list[dict]
        Column dicts from ``_introspectSQLite``.
    fkConstraints : list[dict]
        FK constraint dicts from ``_introspectSQLite``.
    dropFKs : bool
        If True, omit FK constraints (used when cycle workaround is needed).

    Returns:
    --------
    str
        Complete DDL string.
    """
    pkCols = sorted([c for c in columns if c["pk"] > 0], key=lambda c: c["pk"])
    compositePK = len(pkCols) > 1
    singlePK    = len(pkCols) == 1

    colDefs = []
    for col in columns:
        name = f'"{col["name"]}"'

        # Single INTEGER PRIMARY KEY → BIGSERIAL (SQLite autoincrement equivalent)
        if singlePK and col["pk"] == 1 and col["pgType"] == "BIGINT":
            pgType = "BIGSERIAL"
        else:
            pgType = col["pgType"]

        parts = [name, pgType]

        if singlePK and col["pk"] == 1:
            parts.append("PRIMARY KEY")
        elif not compositePK and col["notNull"]:
            parts.append("NOT NULL")

        colDefs.append("    " + " ".join(parts))

    # Composite primary key constraint
    if compositePK:
        pkNames = ", ".join(f'"{c["name"]}"' for c in pkCols)
        colDefs.append(f"    PRIMARY KEY ({pkNames})")

    # FK constraints
    if not dropFKs:
        for fk in fkConstraints:
            constraintName = f'fk_{tableName}_{fk["col"]}'[:63]  # PG identifier limit
            colDefs.append(
                f'    CONSTRAINT "{constraintName}" '
                f'FOREIGN KEY ("{fk["col"]}") '
                f'REFERENCES "{fk["refTable"]}" ("{fk["refCol"]}")'
            )

    body = ",\n".join(colDefs)
    return f'CREATE TABLE IF NOT EXISTS "{tableName}" (\n{body}\n);'


# %%
# Full database migration: SQLite → PostgreSQL
def migrateDatabase(dbId: str, dataDir: str) -> bool:
    """
    Migrate a single BIRD SQLite database into PostgreSQL.

    Creates the ``bird_{db_id}`` PostgreSQL database if it does not
    exist, then creates all tables in FK-dependency order and
    bulk-inserts all rows.

    Arguments:
    ----------
    dbId : str
        The BIRD database identifier (e.g. ``concert_singer``).
    dataDir : str
        Root of the bird_data volume mount.

    Returns:
    --------
    bool
        True if migration succeeded, False on error.
    """
    sqlitePath = os.path.join(dataDir, "dev_databases", dbId, f"{dbId}.sqlite")
    if not os.path.isfile(sqlitePath):
        logger.error("SQLite file not found: %s", sqlitePath)
        return False

    pgName = f"bird_{dbId}"
    logger.info("=" * 60)
    logger.info("Migrating: %s → PostgreSQL database %s", dbId, pgName)

    try:
        # Create PostgreSQL database if it doesn't exist
        adminConn = adminConnect()
        cur = adminConn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (pgName,))
        if not cur.fetchone():
            cur.execute(f'CREATE DATABASE "{pgName}"')
            logger.info("  Created PostgreSQL database: %s", pgName)
        else:
            logger.info("  Database %s already exists — re-migrating", pgName)
        adminConn.close()

        # Open SQLite source
        sqliteConn = sqlite3.connect(sqlitePath)
        sqliteConn.row_factory = None  # Return plain tuples

        tables = _introspectSQLite(sqliteConn, dbId)
        sortedTables = _topoSort(tables)

        # Open PostgreSQL target
        pgConn = dbConnect(pgName)
        pgCur  = pgConn.cursor()

        # Drop and recreate tables in FK dependency order
        # Drop in reverse order to respect FK constraints
        for table in reversed(sortedTables):
            pgCur.execute(f'DROP TABLE IF EXISTS "{table["tableName"]}" CASCADE')

        for table in sortedTables:
            ddl = _buildCreateTable(
                table["tableName"], table["columns"], table["fkConstraints"]
            )
            try:
                pgCur.execute(ddl)
            except Exception as e:
                # FK constraint may fail if referenced table was not created yet
                # (cycle or missing table) — retry without FKs
                logger.warning(
                    "  DDL failed for %s (will retry without FKs): %s",
                    table["tableName"], str(e)[:80],
                )
                pgCur.execute(f'DROP TABLE IF EXISTS "{table["tableName"]}" CASCADE')
                ddl_nofk = _buildCreateTable(
                    table["tableName"], table["columns"], [], dropFKs=True
                )
                pgCur.execute(ddl_nofk)

        # Bulk-insert data for each table
        for table in sortedTables:
            tableName     = table["tableName"]        # lowercase (for PG)
            origTableName = table["origTableName"]    # original case (for SQLite)
            columns       = table["columns"]
            colNames      = [c["name"] for c in columns]       # lowercase
            origColNames  = [c["origName"] for c in columns]   # original case
            pgTypes       = [c["pgType"] for c in columns]

            sqliteCur = sqliteConn.cursor()
            # Use original-case names for the SQLite SELECT
            sqliteQuotedCols = ", ".join(f'"{n}"' for n in origColNames)
            sqliteCur.execute(f'SELECT {sqliteQuotedCols} FROM "{origTableName}"')

            # Use psycopg.sql to safely handle identifiers that contain
            # special characters like % or () (common in BIRD column names).
            insertSQL = pgsql.SQL("INSERT INTO {tbl} ({cols}) VALUES ({phs})").format(
                tbl=pgsql.Identifier(tableName),
                cols=pgsql.SQL(", ").join(pgsql.Identifier(n) for n in colNames),
                phs=pgsql.SQL(", ").join(pgsql.Placeholder() for _ in colNames),
            )

            rowCount  = 0
            batch     = []
            insertOK  = True

            for row in sqliteCur:
                coerced = tuple(_coerceValue(v, pgTypes[i]) for i, v in enumerate(row))
                batch.append(coerced)
                if len(batch) >= _INSERT_BATCH:
                    try:
                        pgCur.executemany(insertSQL, batch)
                    except Exception as e:
                        # Retry with nullable columns if NOT NULL violation
                        logger.warning(
                            "  Insert failed for %s (retrying nullable): %s",
                            tableName, str(e)[:80],
                        )
                        pgCur.execute(f'DROP TABLE IF EXISTS "{tableName}" CASCADE')
                        ddl_nullable = _buildCreateTable(tableName, [
                            {**c, "notNull": False} for c in columns
                        ], [], dropFKs=True)
                        pgCur.execute(ddl_nullable)
                        pgCur.executemany(insertSQL, batch)
                        insertOK = False  # note: FKs already dropped in this path
                    rowCount += len(batch)
                    batch = []

            if batch:
                try:
                    pgCur.executemany(insertSQL, batch)
                except Exception as e:
                    logger.warning(
                        "  Final batch insert failed for %s: %s",
                        tableName, str(e)[:80],
                    )
                rowCount += len(batch)

            logger.info("  Inserted %d rows into %s", rowCount, tableName)

            # Reset BIGSERIAL sequences after bulk insert
            for col in columns:
                if col["pk"] == 1 and col["pgType"] == "BIGINT" and len([c for c in columns if c["pk"] > 0]) == 1:
                    try:
                        pgCur.execute(
                            f"SELECT setval("
                            f"pg_get_serial_sequence('\"{tableName}\"', %s), "
                            f"COALESCE(MAX(\"{col['name']}\"), 1)) "
                            f"FROM \"{tableName}\"",
                            (col["name"],),
                        )
                    except Exception:
                        pass  # sequence reset is best-effort

        sqliteConn.close()
        pgConn.close()
        logger.info("Migration complete: %s", pgName)
        return True

    except Exception as e:
        logger.error("Migration failed for %s: %s", dbId, str(e)[:200])
        return False


# %%
# Batch migration — migrate all databases referenced in the question set
def migrateAll(dbIds: list, dataDir: str) -> dict:
    """
    Migrate all BIRD databases from SQLite to PostgreSQL.

    Arguments:
    ----------
    dbIds : list[str]
        List of unique BIRD db_id values to migrate.
    dataDir : str
        Root of the bird_data volume mount.

    Returns:
    --------
    dict[str, bool]
        Mapping of db_id → migration success flag.
    """
    results = {}
    for i, dbId in enumerate(sorted(dbIds), start=1):
        logger.info("[%d/%d] Migrating %s", i, len(dbIds), dbId)
        results[dbId] = migrateDatabase(dbId, dataDir)

    succeeded = sum(1 for v in results.values() if v)
    logger.info("Migration summary: %d/%d succeeded", succeeded, len(dbIds))
    if succeeded < len(dbIds):
        failed = [k for k, v in results.items() if not v]
        logger.warning("Failed migrations: %s", failed)

    return results


# %%
# Execute gold SQL against PostgreSQL to capture gold results
def executeGoldSQL(questions: list) -> list:
    """
    Execute the PostgreSQL-converted gold SQL for every question and
    store the results in-place.

    Questions whose converted gold SQL fails to execute are marked with
    ``gold_conversion_error: True`` and excluded from accuracy scoring.

    Arguments:
    ----------
    questions : list[dict]
        Internal question records with ``gold_query_pg`` populated.
        Modified in-place.

    Returns:
    --------
    list[dict]
        The same list with ``gold_result`` and ``gold_conversion_error``
        fields populated.
    """
    # Group questions by database to reuse connections
    from collections import defaultdict
    byDB = defaultdict(list)
    for q in questions:
        byDB[q["database"]].append(q)

    totalOK  = 0
    totalErr = 0

    for pgName, dbQuestions in sorted(byDB.items()):
        try:
            conn = dbConnect(pgName)
        except Exception as e:
            logger.error(
                "Cannot connect to %s — marking %d questions as conversion errors: %s",
                pgName, len(dbQuestions), str(e)[:80],
            )
            for q in dbQuestions:
                q["gold_conversion_error"] = True
                q["gold_result"] = None
            totalErr += len(dbQuestions)
            continue

        dbOK = dbErr = 0
        cur = conn.cursor()

        # Set a per-query timeout to avoid hanging on complex gold queries
        try:
            cur.execute("SET statement_timeout = '30s'")
        except Exception:
            pass

        for q in dbQuestions:
            try:
                cur.execute(q["gold_query_pg"])
                q["gold_result"] = cur.fetchall()  # list of tuples
                dbOK += 1
            except Exception as e:
                q["gold_conversion_error"] = True
                q["gold_result"] = None
                dbErr += 1
                logger.warning(
                    "Gold SQL failed for #%d (%s): %s",
                    q["id"], pgName, str(e)[:120],
                )
                # Re-issue timeout reset in case the error left the connection in a bad state
                try:
                    conn.rollback()
                    cur.execute("SET statement_timeout = '30s'")
                except Exception:
                    pass

        conn.close()
        logger.info(
            "Gold SQL for %s: %d OK, %d errors", pgName, dbOK, dbErr
        )
        totalOK  += dbOK
        totalErr += dbErr

    logger.info(
        "Gold SQL total: %d OK, %d errors (%.1f%% error rate)",
        totalOK, totalErr,
        totalErr / (totalOK + totalErr) * 100 if (totalOK + totalErr) > 0 else 0,
    )
    return questions
