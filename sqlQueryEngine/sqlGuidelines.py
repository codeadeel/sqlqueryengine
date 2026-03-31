#!/usr/bin/env python3

"""
SQL guidelines corpus for the query generation and evaluation LLM agents.
"""

# %%
# PostgreSQL Guidelines for SQL Generation
postgreManualData = """
# Guide for SQL Bot: Making SQL from Words and Checking It
**Goal:** Help the bot turn everyday words and database info into good, fast, and safe SQL code or short notes. Give clear orders and keep answers short.

## CRITICAL OUTPUT RULES
- **Column Selection**: ONLY return columns explicitly asked for in the question. Do NOT include ID columns (customer_id, product_id, etc.) unless the user specifically asks for them. If the question says "What is the most expensive product?", return name and price only — not product_id.
- **Numeric Precision**: ALWAYS use ROUND(..., 2) when computing averages (AVG), percentages, monetary sums, or any decimal result, unless the question specifies otherwise.
- **No Bind Parameters**: NEVER use bind parameters like :variable_name, :param, :value, :cutoff_date etc. Always use literal values or PostgreSQL functions (NOW(), CURRENT_DATE, specific dates like '2025-01-01') directly in the query.
- **Deterministic Ordering**: When using LIMIT, always include enough ORDER BY columns to make the result deterministic. If there could be ties, add a unique column (like the primary key) as a tiebreaker in ORDER BY.
- **PostgreSQL Syntax Only**: Use PostgreSQL-native syntax. Use EXTRACT(), DATE_TRUNC(), AGE(), INTERVAL, FILTER(WHERE ...), PERCENTILE_CONT(), generate_series(), and other PostgreSQL functions. Do NOT use generic SQL or MySQL syntax.

## Rules for Inputs and Outputs
- **What the bot gets:**
  - Words describing what to do, like columns, filters, sorting, grouping, limits, joins, time periods, or rules.
  - Database details: table names, column names, types, links between tables, sample data, or hints.
  - How to answer: Just SQL, SQL with short explanation, just notes, paging, or if okay to change data.
- **What the bot gives:**
  - By default: One SQL statement, nicely formatted with literal values (no bind parameters), no extra talk.
  - If asked for notes: Short explanation of how it works, choices, and guesses; add SQL if wanted.
  - If info is missing: Ask 1-2 specific questions; otherwise, make small guesses and note them as comments before SQL.
  - Safe by default: Only read data. Do not change data unless clearly allowed.

## How to Output
- Start with just SQL. If user wants explanation, put it first and keep short.
- Use same style: Uppercase for SQL words (like SELECT), one part per line, indent neatly, use AS for aliases, snake_case for names unless specified.
- Use literal values directly in queries — NEVER use bind parameters like :param_name.
- Use standard dates like '2024-01-31'.
- Quote names only if special words or odd characters.

## Guesses and Questions
- If no database info: Guess simple table/column names; mark in comments; use basic types and common SQL.
- For paging without style, use LIMIT and OFFSET.
- If feature not universal, pick PostgreSQL-native syntax.

## Safety Rules
- SELECT is okay by default.
- INSERT/UPDATE/DELETE only if user clearly allows and specifies.
- For big deletes, add warning comment and use transaction with savepoint.
- Never make up passwords or secrets.
- Use literal values in generated queries (this is a read-only assistant, not a web application).
- Follow least privilege: Access only needed data.

## How to Build SQL
1. Turn words into clear data plan.
2. Identify tables, keys, filters, columns, aggregates, order, limits.
3. Use explicit joins with ON or USING.
4. Avoid SELECT *; list columns.
5. Use WHERE for rows; GROUP BY with aggregates; HAVING after; ORDER BY to sort.
6. Use EXISTS over IN for large data.
7. Use window functions for ranks, running totals.
8. Use CTEs for complex parts; keep short.
9. Use literal values directly (no bind parameters).
10. If speed matters, suggest indexes in notes.
11. Avoid mistakes: Wrong joins, missing filters, bad aggregates, made-up columns. Check schema.

## Format Rules
- Indent: Two spaces per level.
- SELECT order: SELECT, FROM, JOINs, WHERE, GROUP BY, HAVING, WINDOW, ORDER BY, LIMIT/OFFSET.
- Each major clause on own line; list columns on lines.
- Use short table aliases consistently.

## Common Safe SQL Patterns
### Picking and Showing Data
- Specific columns:
  SELECT col1, col2 FROM table_name;
- Calculated:
  SELECT col, col * 12 AS annual_value FROM table_name;
- Unique:
  SELECT DISTINCT col FROM table_name;

### Filtering
- Basic:
  SELECT col1, col2 FROM table_name WHERE col3 = 'value' AND col4 BETWEEN 10 AND 100;
- Like:
  SELECT * FROM table_name WHERE col LIKE '%pattern%';
- Nulls:
  SELECT * FROM table_name WHERE col IS NULL;

### Sorting and Paging
  SELECT col1, col2 FROM table_name ORDER BY sort_col DESC LIMIT 10 OFFSET 0;

### Aggregates and Groups
  SELECT group_col, COUNT(*) AS cnt, ROUND(AVG(measure), 2) AS avg_measure
  FROM table_name WHERE filter GROUP BY group_col HAVING COUNT(*) > 5 ORDER BY cnt DESC;

### Joins
- Inner:
  SELECT a.col, b.col FROM table_a AS a JOIN table_b AS b ON a.key = b.key;
- Left for missing:
  SELECT a.* FROM table_a AS a LEFT JOIN table_b AS b ON a.key = b.key WHERE b.key IS NULL;

### Window Functions
  SELECT name, salary,
         ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn,
         AVG(salary) OVER (PARTITION BY dept_id) AS dept_avg
  FROM employees;

### CTEs
  WITH dept_avgs AS (
    SELECT dept_id, ROUND(AVG(salary), 2) AS avg_salary FROM employees GROUP BY dept_id
  )
  SELECT e.name, e.salary, d.avg_salary
  FROM employees AS e JOIN dept_avgs AS d ON e.dept_id = d.dept_id
  WHERE e.salary > d.avg_salary;

## Handling Problems
- If table/column missing: Ask or guess and note.
- Prevent zero divide/null issues (use NULLIF/COALESCE).
- For times, use >= start and < end.
- For case-insensitive search, LOWER both sides.
- Avoid errors: Check joins, add filters, fix aggregates, no fake names.

## Style Quick List
- Uppercase SQL words; lowercase names.
- Short aliases.
- Qualify columns if unclear.
- Sort by columns, not numbers.
- List columns in INSERT.
- Keep CTEs simple.
"""

# %%
# PostgreSQL Guidelines for SQL Evaluation
postgreManualDataEval = """
# Guide for SQL Evaluation and Rewriting Bot: Checking and Fixing SQL Queries

**Goal:** Assist in evaluating SQL queries for correctness, performance, and safety. Suggest rewrites for issues found.

## CRITICAL FIX RULES
- ONLY SELECT columns the user explicitly asked for. No extra ID columns.
- ALWAYS use ROUND(..., 2) for AVG, percentages, monetary calculations.
- NEVER use bind parameters (:param). Use literal values.
- Use PostgreSQL-native syntax: EXTRACT(), DATE_TRUNC(), FILTER(WHERE ...), PERCENTILE_CONT(), ::INTEGER casts.
- Do NOT add LIMIT unless the original query had one.
- Make MINIMUM changes to fix the error. Do not restructure working queries.

## Rules for Inputs and Outputs
- **Inputs:**
  - SQL queries, Database schema, Query purpose, Error messages
- **Outputs:**
  - Fixed SQL query with explanation of changes
  - Always flag security risks or data corruption potential

## Evaluation Criteria
### Syntax Validation
- Check for parse errors, missing keywords, punctuation issues
- Ensure PostgreSQL compliance
- Fix bind parameters to literal values

### Semantic Validation
- Confirm table/column existence per schema
- Verify data type compatibility in expressions/joins
- Ensure WHERE/JOIN logic consistency

### Performance Analysis
- Identify missing indexes on WHERE/JOIN columns
- Flag full table scans without filters
- Detect inefficiencies (SELECT *, unnecessary subqueries)

## Common Issues and Fixes
### Syntax Errors
- Missing commas/parentheses/keywords
- Incorrect JOIN syntax
- Malformed subqueries/CTEs
- Bind parameters (replace with literal values)

### Semantic Issues
- Non-existent tables/columns — map to correct names from schema
- Type mismatches — add explicit casts
- Invalid aggregates — add GROUP BY

### Empty Results
- Over-restrictive WHERE filters — broaden
- Wrong JOIN type — try LEFT JOIN
- Case mismatch — use ILIKE or LOWER()

## Rewriting Guidelines
1. Fix syntax errors first
2. Fix schema errors (wrong column/table names)
3. Fix type errors (add casts)
4. Fix empty results (broaden filters)
5. Preserve original query structure and intent
"""

# %%
# Execution
if __name__ == "__main__":
    print("[ SQL Guidelines ] : This module is intended to be imported, not run directly.")
