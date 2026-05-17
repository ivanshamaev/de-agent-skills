---
name: sqlfluff
description: SQLFluff SQL linter — .sqlfluff config, dialect selection (ansi/bigquery/clickhouse/duckdb/hive/postgres/snowflake/sparksql/trino), rule sets, templating (Jinja/dbt), fix command, VS Code integration, pre-commit hook, CI/CD GitHub Actions, custom rules, noqa inline suppression
---

# SQLFluff — SQL Linter and Auto-Formatter

## When to Use

Load this skill when the user needs to:
- Enforce consistent SQL style across a team or codebase (keyword case, indentation, comma placement, alias conventions)
- Set up CI quality gates that reject non-conforming SQL before merge
- Lint a dbt project's `.sql` model files using the dbt templater (primary use case)
- Automatically fix style violations with `sqlfluff fix`
- Clean up legacy SQL that has grown inconsistent over time
- Write a `.sqlfluff` configuration file or tune specific rules
- Configure a pre-commit hook to lint SQL locally before push
- Create a GitHub Actions workflow that annotates PRs with inline SQL violations
- Suppress specific rule violations inline with `-- noqa` comments
- Write a custom SQLFluff rule as a plugin

---

## Installation and Basic Usage

### Install

```bash
# Core linter (ANSI dialect, Jinja templater)
pip install sqlfluff

# dbt templater (required for dbt projects — separate package)
pip install sqlfluff-templater-dbt

# With the dbt adapter for your warehouse (needed by the dbt templater)
pip install sqlfluff-templater-dbt dbt-trino==1.8.5
pip install sqlfluff-templater-dbt dbt-postgres==1.8.2
pip install sqlfluff-templater-dbt dbt-bigquery==1.8.2
pip install sqlfluff-templater-dbt dbt-snowflake==1.8.4

# Pin sqlfluff version for reproducible CI
pip install "sqlfluff==3.3.0" "sqlfluff-templater-dbt==3.3.0"
```

### List available dialects

```bash
sqlfluff dialects
# ansi, athena, bigquery, clickhouse, databricks, db2, duckdb, exasol,
# greenplum, hive, materialize, mysql, oracle, postgres, redshift,
# snowflake, soql, sparksql, sqlite, teradata, trino, tsql, vertica
```

### Lint a file or directory

```bash
# Lint a single file
sqlfluff lint path/to/query.sql

# Lint a directory recursively
sqlfluff lint models/

# Specify dialect on the command line
sqlfluff lint models/ --dialect trino

# Lint and emit GitHub PR annotations (for CI)
sqlfluff lint models/ --format github-annotation-native --annotation-level failure

# Human-readable output (default)
sqlfluff lint models/ --format human

# Machine-readable output
sqlfluff lint models/ --format json
sqlfluff lint models/ --format yaml

# Exit codes:
#   0 — no violations
#   1 — lint violations found
#   2 — linter error (parse error, config issue)
```

Sample output:

```
== [models/staging/stg_orders.sql] FAIL
L:   5 | P:  1 | LT02 | Expected 4 spaces of indentation, found 2. [layout.indent]
L:   8 | P: 12 | AL01 | Implicit/explicit aliasing of table. [aliasing.table]
L:  15 | P:  5 | CP01 | Keywords must be consistently upper case. [capitalisation.keywords]
All Finished! 📛 3 violations found in 1 out of 1 files.
```

### Fix violations automatically

```bash
# Fix safe violations only (default)
sqlfluff fix models/staging/stg_orders.sql

# Fix all violations including potentially unsafe ones (e.g. whitespace inside strings)
sqlfluff fix models/ --force

# Preview what would be fixed without writing (dry run)
sqlfluff fix models/ --check

# Fix only specific rules
sqlfluff fix models/ --rules LT01,LT02,CP01
```

**Safe vs unsafe fixes:** By default `sqlfluff fix` only applies fixes that are guaranteed to be semantically equivalent (whitespace, casing, trailing newlines). Fixes that could change semantics (e.g. rewriting `!=` to `<>`) are marked unsafe and require `--force` or individual rule confirmation.

### Parse tree inspection

```bash
# Dump the parse tree — useful when writing custom rules or debugging parse errors
sqlfluff parse models/staging/stg_orders.sql

# Output as JSON
sqlfluff parse models/staging/stg_orders.sql --format json
```

---

## `.sqlfluff` Configuration File

Place `.sqlfluff` at the root of your project (or dbt project directory). SQLFluff walks up the directory tree to find the nearest config file.

```ini
# .sqlfluff — complete annotated configuration for a dbt + Trino project

[sqlfluff]
# --- Dialect ---
# Required. Must match your warehouse. Controls keyword recognition and parsing.
# Options: ansi | bigquery | clickhouse | duckdb | hive | postgres | snowflake |
#          sparksql | trino | tsql | vertica (see `sqlfluff dialects`)
dialect = trino

# --- Templater ---
# "dbt"   — uses dbt to render Jinja; requires sqlfluff-templater-dbt + dbt adapter
# "jinja" — renders Jinja directly without dbt; use for non-dbt projects
# "python"— Python string.Template; rarely used
# "raw"   — no templating (fastest, for plain SQL files)
templater = dbt

# --- Line length ---
max_line_length = 120

# --- Indentation ---
# "space" or "tab"
indent_unit = space
tab_space_size = 4

# --- File extensions to lint ---
sql_file_exts = .sql,.sql.j2

# --- Rules to run ---
# Default: all rules. Specify a comma-separated allowlist to run only these rules.
# rules = AL01,AL02,CP01,LT01,LT02,LT04,LT05

# --- Rules to exclude globally ---
# RF04: reserved words as identifiers — noisy in many real-world schemas
# CV03: COUNT(*) vs COUNT(1) — team preference varies
# AM04: COUNT DISTINCT syntax — dialect-dependent
exclude_rules = RF04,CV03

# --- Output format ---
# human | json | yaml | github-annotation-native | github-annotation
output_line_format = {code} {description}

# --- Parallel execution (see Performance section) ---
# processes = 4

# --- Ignore files / directories (glob patterns) ---
ignore = dbt_packages, target, logs, .venv

[sqlfluff:indentation]
# Whether JOIN clauses are indented relative to SELECT
indented_joins = false

# Whether CTEs are indented inside the WITH block
indented_ctes = false

# Whether the ON / USING clause of a JOIN is indented
indented_using_on = true

# Whether Jinja/dbt template blocks ({% if %}, {% for %}) contribute to indentation
# Set false for dbt to avoid double-indenting SQL inside Jinja blocks
template_blocks_indent = false

[sqlfluff:layout:type:comma]
# "trailing" — comma at end of line (most common in SQL)
# "leading"  — comma at start of next line (some teams prefer this)
line_position = trailing
spacing_before = touch
spacing_after = single

[sqlfluff:layout:type:comparison_operator]
# Force comparison operators to have spaces on both sides
spacing_before = single
spacing_after = single

[sqlfluff:rules:capitalisation.keywords]
# "upper"      — SELECT, FROM, WHERE ...
# "lower"      — select, from, where ...
# "capitalise" — Select, From, Where ...
# "consistent" — match whatever is used first in the file
capitalisation_policy = upper

[sqlfluff:rules:capitalisation.identifiers]
# How column/table/alias identifiers should be cased
# "lower" is typical for most warehouses; "consistent" lets teams mix
capitalisation_policy = lower

[sqlfluff:rules:capitalisation.functions]
# Built-in function names: COALESCE, COUNT, SUM, etc.
capitalisation_policy = upper

[sqlfluff:rules:capitalisation.literals]
# NULL, TRUE, FALSE
capitalisation_policy = upper

[sqlfluff:rules:capitalisation.types]
# Data type keywords: INT, VARCHAR, TIMESTAMP, etc.
capitalisation_policy = upper

[sqlfluff:rules:aliasing.table]
# "explicit"  — require AS keyword in aliases: FROM orders AS o
# "implicit"  — forbid AS keyword: FROM orders o
# "consistent"— match whatever style appears first
aliasing = explicit

[sqlfluff:rules:aliasing.column]
# Same options as aliasing.table
aliasing = explicit

[sqlfluff:rules:aliasing.expression]
# Require aliases on complex expressions in SELECT
# "explicit" | "implicit" | "consistent"
aliasing = explicit

[sqlfluff:rules:ambiguous.column_references]
# Forbid bare column references in GROUP BY / ORDER BY (use column names, not numbers)
# "consistent" | "qualified" | "unqualified"
group_by_and_order_by_style = consistent

[sqlfluff:rules:references.from]
# Require every column reference in SELECT to be qualified with table alias
# when more than one table is in scope
# "consistent" | "qualified" | "unqualified"
# force_enable = false  # set true to always enforce qualification

[sqlfluff:rules:convention.not_equal]
# Which NOT EQUAL operator to enforce
# "consistent" | "<>" | "!="
preferred_not_equal_style = consistent

[sqlfluff:rules:convention.count_rows]
# "consistent" | "zero" (COUNT(0)) | "star" (COUNT(*)) | "expression" (COUNT(1))
preferred_count_rows_expression = star

[sqlfluff:templater:jinja]
# Context variables for standalone Jinja templating (templater = jinja).
# Not used when templater = dbt — dbt provides its own context.
[sqlfluff:templater:jinja:context]
# Define Jinja variables that appear in your SQL templates
# my_date = '2024-01-01'
# env_name = 'prod'
```

---

## dbt Integration

The dbt templater is the most common production use case. It uses dbt's own Jinja engine to render templates before linting, so `{{ ref() }}`, `{{ source() }}`, `{{ config() }}`, and custom macros all resolve correctly.

### Installation

```bash
# Both packages must be on the same minor version
pip install "sqlfluff==3.3.0" "sqlfluff-templater-dbt==3.3.0" "dbt-trino==1.8.5"

# Always run dbt deps before linting — the templater needs dbt packages
dbt deps
```

### `.sqlfluff` for a dbt project

```ini
[sqlfluff]
dialect = trino
templater = dbt
max_line_length = 120
indent_unit = space
tab_space_size = 4

# Rules commonly excluded in dbt projects (explained below)
exclude_rules = RF04,ST06,AM04

[sqlfluff:indentation]
indented_joins = false
indented_ctes = false
template_blocks_indent = false   # CRITICAL for dbt — prevents double-indent inside {% if %}

[sqlfluff:templater:dbt]
# Path to the dbt project root (contains dbt_project.yml)
# Default: current directory
project_dir = ./

# Path to profiles.yml
# Default: ~/.dbt/
profiles_dir = ./

# Which profile name to use (must exist in profiles.yml)
# Default: value of "profile:" in dbt_project.yml
profile = my_project

# Which target in the profile to use for linting
# Use a lightweight "dev" or "ci" target that doesn't need a live connection
# (sqlfluff only renders templates; it doesn't execute SQL)
target = dev
```

### Running sqlfluff on a dbt project

```bash
# From the dbt project root
sqlfluff lint models/

# Lint specific subdirectories
sqlfluff lint models/staging/ models/marts/

# Lint a single model
sqlfluff lint models/marts/finance/fct_orders.sql

# Pass dialect explicitly (overrides .sqlfluff)
sqlfluff lint models/ --dialect trino

# Fix style issues in all models
sqlfluff fix models/ --force

# Lint only changed files (useful locally, before commit)
git diff --name-only HEAD | grep '\.sql$' | xargs sqlfluff lint --dialect trino
```

### How dbt templating works with SQLFluff

SQLFluff invokes dbt's compilation engine to render each model before parsing. This means:

| Template construct | Behavior |
|---|---|
| `{{ ref('stg_orders') }}` | Resolved to the fully-qualified table name |
| `{{ source('raw', 'orders') }}` | Resolved to the source table reference |
| `{{ config(materialized='table') }}` | Block is stripped before linting |
| `{% if target.name == 'prod' %}` | **Rendered** — both branches are linted if `execute=True` |
| `{{ my_macro(...) }}` | Expanded via dbt macro resolution |
| `{{ var('my_var') }}` | Requires the variable to be defined in `dbt_project.yml` or passed via `--vars` |

**What doesn't work:**
- Macros from packages not installed (`dbt deps` not run)
- `execute` context functions (only available at runtime, not compile)
- Models with `{{ run_query() }}` inside macros that produce SQL

### Example: well-linted dbt model

```sql
-- models/marts/finance/fct_orders.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}
        WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
),

enriched AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.status,
        o.amount_usd,
        o.created_at,
        c.country_code
    FROM source AS o
    LEFT JOIN {{ ref('dim_customers') }} AS c
        ON o.customer_id = c.customer_id
)

SELECT *
FROM enriched
```

### Common dbt-specific rule exclusions and why

```ini
[sqlfluff]
exclude_rules =
    RF04,    # Reserved words as identifiers — dbt uses "ref", "source" etc. as identifiers
    ST06,    # SELECT wildcards — dbt staging models legitimately use SELECT *
    AM04,    # COUNT(*) with DISTINCT — dialect-specific; trino supports it natively
    CV10,    # SELECT * forbidden — same as ST06; use whichever rule name your version uses
    LT13     # Start of file — dbt models start with {{ config(...) }}, not a SQL statement
```

**Rule-specific explanation:**

- **RF04** (`reserved_words`): dbt macros like `{{ ref(...) }}` get rendered as identifiers that sometimes match reserved words depending on the table name.
- **ST06 / CV10** (`select_wildcards`): Staging models intentionally use `SELECT *` to pass through all source columns. Suppress at the model level or globally.
- **LT13** (`start_of_file`): dbt models start with a Jinja `{{ config(...) }}` block, which SQLFluff sees as a non-SQL start. This fires LT13 on every model.
- **template_blocks_indent = false**: Without this, SQL inside `{% if is_incremental() %}` gets double-indented. Always set this in dbt projects.

### Selective inline suppression in dbt models

```sql
-- Suppress SELECT * on a specific staging model
SELECT * -- noqa: ST06
FROM {{ ref('raw_events') }}

-- Suppress a specific rule on a JOIN that legitimately uses implicit style
FROM orders o  -- noqa: AL01
LEFT JOIN customers c ON o.customer_id = c.customer_id  -- noqa: AL01
```

---

## Rule Reference

Rules were renamed from `L001`-style codes to semantic names in SQLFluff 2.0. Always use the new names. The old `L0XX` codes still work as aliases but are deprecated.

### Aliasing (AL)

| Rule | Name | Violation | Fix |
|---|---|---|---|
| AL01 | `aliasing.table` | `FROM orders o` | `FROM orders AS o` |
| AL02 | `aliasing.column` | `SELECT id + 1 order_id` | `SELECT id + 1 AS order_id` |
| AL03 | `aliasing.expression` | `SELECT 1 + 1` (no alias) | `SELECT 1 + 1 AS two` |
| AL04 | `aliasing.unique` | Two CTEs or tables with same alias | Rename one alias |
| AL05 | `aliasing.unused` | CTE defined but never referenced | Remove the CTE |
| AL06 | `aliasing.length` | Table alias shorter than configured min | Lengthen alias |
| AL07 | `aliasing.forbid` | Aliases forbidden when configured | Remove alias |
| AL08 | `aliasing.column.wildcard` | `SELECT t.*` — qualified wildcard | Configure per project |
| AL09 | `aliasing.self_alias.column` | Column aliased to its own name: `id AS id` | Remove the alias |

```sql
-- AL01 violation
SELECT o.id FROM orders o;

-- AL01 fix
SELECT o.id FROM orders AS o;

-- AL05 violation — unused CTE
WITH unused_cte AS (SELECT 1),
     main AS (SELECT * FROM orders)
SELECT * FROM main;

-- AL05 fix — remove the unused CTE
WITH main AS (SELECT * FROM orders)
SELECT * FROM main;
```

### Ambiguous (AM)

| Rule | Name | Description |
|---|---|---|
| AM01 | `ambiguous.distinct` | `SELECT DISTINCT COUNT(x)` — DISTINCT inside aggregate |
| AM02 | `ambiguous.union` | Bare `UNION` (not `UNION ALL`) is almost always a mistake |
| AM03 | `ambiguous.order_by` | Numeric ORDER BY column references |
| AM04 | `ambiguous.column_count` | `COUNT(DISTINCT ...)` — style enforcement |
| AM05 | `ambiguous.join` | JOIN without explicit type (INNER/LEFT/RIGHT/CROSS) |
| AM06 | `ambiguous.select_star_then_whitelisted` | `SELECT *, col` — mixing * with named columns |

```sql
-- AM02 violation: UNION removes duplicates, which is usually unintended
SELECT id FROM a
UNION
SELECT id FROM b;

-- AM02 fix: be explicit
SELECT id FROM a
UNION ALL
SELECT id FROM b;

-- AM05 violation: ambiguous join type
SELECT * FROM a JOIN b ON a.id = b.id;

-- AM05 fix
SELECT * FROM a INNER JOIN b ON a.id = b.id;
```

### Capitalisation (CP)

| Rule | Name | Description |
|---|---|---|
| CP01 | `capitalisation.keywords` | SELECT, FROM, WHERE, JOIN, etc. |
| CP02 | `capitalisation.identifiers` | table_name, column_name |
| CP03 | `capitalisation.functions` | COALESCE, COUNT, SUM, UPPER |
| CP04 | `capitalisation.literals` | NULL, TRUE, FALSE |
| CP05 | `capitalisation.types` | INT, VARCHAR, TIMESTAMP |

```sql
-- CP01 violation
select id, name from orders where status = 'active';

-- CP01 fix
SELECT id, name FROM orders WHERE status = 'active';

-- CP03 violation
SELECT coalesce(name, 'unknown') FROM customers;

-- CP03 fix
SELECT COALESCE(name, 'unknown') FROM customers;
```

Configure policy per rule:

```ini
[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = upper    # SELECT, FROM, WHERE → uppercase

[sqlfluff:rules:capitalisation.identifiers]
capitalisation_policy = lower    # table_name, column_name → lowercase

[sqlfluff:rules:capitalisation.functions]
capitalisation_policy = upper    # COALESCE, COUNT → uppercase

[sqlfluff:rules:capitalisation.literals]
capitalisation_policy = upper    # NULL, TRUE, FALSE → uppercase

[sqlfluff:rules:capitalisation.types]
capitalisation_policy = upper    # INT, VARCHAR → uppercase
```

### Convention (CV)

| Rule | Name | Description |
|---|---|---|
| CV01 | `convention.not_equal` | Enforce `<>` vs `!=` consistently |
| CV02 | `convention.coalesce` | Prefer COALESCE over NVL/IFNULL/IIF |
| CV03 | `convention.count_rows` | COUNT(*) vs COUNT(1) vs COUNT(0) |
| CV04 | `convention.is_null` | Use `IS NULL` not `= NULL` |
| CV05 | `convention.casting_style` | CAST(x AS type) vs x::type (dialect-specific) |
| CV06 | `convention.statement_terminator` | Require/forbid trailing semicolons |
| CV07 | `convention.select_union_order` | SELECT before UNION ORDER BY |
| CV08 | `convention.left_join` | Prefer LEFT JOIN over RIGHT JOIN |
| CV09 | `convention.inner_join` | Require INNER keyword (or forbid it) |
| CV10 | `convention.select_wildcards` | Forbid `SELECT *` |
| CV11 | `convention.unnecessary_else` | `CASE WHEN x THEN y ELSE NULL END` → remove ELSE NULL |

```sql
-- CV04 violation
SELECT * FROM orders WHERE cancelled_at = NULL;

-- CV04 fix
SELECT * FROM orders WHERE cancelled_at IS NULL;

-- CV11 violation
SELECT
    CASE
        WHEN status = 'active' THEN 1
        ELSE NULL
    END AS is_active
FROM orders;

-- CV11 fix (ELSE NULL is implicit)
SELECT
    CASE
        WHEN status = 'active' THEN 1
    END AS is_active
FROM orders;

-- CV02 violation
SELECT NVL(name, 'unknown') FROM customers;

-- CV02 fix
SELECT COALESCE(name, 'unknown') FROM customers;
```

### Layout (LT)

| Rule | Name | Description |
|---|---|---|
| LT01 | `layout.trailing_whitespace` | Trailing spaces on a line |
| LT02 | `layout.indent` | Indentation depth/character |
| LT03 | `layout.operators` | Spacing around operators |
| LT04 | `layout.commas` | Trailing vs leading comma position |
| LT05 | `layout.long_lines` | Lines exceeding `max_line_length` |
| LT06 | `layout.functions` | Space before function parenthesis |
| LT07 | `layout.cte_bracket` | Spacing around CTE `AS (` |
| LT08 | `layout.cte_newline` | Blank lines between CTEs |
| LT09 | `layout.select_targets` | Each SELECT expression on its own line |
| LT10 | `layout.select_modifiers` | DISTINCT/ALL placement in SELECT |
| LT11 | `layout.set_operators` | UNION/INTERSECT/EXCEPT spacing |
| LT12 | `layout.end_of_file` | Trailing newline at end of file |
| LT13 | `layout.start_of_file` | No leading blank lines at file start |

```sql
-- LT04 violation: leading comma (if trailing configured)
SELECT
    id
  , name
  , status
FROM orders;

-- LT04 fix: trailing comma
SELECT
    id,
    name,
    status
FROM orders;

-- LT09 violation: multiple targets on one line
SELECT id, name, status FROM orders;

-- LT09 fix: each target on its own line
SELECT
    id,
    name,
    status
FROM orders;
```

Configure comma position:

```ini
[sqlfluff:layout:type:comma]
line_position = trailing    # or "leading"
```

### References (RF)

| Rule | Name | Description |
|---|---|---|
| RF01 | `references.from` | Referenced tables must exist in FROM clause |
| RF02 | `references.qualification` | Column references must be qualified in multi-table queries |
| RF03 | `references.consistent` | Qualification must be consistent |
| RF04 | `references.keywords` | Avoid reserved words as identifiers |
| RF05 | `references.quoting` | Consistent use of quoting |
| RF06 | `references.special_chars` | Unnecessary quoting of identifiers |

```sql
-- RF02 violation: ambiguous column reference with two tables in scope
SELECT id, name
FROM orders
JOIN customers ON orders.customer_id = customers.id;

-- RF02 fix: qualify all references
SELECT orders.id, customers.name
FROM orders
JOIN customers ON orders.customer_id = customers.id;

-- Or use aliases
SELECT o.id, c.name
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.id;
```

### Structure (ST)

| Rule | Name | Description |
|---|---|---|
| ST01 | `structure.else_null` | Same as CV11 — redundant ELSE NULL |
| ST02 | `structure.simple_case` | `CASE WHEN x = TRUE THEN y` → `CASE WHEN x THEN y` |
| ST03 | `structure.unused_cte` | CTE defined but not referenced (same as AL05) |
| ST04 | `structure.nested_case` | Nested CASE can be flattened |
| ST05 | `structure.subquery` | Subquery in WHERE/FROM could be a CTE |
| ST06 | `structure.select_wildcards` | SELECT * is ambiguous |
| ST07 | `structure.using` | USING clause ambiguity |
| ST08 | `structure.distinct` | DISTINCT in aggregate |
| ST09 | `structure.join_condition_order` | JOIN conditions in consistent order |

```sql
-- ST02 violation: CASE WHEN comparing to TRUE
SELECT
    CASE
        WHEN is_active = TRUE THEN 'yes'
        ELSE 'no'
    END
FROM customers;

-- ST02 fix
SELECT
    CASE
        WHEN is_active THEN 'yes'
        ELSE 'no'
    END
FROM customers;

-- ST04 violation: nested CASE
SELECT
    CASE
        WHEN a > 0 THEN
            CASE
                WHEN b > 0 THEN 'both'
                ELSE 'only a'
            END
        ELSE 'neither'
    END
FROM t;

-- ST04 fix: flatten with AND
SELECT
    CASE
        WHEN a > 0 AND b > 0 THEN 'both'
        WHEN a > 0 THEN 'only a'
        ELSE 'neither'
    END
FROM t;
```

---

## Inline Suppression

Use `-- noqa` comments to suppress violations on specific lines. Use sparingly — prefer fixing the underlying issue.

### Syntax

```sql
-- Suppress ALL rules on this line
SELECT * FROM orders  -- noqa

-- Suppress specific rules by code (comma-separated)
SELECT * FROM orders  -- noqa: ST06,AL01

-- Suppress on the next line (useful for multi-line expressions)
-- noqa: LT05
SELECT very_long_column_name_1, very_long_column_name_2, very_long_column_name_3 FROM t

-- Block suppression: disable rules for a range of lines
-- noqa: disable=all
SELECT
    *  -- this is intentional in a staging passthrough model
FROM {{ ref('raw_events') }}
-- noqa: enable=all

-- Block suppression for specific rules
-- noqa: disable=ST06,AL01
SELECT * FROM source_table
-- noqa: enable=ST06,AL01
```

### When to use vs fixing the issue

**Use `-- noqa` when:**
- The violation is a genuine false positive (e.g., a dbt macro expands to a reserved word)
- The pattern is intentional by design (e.g., `SELECT *` in a staging passthrough)
- A rule is overly strict for a specific exceptional case
- Generated SQL that cannot be reformatted

**Fix the underlying issue when:**
- The rule catches a real style inconsistency
- The `-- noqa` would need to be placed on dozens of lines (configure globally instead)
- The violation indicates a potential logic error (CV04, AM02, AM05)

```sql
-- Bad: noqa used to mask a real problem
SELECT * FROM a JOIN b ON a.id = b.id  -- noqa: AM05

-- Better: fix the root cause
SELECT * FROM a INNER JOIN b ON a.id = b.id

-- Acceptable: dbt staging passthrough where SELECT * is intentional
SELECT *  -- noqa: ST06
FROM {{ source('raw', 'orders') }}
```

---

## Pre-commit Hook

Add SQLFluff to `.pre-commit-config.yaml` to lint (and optionally fix) SQL before every commit.

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 3.3.0          # pin to a specific release tag
    hooks:
      # Lint only — fails if violations found, does NOT modify files
      - id: sqlfluff-lint
        name: SQLFluff Lint
        args:
          - --dialect=trino
          # If using dbt templater, pass these args:
          # - --templater=dbt
          # - --config=.sqlfluff
        additional_dependencies:
          # Required for dbt templater:
          - sqlfluff-templater-dbt==3.3.0
          - dbt-trino==1.8.5
        files: '^dbt/models/.*\.sql$'     # only lint model files
        exclude: '^dbt/target/|dbt_packages/'

      # Fix and re-stage — automatically rewrites style violations
      - id: sqlfluff-fix
        name: SQLFluff Fix
        args:
          - --dialect=trino
          - --force              # apply unsafe fixes too
        additional_dependencies:
          - sqlfluff-templater-dbt==3.3.0
          - dbt-trino==1.8.5
        files: '^dbt/models/.*\.sql$'
        exclude: '^dbt/target/|dbt_packages/'
```

**Important notes for dbt + pre-commit:**
- The dbt templater requires `dbt deps` to have been run before the hook executes. Add a pre-commit hook to run `dbt deps` if needed, or use the Jinja templater instead for pre-commit to avoid this dependency.
- The `additional_dependencies` list installs packages into the pre-commit isolated environment — they must match the versions in your project.
- If `dbt deps` is too slow for pre-commit, consider using `templater = jinja` with stub context variables for the pre-commit hook, and `templater = dbt` in CI only.

```yaml
# Lightweight pre-commit config using jinja templater (no dbt dep required)
repos:
  - repo: https://github.com/sqlfluff/sqlfluff
    rev: 3.3.0
    hooks:
      - id: sqlfluff-lint
        args: [--dialect=trino, --templater=jinja]
        files: '^dbt/models/.*\.sql$'
        exclude: '^dbt/target/|dbt_packages/'
```

Install and run hooks:

```bash
# Install pre-commit (once per machine)
pip install pre-commit

# Install hooks into your repo
pre-commit install

# Run manually against all files
pre-commit run sqlfluff-lint --all-files

# Run on staged files only (what happens on git commit)
pre-commit run sqlfluff-lint
```

---

## CI/CD GitHub Actions

### Workflow 1: Lint-only with Native GitHub Annotations

This workflow runs on every PR that touches SQL files and produces inline code annotations on changed lines — no additional GitHub App required.

```yaml
# .github/workflows/sqlfluff-lint.yml
name: SQLFluff Lint

on:
  pull_request:
    branches: [main]
    paths:
      - 'dbt/models/**/*.sql'
      - 'dbt/macros/**/*.sql'
      - 'dbt/tests/**/*.sql'
      - '.sqlfluff'
      - '.github/workflows/sqlfluff-lint.yml'

permissions:
  contents: read
  pull-requests: write          # required to post PR comments

concurrency:
  group: sqlfluff-${{ github.ref }}
  cancel-in-progress: true

env:
  SQLFLUFF_VERSION: "3.3.0"
  DBT_ADAPTER: "dbt-trino==1.8.5"

jobs:
  lint:
    name: SQLFluff Lint
    runs-on: ubuntu-24.04
    timeout-minutes: 15

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install sqlfluff + dbt templater
        run: |
          pip install --upgrade pip
          pip install \
            "sqlfluff==${{ env.SQLFLUFF_VERSION }}" \
            "sqlfluff-templater-dbt==${{ env.SQLFLUFF_VERSION }}" \
            "${{ env.DBT_ADAPTER }}"

      - name: Install dbt packages
        working-directory: dbt
        run: dbt deps

      # github-annotation-native emits GitHub Actions workflow commands
      # that the runner converts into inline PR annotations — no action needed
      - name: SQLFluff lint (annotate PR)
        id: lint
        working-directory: dbt
        continue-on-error: true   # collect all violations before deciding to fail
        run: |
          sqlfluff lint models/ macros/ tests/ \
            --format github-annotation-native \
            --annotation-level failure \
            --dialect trino \
            2>&1 | tee /tmp/sqlfluff-output.txt
          echo "exit_code=${PIPESTATUS[0]}" >> "$GITHUB_OUTPUT"

      # Post a collapsible summary comment on the PR
      - name: Comment lint results on PR
        if: steps.lint.outputs.exit_code != '0'
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            const raw = fs.readFileSync('/tmp/sqlfluff-output.txt', 'utf8');
            // Truncate to GitHub comment size limit (65536 chars)
            const output = raw.length > 60000 ? raw.slice(0, 60000) + '\n... (truncated)' : raw;
            const body = [
              '## SQLFluff Lint Results',
              '',
              `Found SQL style violations. Fix them with \`sqlfluff fix dbt/models/ --force\`.`,
              '',
              '<details><summary>Full lint output</summary>',
              '',
              '```',
              output,
              '```',
              '</details>'
            ].join('\n');
            await github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body
            });

      - name: Fail workflow if violations found
        if: steps.lint.outputs.exit_code != '0'
        run: |
          echo "SQLFluff found violations. See annotations above."
          exit 1
```

### Workflow 2: Lint + Autofix (commit style fixes back to PR branch)

This workflow detects violations, applies `sqlfluff fix`, and pushes the fixed files back to the PR branch as a new commit. Use only if you trust auto-fixes and your branch protection rules allow workflow commits.

```yaml
# .github/workflows/sqlfluff-autofix.yml
name: SQLFluff Autofix

on:
  pull_request:
    branches: [main]
    paths:
      - 'dbt/models/**/*.sql'
      - 'dbt/macros/**/*.sql'

permissions:
  contents: write               # required to push the fix commit
  pull-requests: write

concurrency:
  group: sqlfluff-fix-${{ github.ref }}
  cancel-in-progress: true

jobs:
  autofix:
    name: SQLFluff Autofix
    runs-on: ubuntu-24.04
    timeout-minutes: 20

    steps:
      # Check out the HEAD commit of the PR branch (not the merge commit)
      - uses: actions/checkout@v4
        with:
          ref: ${{ github.head_ref }}
          # Use a PAT or GitHub App token if your branch protection requires
          # signed commits or prevents GITHUB_TOKEN from pushing
          token: ${{ secrets.GITHUB_TOKEN }}

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install sqlfluff
        run: |
          pip install --upgrade pip
          pip install "sqlfluff==3.3.0" "sqlfluff-templater-dbt==3.3.0" dbt-trino==1.8.5

      - name: Install dbt packages
        working-directory: dbt
        run: dbt deps

      - name: sqlfluff fix
        working-directory: dbt
        run: |
          sqlfluff fix models/ macros/ \
            --dialect trino \
            --force \
            --nocolor

      # Commit and push only if files were changed
      - name: Commit and push fixes
        uses: stefanzweifel/git-auto-commit-action@v5
        with:
          commit_message: "style: sqlfluff autofix [skip ci]"
          file_pattern: "dbt/models/**/*.sql dbt/macros/**/*.sql"
          commit_author: "github-actions[bot] <github-actions[bot]@users.noreply.github.com>"
          # [skip ci] prevents this commit from triggering another workflow run
```

### Matrix strategy — lint multiple dialects / dbt targets

```yaml
# .github/workflows/sqlfluff-matrix.yml
name: SQLFluff Multi-Dialect

on:
  pull_request:
    paths: ['sql/**/*.sql', '.sqlfluff']

jobs:
  lint:
    name: "Lint — ${{ matrix.dialect }}"
    runs-on: ubuntu-24.04
    timeout-minutes: 10

    strategy:
      fail-fast: false          # run all dialects even if one fails
      matrix:
        include:
          - dialect: trino
            path: sql/trino/
            sqlfluff_config: .sqlfluff.trino
          - dialect: postgres
            path: sql/postgres/
            sqlfluff_config: .sqlfluff.postgres
          - dialect: clickhouse
            path: sql/clickhouse/
            sqlfluff_config: .sqlfluff.clickhouse

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install sqlfluff
        run: pip install "sqlfluff==3.3.0"

      - name: Lint ${{ matrix.dialect }}
        run: |
          sqlfluff lint ${{ matrix.path }} \
            --dialect ${{ matrix.dialect }} \
            --config ${{ matrix.sqlfluff_config }} \
            --format github-annotation-native \
            --annotation-level warning
```

### Caching pip dependencies

```yaml
      # actions/setup-python with cache: pip caches the pip download cache
      # For faster installs, also cache the virtual environment
      - name: Cache pip venv
        uses: actions/cache@v4
        with:
          path: ~/.cache/pip
          key: sqlfluff-${{ runner.os }}-pip-${{ hashFiles('**/requirements*.txt') }}
          restore-keys: |
            sqlfluff-${{ runner.os }}-pip-

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip            # built-in pip cache in setup-python
```

---

## VS Code Integration

### Install the extension

Install the **SQLFluff** extension by **dbt Labs** (extension ID: `dbt-labs.sqlfluff`), or the **SQLFluff** extension by **RobertOstrowskiSmith** (older, community-maintained).

From the command palette: `ext install dbt-labs.sqlfluff`

### `.vscode/settings.json`

```jsonc
{
    // SQLFluff extension settings
    "sqlfluff.executablePath": "${workspaceFolder}/.venv/bin/sqlfluff",
    "sqlfluff.linter.run": "onSave",           // "onSave" | "onType" | "off"
    "sqlfluff.linter.lintEntireProject": false, // lint only the open file
    "sqlfluff.config": "${workspaceFolder}/.sqlfluff",

    // Dialect (overrides .sqlfluff for the extension)
    "sqlfluff.dialect": "trino",

    // Templater (use "jinja" for VS Code to avoid needing dbt connection)
    "sqlfluff.linter.arguments": ["--templater", "jinja"],

    // Format on save using sqlfluff fix
    "[sql]": {
        "editor.defaultFormatter": "dbt-labs.sqlfluff",
        "editor.formatOnSave": true
    },

    // Show problems in the Problems panel
    "sqlfluff.linter.diagnosticSeverity": "error",      // error | warning | info | hint
    "sqlfluff.linter.diagnosticSeverityByRule": {
        "LT05": "warning",    // long lines — warning, not error
        "ST06": "info"        // SELECT * — informational
    },

    // File associations — lint .sql.j2 files as SQL
    "files.associations": {
        "*.sql.j2": "sql",
        "*.sql": "sql"
    }
}
```

### Workspace-level settings for a dbt project

```jsonc
// .vscode/settings.json — dbt project with dbt Power User extension
{
    "sqlfluff.executablePath": "${workspaceFolder}/.venv/bin/sqlfluff",
    "sqlfluff.dialect": "trino",

    // Use jinja templater in VS Code — dbt templater requires a live dbt environment
    // which is impractical for real-time linting
    "sqlfluff.linter.arguments": [
        "--templater", "jinja",
        "--exclude-rules", "LT13"
    ],

    "[jinja-sql]": {
        "editor.defaultFormatter": "dbt-labs.sqlfluff",
        "editor.formatOnSave": true,
        "editor.formatOnSaveMode": "file"
    },

    // dbt Power User extension — set dbt path
    "dbt.dbtPythonPathOverride": "${workspaceFolder}/.venv/bin/python"
}
```

### Problem matcher for inline error display

If you run sqlfluff via a VS Code task, use this problem matcher to show violations inline:

```jsonc
// .vscode/tasks.json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "SQLFluff: Lint",
            "type": "shell",
            "command": "${workspaceFolder}/.venv/bin/sqlfluff lint ${file} --dialect trino",
            "group": "test",
            "presentation": { "reveal": "always", "panel": "shared" },
            "problemMatcher": {
                "owner": "sqlfluff",
                "fileLocation": ["absolute"],
                "pattern": {
                    "regexp": "^\\s+L:\\s*(\\d+)\\s+\\|\\s+P:\\s*(\\d+)\\s+\\|\\s+(\\w+)\\s+\\|\\s+(.*)$",
                    "file": 0,
                    "line": 1,
                    "column": 2,
                    "code": 3,
                    "message": 4
                }
            }
        },
        {
            "label": "SQLFluff: Fix",
            "type": "shell",
            "command": "${workspaceFolder}/.venv/bin/sqlfluff fix ${file} --dialect trino --force",
            "group": "test"
        }
    ]
}
```

---

## Custom Rules

SQLFluff supports plugin-based custom rules. Rules are Python classes that walk the parse tree and emit `LintResult` objects.

### Plugin structure

```
my_sqlfluff_plugin/
├── setup.cfg              (or pyproject.toml)
├── my_sqlfluff_plugin/
│   ├── __init__.py
│   └── rules.py
```

### `setup.cfg`

```ini
[metadata]
name = my_sqlfluff_plugin
version = 0.1.0

[options]
packages = find:
install_requires =
    sqlfluff>=3.0.0

# Register the rule plugin via entry_points
[options.entry_points]
sqlfluff = my_sqlfluff_plugin = my_sqlfluff_plugin.rules
```

### `my_sqlfluff_plugin/rules.py`

```python
"""Custom SQLFluff rules for our data engineering team."""
from typing import Optional

from sqlfluff.core.rules import BaseRule, LintResult, RuleContext
from sqlfluff.core.rules.crawlers import SegmentSeekerCrawler


class Rule_MY01(BaseRule):
    """CTEs must have a preceding comment explaining their purpose.

    **Anti-pattern**

    .. code-block:: sql

        WITH raw_orders AS (
            SELECT * FROM orders
        )
        SELECT * FROM raw_orders

    **Best practice**

    .. code-block:: sql

        -- raw_orders: source data from the orders table before any transformations
        WITH raw_orders AS (
            SELECT * FROM orders
        )
        SELECT * FROM raw_orders
    """

    name = "custom.cte_comment"
    aliases = ("MY01",)
    groups = ("all", "custom")
    # The crawler determines which node types this rule visits
    crawl_behaviour = SegmentSeekerCrawler({"cte_definition"})
    is_fix_compatible = False  # this rule cannot be auto-fixed

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Check that each CTE definition is preceded by a comment."""
        segment = context.segment

        # Walk backwards through siblings to find a comment before this CTE
        raw_stack = context.raw_stack
        for prior_segment in reversed(raw_stack):
            if prior_segment.is_type("comment", "block_comment", "inline_comment"):
                return None  # comment found → pass
            if prior_segment.is_type("cte_definition", "with_compound_statement"):
                break  # hit another CTE without finding a comment → fail
            if not prior_segment.is_whitespace and not prior_segment.is_newline:
                break  # non-whitespace non-comment found → fail

        return LintResult(
            anchor=segment,
            description=(
                f"CTE '{segment.get_child('cte_definition').raw}' "
                "must be preceded by a comment explaining its purpose."
            ),
        )


class Rule_MY02(BaseRule):
    """SELECT * is forbidden in mart-layer models (models/marts/**).

    Only applies when the file path contains 'marts/'. Staging models
    may use SELECT * as a passthrough pattern.

    **Anti-pattern** (in models/marts/)

    .. code-block:: sql

        SELECT * FROM stg_orders

    **Best practice**

    .. code-block:: sql

        SELECT
            order_id,
            customer_id,
            status
        FROM stg_orders
    """

    name = "custom.no_star_in_marts"
    aliases = ("MY02",)
    groups = ("all", "custom")
    crawl_behaviour = SegmentSeekerCrawler({"wildcard_expression"})
    is_fix_compatible = False

    def _eval(self, context: RuleContext) -> Optional[LintResult]:
        """Flag SELECT * in mart-layer models."""
        # Only enforce in mart models
        fname = context.filename or ""
        if "marts" not in fname:
            return None

        return LintResult(
            anchor=context.segment,
            description=(
                "SELECT * is forbidden in mart-layer models. "
                "Enumerate the required columns explicitly."
            ),
        )
```

### Install and activate the plugin

```bash
# Install the plugin into the same environment as sqlfluff
pip install -e ./my_sqlfluff_plugin/

# Verify the rule is registered
sqlfluff rules | grep MY
```

```ini
# .sqlfluff — reference the plugin (entry_points auto-registration in newer versions)
# If using load_macros_from for older compatibility:
[sqlfluff]
dialect = trino
# Custom rules are automatically discovered via entry_points
# Optionally restrict to custom rules only:
# rules = MY01,MY02
```

---

## Performance Tuning

### Parallel execution

```bash
# Run with 4 parallel worker processes (default: 1)
sqlfluff lint models/ --processes 4

# Use all available CPU cores
sqlfluff lint models/ --processes -1
```

```ini
# .sqlfluff
[sqlfluff]
processes = 4
```

**When to parallelize:** With >100 SQL files. For dbt templater, parallel processes each start a dbt compilation subprocess — memory usage grows proportionally. Start with `--processes 4` and increase if memory allows.

### Reduce rule set for speed

```bash
# Only run the rules that matter most in CI (fast pass)
sqlfluff lint models/ --rules LT01,LT02,LT04,LT05,CP01,AL01,CV04,AM02

# Exclude expensive rules
sqlfluff lint models/ --exclude-rules RF01,RF02,RF03
```

### Cache options

```bash
# SQLFluff caches rendered templates to speed up repeated linting.
# Cache is stored in .sqlfluff_cache/ by default.

# Disable cache (always re-render — useful for debugging)
sqlfluff lint models/ --disable-progress-bar

# Clear the cache
find . -name ".sqlfluff_cache" -type d -exec rm -rf {} +
```

### Profile-driven optimization

```ini
# .sqlfluff — production CI config optimized for speed
[sqlfluff]
dialect = trino
templater = dbt
max_line_length = 120
processes = 4

# Skip expensive reference-resolution rules in fast CI pass
# Run them separately in a nightly full scan
exclude_rules = RF01,RF02,RF03,RF04,ST05
```

### Targeted linting in CI (only changed files)

```bash
# Only lint SQL files that changed in this PR
CHANGED_FILES=$(git diff --name-only origin/main...HEAD | grep '\.sql$' | tr '\n' ' ')
if [ -n "$CHANGED_FILES" ]; then
    sqlfluff lint $CHANGED_FILES --dialect trino
else
    echo "No SQL files changed."
fi
```

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| `templater = dbt` without `dbt deps` | Macro packages not available; templater fails with import errors | Always run `dbt deps` before `sqlfluff lint` in CI |
| Running `sqlfluff lint` without `--dialect` (no `.sqlfluff`) | Falls back to `ansi` dialect; misses warehouse-specific syntax; many false positives | Always set `dialect` in `.sqlfluff` or pass `--dialect` |
| Using `processes = -1` with dbt templater | Each worker starts a full dbt subprocess; OOM on large projects | Cap at `processes = 4`; profile memory before scaling up |
| Excluding all rules to silence CI | Provides false confidence; CI gate becomes meaningless | Exclude only specific noisy rules (RF04, LT13 for dbt); keep core rules enabled |
| `-- noqa` on every line | Suppression sprawl; linter provides no value | Fix the violations or tune the rule configuration globally |
| Not pinning `sqlfluff` version | Minor version bumps add new rules, turning green CI red unexpectedly | Pin `sqlfluff==3.3.0` and `sqlfluff-templater-dbt==3.3.0` in `requirements.txt` |
| Using old `L0XX` rule codes in `.sqlfluff` | Codes still work as aliases but are deprecated; tooling may not recognize them | Migrate to semantic names: `LT01`, `AL01`, `CP01`, etc. |
| `template_blocks_indent = true` (default) with dbt | SQL inside `{% if %}` / `{% for %}` gets double-indented; LT02 fires on every model | Set `template_blocks_indent = false` in `[sqlfluff:indentation]` for dbt projects |
| Linting `dbt/target/` or `dbt_packages/` | Compiled SQL and package files are not user-written; violations are not actionable | Add `ignore = target,dbt_packages,.venv` to `[sqlfluff]` |
| VS Code extension using `templater = dbt` | dbt templater requires a live project context; editor linting stalls or fails | Use `templater = jinja` in VS Code; use `templater = dbt` in CI only |
| Running `sqlfluff fix --force` in CI without reviewing | Auto-fixes can introduce unexpected changes; committed without review | Run `fix --check` to detect fixable violations; apply fixes locally, not via CI |
| `max_line_length` set too low (e.g. 80) | Long table names and Jinja expressions routinely exceed 80 chars; LT05 fires constantly | Use 120 (standard) or 150 for Jinja-heavy dbt models |
| Not setting `profiles_dir` in `[sqlfluff:templater:dbt]` | SQLFluff looks in `~/.dbt/` — works locally, fails in CI where no home profile exists | Set `profiles_dir = ./` and provide `profiles.yml` in the repo (with env_var credentials) |

---

## References to Consult When Needed

- [SQLFluff Documentation](https://docs.sqlfluff.com/en/stable/)
- [SQLFluff Rule Reference](https://docs.sqlfluff.com/en/stable/reference/rules.html)
- [SQLFluff Configuration Reference](https://docs.sqlfluff.com/en/stable/reference/configuration.html)
- [SQLFluff: dbt Templater](https://docs.sqlfluff.com/en/stable/reference/templating.html#dbt-project-configuration)
- [SQLFluff: Custom Rules](https://docs.sqlfluff.com/en/stable/guides/custom_rules.html)
- [SQLFluff: GitHub Actions](https://docs.sqlfluff.com/en/stable/production/github_actions.html)
- [SQLFluff: Pre-commit hooks](https://docs.sqlfluff.com/en/stable/production/pre_commit.html)
- [SQLFluff GitHub](https://github.com/sqlfluff/sqlfluff)
- [sqlfluff-templater-dbt on PyPI](https://pypi.org/project/sqlfluff-templater-dbt/)
- [dbt: Docs on SQLFluff integration](https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles)
- [pre-commit: SQLFluff hooks](https://github.com/sqlfluff/sqlfluff/blob/main/.pre-commit-hooks.yaml)
- [stefanzweifel/git-auto-commit-action](https://github.com/stefanzweifel/git-auto-commit-action)
