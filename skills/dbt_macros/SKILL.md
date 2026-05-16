---
name: dbt-macros
description: dbt Jinja macros — macro authoring fundamentals, Jinja syntax (blocks/filters/tests), context variables (this/target/adapter/execute), run_query, adapter.dispatch for cross-database macros, generate_schema_name, hooks, custom materializations, dbt-utils patterns
---

# dbt Macros & Jinja

## When to Use

Activate this skill when the task involves:
- Writing dbt macros from scratch or improving existing ones
- Using Jinja control flow, filters, and variables in dbt SQL
- Implementing cross-database macros with `adapter.dispatch`
- Dynamic SQL generation via `run_query` / `execute`
- Overriding built-in dbt macros (`generate_schema_name`, `generate_alias_name`)
- Writing macros that call `adapter` methods (schema introspection, DDL)
- Organizing macros across files and packages

---

## Jinja Fundamentals

### Three Delimiters

```jinja
{{ expression }}    {# outputs a value into SQL #}
{% statement %}     {# control flow: for, if, set, do — produces no output #}
{# comment #}       {# stripped at compile time #}
```

### Variables

```jinja
{% set payment_methods = ["credit_card", "bank_transfer", "gift_card"] %}
{% set threshold = 1000 %}
{% set label = "high_value" if threshold > 500 else "low_value" %}
```

### Whitespace Control

Jinja preserves whitespace by default. Use `-` to strip it:

```jinja
{%- set x = 1 -%}          {# strip left AND right #}
{{- column_name -}}         {# strip around expression #}
```

### Filters

```jinja
{{ column | upper }}                    -- COLUMN
{{ column | lower }}                    -- column
{{ column | replace(" ", "_") }}        -- col_name
{{ items | join(", ") }}                -- a, b, c
{{ items | list | length }}             -- 3
{{ value | default("unknown") }}        -- 'unknown' if value is falsy
{{ items | reject("equalto", "x") | list }}   -- removes "x" from list
{{ text | trim }}                       -- strips leading/trailing whitespace
{{ value | int }}                       -- cast to Python int
{{ value | string }}                    -- cast to string
```

### Control Flow

```jinja
{# if/elif/else #}
{% if target.name == "prod" %}
    limit 1000000
{% elif target.name == "dev" %}
    limit 1000
{% else %}
    limit 100
{% endif %}

{# for loop with loop variable #}
{% for col in columns %}
    {{ col }}{%- if not loop.last -%},{%- endif %}
{% endfor %}

{# loop.index (1-based), loop.index0 (0-based), loop.first, loop.last #}
{% for col in columns %}
  {{ loop.index }}: {{ col }}
{% endfor %}
```

---

## Macro Basics

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name, scale=2) %}
    ({{ column_name }} / 100.0)::numeric(16, {{ scale }})
{% endmacro %}
```

Call in a model:
```sql
-- models/stg_payments.sql
select
    payment_id,
    {{ cents_to_dollars('amount') }}               as amount_usd,
    {{ cents_to_dollars('refund_amount', scale=4) }} as refund_usd
from {{ ref('raw_payments') }}
```

Compiled output:
```sql
select
    payment_id,
    (amount / 100.0)::numeric(16, 2)               as amount_usd,
    (refund_amount / 100.0)::numeric(16, 4)         as refund_usd
from analytics.raw_payments
```

### Macro Returning a Value

Use `return()` to return a value for use in `{% set %}`:

```jinja
{% macro is_incremental_model() %}
    {{ return(model.config.materialized == 'incremental') }}
{% endmacro %}

{% if is_incremental_model() %}
    -- incremental-only logic
{% endif %}
```

### `do` Tag — Side-Effect Calls

```jinja
{% do log("Starting transform for " ~ model.name, info=true) %}
{% do adapter.create_schema(api.Relation.create(database=target.database, schema=target.schema ~ "_tmp")) %}
```

---

## Context Variables

### `this` — Current Relation

```jinja
-- The current model's fully-qualified relation:
{{ this }}                  -- "analytics"."dbt_prod"."orders"
{{ this.database }}         -- "analytics"
{{ this.schema }}           -- "dbt_prod"
{{ this.identifier }}       -- "orders"
{{ this.include(database=false) }}  -- "dbt_prod"."orders"
```

Use `this` to reference the current table in incremental models:

```sql
{% if is_incremental() %}
    where event_at > (select max(event_at) from {{ this }})
{% endif %}
```

### `target` — Warehouse Connection

```jinja
{{ target.name }}       -- "dev" | "prod" | "ci"
{{ target.schema }}     -- "dbt_ivan"
{{ target.database }}   -- "analytics"
{{ target.type }}       -- "postgres" | "snowflake" | "bigquery" | "spark"
{{ target.threads }}    -- 4
```

Use to branch logic by environment:

```sql
{% if target.name == "prod" %}
    {{ config(materialized="table") }}
{% else %}
    {{ config(materialized="view") }}
{% endif %}
```

### `model` — Node Metadata

```jinja
{{ model.name }}              -- "stg_orders"
{{ model.config.schema }}     -- "staging"
{{ model.config.materialized }} -- "incremental"
{{ model.unique_id }}         -- "model.my_project.stg_orders"
{{ model.fqn }}               -- ["my_project", "staging", "stg_orders"]
{{ model.tags }}              -- ["daily", "finance"]
{{ model.refs }}              -- list of ref() calls in this model
```

### `execute` — Compile vs. Execute Guard

`execute` is `True` only when dbt is **running** the model (not during parsing or `dbt compile`). Always wrap `run_query` and DDL calls in `{% if execute %}`:

```jinja
{% if execute %}
    {% set results = run_query("select distinct region from dim_geo") %}
    {% set regions = results.columns[0].values() %}
{% else %}
    {% set regions = [] %}
{% endif %}

select
    order_id,
    {% for region in regions %}
    sum(case when region = '{{ region }}' then revenue end) as {{ region | lower | replace(' ', '_') }}_revenue
    {%- if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('fct_orders') }}
group by 1
```

### `var()` — Project Variables

```jinja
{# dbt_project.yml:
vars:
  lookback_days: 90
  start_date: "2023-01-01"
#}

{{ var('lookback_days') }}           -- 90
{{ var('lookback_days', default=30) }} -- 30 if not defined

-- Override at runtime:
-- dbt run --vars '{"lookback_days": 30}'
```

### `env_var()` — Environment Variables

```jinja
{{ env_var('DBT_SCHEMA_PREFIX', 'dbt_') }}       -- value or default
{{ env_var('DATABASE_PASSWORD') }}                -- required; raises if absent
```

---

## `run_query` — Dynamic SQL Execution

`run_query` sends SQL to the warehouse and returns an **agate Table** result. **Always guard with `{% if execute %}`**.

### agate Table Access

```jinja
{% set query %}
    select distinct
        payment_method,
        count(*) as cnt
    from {{ ref('stg_payments') }}
    group by 1
    order by 2 desc
{% endset %}

{% if execute %}
    {% set results = run_query(query) %}
    {% set methods = results.columns['payment_method'].values() %}
    {% set counts  = results.columns['cnt'].values() %}
{% else %}
    {% set methods = [] %}
    {% set counts  = [] %}
{% endif %}
```

| Access Pattern | Code |
|----------------|------|
| Column by index | `results.columns[0].values()` |
| Column by name | `results.columns['col_name'].values()` |
| Single value | `results.rows[0][0]` |
| Row count | `results \| length` |
| Print table | `results.print_table()` |

### DDL / DML Macros (non-SELECT)

```jinja
{% macro truncate_staging(table_name) %}
    {%- set schema = target.schema ~ "_staging" -%}
    {% if execute %}
        {% do run_query("truncate table " ~ schema ~ "." ~ table_name) %}
        {% do log("Truncated " ~ schema ~ "." ~ table_name, info=true) %}
    {% endif %}
{% endmacro %}
```

Only run during `dbt run` / `dbt build`, skip on `dbt compile` and `dbt docs generate`:

```jinja
{% if execute and flags.WHICH in ('run', 'build') %}
    {% do run_query("delete from " ~ this ~ " where created_at < current_date - 90") %}
{% endif %}
```

---

## `adapter` Methods

### Schema Introspection

```jinja
{# Get all columns in a relation #}
{%- set cols = adapter.get_columns_in_relation(this) -%}
{% for col in cols %}
    {{ col.name }} ({{ col.data_type }}, nullable={{ col.is_nullable }})
{% endfor %}

{# Check if a relation exists before referencing it #}
{%- set rel = adapter.get_relation(
        database=target.database,
        schema=target.schema,
        identifier='my_table') -%}
{% if rel is not none %}
    -- table exists, safe to select from it
    select * from {{ rel }}
{% endif %}
```

### DDL Operations

```jinja
{# Create a schema #}
{% do adapter.create_schema(
    api.Relation.create(database=target.database, schema=target.schema ~ "_audit")
) %}

{# Drop a relation #}
{%- set stale = adapter.get_relation(database=target.database, schema=target.schema, identifier='old_table') -%}
{% if stale is not none %}
    {% do adapter.drop_relation(stale) %}
{% endif %}

{# Rename a relation #}
{% do adapter.rename_relation(from_relation, to_relation) %}
```

### `adapter.dispatch` — Cross-Database Macros

The core pattern for writing macros that behave differently per SQL dialect:

```jinja
-- macros/safe_divide.sql
{% macro safe_divide(numerator, denominator) -%}
    {{ return(adapter.dispatch('safe_divide', 'my_project')(numerator, denominator)) }}
{%- endmacro %}

-- Default: works on PostgreSQL, Trino, DuckDB
{% macro default__safe_divide(numerator, denominator) %}
    case when {{ denominator }} = 0 then null
         else {{ numerator }}::float / {{ denominator }}
    end
{% endmacro %}

-- BigQuery uses SAFE_DIVIDE
{% macro bigquery__safe_divide(numerator, denominator) %}
    SAFE_DIVIDE({{ numerator }}, {{ denominator }})
{% endmacro %}

-- Spark SQL
{% macro spark__safe_divide(numerator, denominator) %}
    case when {{ denominator }} = 0 then null
         else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}
```

Dispatch resolution order for `postgres`:
1. `my_project.postgres__safe_divide`
2. `my_project.default__safe_divide`

#### Override a Package Macro via `dbt_project.yml`

```yaml
# dbt_project.yml
dispatch:
  - macro_namespace: dbt_utils
    search_order: ['my_project', 'dbt_utils']
  - macro_namespace: dbt
    search_order: ['my_project', 'my_org_macros', 'dbt']
```

With this config, `dbt_utils.generate_surrogate_key` will first look for `my_project.postgres__generate_surrogate_key` before falling back to `dbt_utils`.

---

## Cross-Database Built-in Macros (`dbt.*`)

These are the `dbt` namespace macros that abstract SQL dialect differences:

```sql
-- Date / time
{{ dbt.date_trunc("month", "created_at") }}             -- date_trunc('month', created_at)
{{ dbt.dateadd("day", 7, "created_at") }}               -- created_at + interval '7 day'
{{ dbt.datediff("start_date", "end_date", "day") }}     -- end_date::date - start_date::date
{{ dbt.current_timestamp() }}                           -- now() / current_timestamp / getdate()
{{ dbt.last_day("created_at", "month") }}               -- last day of month

-- Types
{{ dbt.type_string() }}        -- TEXT (PG) / VARCHAR (Snowflake) / STRING (BQ)
{{ dbt.type_timestamp() }}     -- TIMESTAMP
{{ dbt.type_bigint() }}        -- BIGINT
{{ dbt.type_int() }}           -- INT
{{ dbt.type_float() }}         -- FLOAT
{{ dbt.type_numeric() }}       -- NUMERIC(28,6)
{{ dbt.type_boolean() }}       -- BOOLEAN

-- Casting
{{ dbt.safe_cast("user_id", dbt.type_bigint()) }}       -- cast(user_id as BIGINT)
{{ dbt.cast("amount", dbt.type_numeric()) }}            -- cast(amount as NUMERIC(28,6))

-- Strings
{{ dbt.concat(["first_name", "' '", "last_name"]) }}    -- first_name || ' ' || last_name
{{ dbt.hash("order_id") }}                              -- md5(cast(order_id as varchar))
{{ dbt.length("email") }}                               -- length(email)
{{ dbt.replace("status", "'_'", "' '") }}              -- replace(status, '_', ' ')
{{ dbt.split_part("full_name", "' '", 1) }}            -- split_part(full_name, ' ', 1)

-- Aggregates
{{ dbt.listagg("tag", "','", "order by tag") }}         -- array_to_string(array_agg(tag order by tag), ',')
{{ dbt.any_value("description") }}                      -- any(description)
{{ dbt.bool_or("is_active") }}                          -- bool_or(is_active)
```

---

## Built-in Override Macros

### `generate_schema_name` — Custom Schema Logic

Override to prevent dbt from prefixing custom schemas with the target schema:

```jinja
-- macros/get_custom_schema.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {# In prod: use custom_schema_name directly, no prefix #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {# In dev: prefix with target.schema to isolate environments #}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

### `generate_alias_name` — Custom Alias Logic

```jinja
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is not none -%}
        {{ custom_alias_name | trim }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}
```

### `generate_database_name`

```jinja
{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {%- if custom_database_name is none -%}
        {{ target.database }}
    {%- else -%}
        {{ custom_database_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

---

## Production-Quality Macro Patterns

### 1. Dynamic PIVOT

```jinja
-- macros/pivot.sql
{% macro pivot(column, values, alias=true, agg='sum', then_value=1, else_value=0) %}
    {% for v in values %}
    {{ agg }}(case when {{ column }} = '{{ v }}' then {{ then_value }} else {{ else_value }} end)
    {%- if alias %} as {{ v | lower | replace(' ', '_') | replace('-', '_') }}{% endif %}
    {%- if not loop.last %},{% endif %}
    {% endfor %}
{% endmacro %}
```

```sql
-- Usage
select
    order_id,
    {{ pivot('payment_method', ['credit_card', 'bank_transfer', 'gift_card'], agg='sum', then_value='amount') }}
from {{ ref('stg_payments') }}
group by 1
```

### 2. Union All Tables by Pattern

```jinja
-- macros/union_relations.sql
{% macro union_relations(relations, exclude=[], column_override={}) %}
    {% set cols_list = [] %}
    {% for rel in relations %}
        {% set rel_cols = adapter.get_columns_in_relation(rel) %}
        {% for col in rel_cols %}
            {% if col.name | lower not in (exclude | map('lower') | list) %}
                {% do cols_list.append(col.name | lower) %}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {% set all_cols = cols_list | unique | list | sort %}

    {% for rel in relations %}
        {% set rel_cols = adapter.get_columns_in_relation(rel) | map(attribute='name') | map('lower') | list %}
        select
            {% for col in all_cols %}
            {% if col in rel_cols %}
                {{ col }}
            {% else %}
                null as {{ col }}
            {% endif %}
            {%- if not loop.last -%},{%- endif %}
            {% endfor %}
        from {{ rel }}
        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor %}
{% endmacro %}
```

### 3. Audit Columns Macro

```jinja
-- macros/audit_columns.sql
{% macro audit_columns() %}
    current_timestamp                           as dbt_loaded_at,
    '{{ invocation_id }}'                       as dbt_invocation_id,
    '{{ model.unique_id }}'                     as dbt_model_id
{% endmacro %}
```

```sql
-- models/marts/fct_orders.sql
select
    order_id,
    customer_id,
    total,
    {{ audit_columns() }}
from {{ ref('int_orders') }}
```

### 4. Grant Permissions After Build

```jinja
-- macros/grants.sql
{% macro grant_select(role, schema=none) %}
    {% set target_schema = schema or target.schema %}
    {% if execute and target.name == 'prod' %}
        {% set sql %}
            grant usage on schema {{ target.database }}.{{ target_schema }} to role {{ role }};
            grant select on all tables  in schema {{ target.database }}.{{ target_schema }} to role {{ role }};
            grant select on all views   in schema {{ target.database }}.{{ target_schema }} to role {{ role }};
        {% endset %}
        {% do run_query(sql) %}
        {% do log("Granted SELECT on " ~ target_schema ~ " to " ~ role, info=true) %}
    {% endif %}
{% endmacro %}
```

Use in `dbt_project.yml` post-hook:
```yaml
models:
  my_project:
    marts:
      +post-hook: "{{ grant_select(role='bi_reader') }}"
```

### 5. Idempotent Schema + Table Creation

```jinja
{% macro create_if_not_exists(schema, table_name, ddl_body) %}
    {% set rel = adapter.get_relation(
        database=target.database,
        schema=schema,
        identifier=table_name
    ) %}
    {% if rel is none %}
        {% set sql %}
            create table {{ target.database }}.{{ schema }}.{{ table_name }} (
                {{ ddl_body }}
            )
        {% endset %}
        {% do run_query(sql) %}
        {% do log("Created " ~ schema ~ "." ~ table_name, info=true) %}
    {% else %}
        {% do log(schema ~ "." ~ table_name ~ " already exists, skipping", info=true) %}
    {% endif %}
{% endmacro %}
```

### 6. Column Existence Guard

```jinja
-- macros/column_exists.sql
{% macro column_exists(relation, column_name) %}
    {%- set cols = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {{ return(column_name | lower in cols) }}
{% endmacro %}
```

```sql
-- models/stg_events.sql
select
    event_id,
    event_type,
    {% if column_exists(source('raw', 'events'), 'user_agent') %}
    user_agent,
    {% endif %}
    created_at
from {{ source('raw', 'events') }}
```

---

## Hooks and Operations

### `on-run-start` / `on-run-end`

```yaml
# dbt_project.yml
on-run-start:
  - "{{ logging.log_run_start() }}"

on-run-end:
  - "{{ grant_select(role='reporter') }}"
  - "{{ logging.log_run_end() }}"
```

### `pre-hook` / `post-hook` on Models

```yaml
models:
  my_project:
    staging:
      +pre-hook:
        - "{{ truncate_if_exists(this) }}"
      +post-hook:
        - "analyze {{ this }}"
        - "{{ grant_select(role='analysts') }}"
```

### Operations — Run Macros Without Models

```bash
# Run a macro directly (no model output)
dbt run-operation grant_select --args '{role: bi_team}'
dbt run-operation truncate_staging --args '{table_name: stg_events}'
```

---

## Documenting Macros

```yaml
# macros/schema.yml
macros:
  - name: cents_to_dollars
    description: Converts integer cents to decimal dollars.
    arguments:
      - name: column_name
        type: column name (string)
        description: Column containing the cent value.
      - name: scale
        type: integer
        description: Decimal places in the output. Default is 2.

  - name: safe_divide
    description: >
      Divides numerator by denominator, returning NULL when denominator is 0.
      Cross-database via adapter.dispatch.
    arguments:
      - name: numerator
        type: expression
        description: The dividend expression.
      - name: denominator
        type: expression
        description: The divisor expression.
```

---

## Anti-Patterns

1. **Calling `run_query` without `{% if execute %}`** — runs during `dbt compile` and `dbt docs generate`, causing unintended warehouse queries. Always guard with `{% if execute %}`.

2. **Hardcoding schema names in macros** — `analytics.my_table` breaks across environments. Use `{{ this }}`, `{{ target.schema }}`, or `{{ ref() }}` instead.

3. **Using Python string quoting for SQL values** — `{{ cents_to_dollars(amount) }}` (no quotes) treats `amount` as a Jinja variable (undefined). Always quote column names: `{{ cents_to_dollars('amount') }}`.

4. **Writing adapter-specific SQL in generic macros** — `date_part(...)` works on Postgres but breaks on BigQuery. Use `dbt.date_trunc()` and `dbt.dateadd()` built-ins, or `adapter.dispatch`.

5. **One macro file per macro** — clutters the `macros/` directory. Group related macros by domain: `macros/dates.sql`, `macros/grants.sql`, `macros/schema_overrides.sql`.

6. **Overusing Jinja where SQL suffices** — Jinja adds cognitive overhead. If the logic can be a CTE or window function, keep it in SQL. Use macros only for genuinely reusable or cross-database patterns.

7. **Using `flags.WHICH` checks as the only protection** — `flags.WHICH` is internal API and may change. Prefer `{% if execute %}` as the primary guard; use `flags.WHICH` only when you need command-specific behavior.

8. **Not returning from macros meant to produce a value** — `{% macro foo() %} ... {% endmacro %}` always returns a string (including whitespace). Use `{{ return(value) }}` for macros that return scalars or lists used in `{% set %}`.

9. **Ignoring whitespace in generated SQL** — extra blank lines and spaces make compiled SQL hard to read and debug. Use `{%- -%}` whitespace control, especially inside loops.

10. **Not documenting macros in `schema.yml`** — undocumented macros become mystery functions. Document all public macros with arguments and descriptions.

---

## References to Consult When Needed

- dbt Jinja macros overview: `docs.getdbt.com/docs/build/jinja-macros`
- dbt Jinja context variables: `docs.getdbt.com/reference/dbt-jinja-functions`
- `adapter.dispatch` reference: `docs.getdbt.com/reference/dbt-jinja-functions/dispatch`
- Cross-database built-ins: `docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros`
- `run_query` reference: `docs.getdbt.com/reference/dbt-jinja-functions/run_query`
- Jinja template designer docs: `jinja.palletsprojects.com/en/3.1.x/templates/`
- dbt-utils package: `github.com/dbt-labs/dbt-utils`
