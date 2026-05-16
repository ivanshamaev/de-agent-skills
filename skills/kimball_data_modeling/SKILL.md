---
name: kimball_data_modeling
description: Use when designing or implementing dimensional data models following Kimball methodology — fact tables (transaction/snapshot/accumulating), dimension tables, SCD types (1/2/3/4/6), star/snowflake schemas, surrogate keys, conformed dimensions, role-playing dimensions, junk dimensions, bridge tables, date dimensions, degenerate dimensions, DDL patterns, and DML update strategies for loading and maintaining dimensional tables.
---

# Kimball Dimensional Data Modeling

## When to Use

Use this skill when:
- Designing a new data warehouse or data mart schema
- Choosing between fact table types (transaction / periodic snapshot / accumulating snapshot)
- Designing dimension tables and choosing an SCD strategy
- Writing DDL for fact and dimension tables in a lakehouse or DWH
- Implementing DML load patterns: initial load, incremental append, SCD1/2/3 updates
- Deciding on surrogate key strategy, grain, or measure type
- Implementing specific dimension patterns: date dim, conformed dims, role-playing, junk, bridge
- Reviewing or refactoring an existing dimensional model

---

## Core Principles

Kimball dimensional modeling organizes data around **business processes**. Every model answers one question: *what business event or state does this table describe?*

Four-step design process (always in this order):

1. **Identify the business process** — what event are we measuring? (orders placed, inventory levels, payments received)
2. **Declare the grain** — what does one row represent? The grain must be atomic: as detailed as the source data allows
3. **Identify the dimensions** — what context describes the event? (who, what, where, when, how)
4. **Identify the facts** — what numeric measures characterize the event? (amount, quantity, duration)

**The grain is the most critical decision.** Everything else follows from it. Never mix grains in one fact table.

---

## Schema Types

### Star Schema

One central fact table connected directly to denormalized dimension tables. Every join is exactly one hop from fact to dim. Optimal for query performance and comprehensibility.

```
               dim_date
                  │
dim_customer ─── fct_orders ─── dim_product
                  │
              dim_channel
```

### Snowflake Schema

Dimension tables are normalized into sub-dimensions. Saves storage, but adds joins and is harder to understand. Generally **avoid** unless storage is critically constrained or a dimension hierarchy is queried independently.

```
dim_country ─── dim_customer ─── fct_orders ─── dim_product ─── dim_category
```

**Recommendation**: Always start with a star schema. Snowflake only for very large dimension tables with clear hierarchical query patterns.

### Fact Constellation (Galaxy Schema)

Multiple fact tables sharing conformed dimensions. Standard for enterprise DWH with multiple business processes.

```
dim_date ─── fct_orders ─── dim_product
    │                            │
    └──── fct_inventory ─────────┘
```

---

## Fact Tables

### Types of Fact Tables

#### Transaction Fact Table

One row per **discrete business event** at the finest grain. Append-only. The most common type.

- Events: orders placed, payments made, log entries, sensor readings
- Rows never change after load (immutable)
- Additive measures across all dimensions

```sql
create table fct_order_lines (
    -- Surrogate keys (FK to dims)
    order_line_sk       bigint        not null,   -- own surrogate key
    order_date_sk       int           not null,
    ship_date_sk        int,                       -- nullable: not yet shipped
    customer_sk         bigint        not null,
    product_sk          bigint        not null,
    channel_sk          int           not null,
    -- Degenerate dimension
    order_id            varchar(20)   not null,    -- source order identifier
    order_line_number   int           not null,
    -- Measures
    quantity            int           not null,
    unit_price          decimal(12,2) not null,
    discount_amount     decimal(12,2) not null  default 0,
    extended_amount     decimal(12,2) not null,   -- quantity * unit_price - discount
    cost_amount         decimal(12,2),
    -- Audit
    etl_loaded_at       timestamp     not null
)
-- partitioned by order_date for efficient range queries
```

#### Periodic Snapshot Fact Table

One row per **entity per time period** — captures the state at regular intervals regardless of activity. Good for balance sheets, inventory levels, KPI dashboards.

- Grain: one row per account per day, one row per product per week
- Rows accumulate; each period adds new rows
- Semi-additive measures (sum across products but not across time periods)

```sql
create table fct_account_daily_balance (
    snapshot_date_sk    int           not null,
    account_sk          bigint        not null,
    -- Semi-additive measures (SUM across accounts = valid; SUM across dates = wrong)
    balance             decimal(18,2) not null,
    available_credit    decimal(18,2),
    days_delinquent     int           not null  default 0,
    -- Fully additive within a period
    transactions_count  int           not null  default 0,
    deposits_amount     decimal(18,2) not null  default 0,
    withdrawals_amount  decimal(18,2) not null  default 0,
    -- Audit
    etl_loaded_at       timestamp     not null,
    primary key (snapshot_date_sk, account_sk)
)
```

#### Accumulating Snapshot Fact Table

One row per **pipeline instance** that is **updated** as the process advances through stages. Captures workflow progress with multiple date milestones.

- Grain: one row per order, loan application, shipment, HR onboarding process
- Rows are **updated** (not appended) as milestones are reached
- Contains multiple date FKs, many nullable until the stage is reached
- Lag measures (duration between milestones) are meaningful

```sql
create table fct_order_fulfillment (
    order_sk                bigint        not null,
    -- One date FK per pipeline milestone
    order_placed_date_sk    int           not null,
    payment_received_date_sk int,                    -- null until paid
    warehouse_picked_date_sk int,
    shipped_date_sk          int,
    delivered_date_sk        int,
    returned_date_sk         int,                    -- null if not returned
    -- Dimension FKs
    customer_sk             bigint        not null,
    channel_sk              int           not null,
    -- Lag measures (days between stages)
    days_to_payment         int,
    days_to_ship            int,
    days_to_deliver         int,
    -- Financial measures
    order_amount            decimal(12,2) not null,
    -- Status
    pipeline_status         varchar(20)   not null,  -- placed | paid | shipped | delivered | returned
    -- Audit
    etl_loaded_at           timestamp     not null,
    etl_updated_at          timestamp     not null,
    primary key (order_sk)
)
```

#### Factless Fact Table

Records **events with no numeric measures** — the presence of a row is itself the fact.

```sql
-- Attendance / coverage events
create table fct_student_attendance (
    attendance_date_sk  int     not null,
    student_sk          bigint  not null,
    course_sk           int     not null,
    status              varchar(10) not null,  -- present | absent | late
    primary key (attendance_date_sk, student_sk, course_sk)
);

-- Eligible promotions bridge (which customers qualify for which promotions)
create table fct_promotion_coverage (
    promo_start_date_sk int    not null,
    customer_sk         bigint not null,
    promotion_sk        int    not null
);
```

### Measure Types

| Type | Definition | Sum across dims? | Sum across time? | Example |
|---|---|---|---|---|
| **Additive** | Sums meaningfully in all directions | ✅ | ✅ | revenue, quantity, cost |
| **Semi-additive** | Sums across some dims, not time | ✅ | ❌ | account balance, inventory level |
| **Non-additive** | Cannot be summed at all | ❌ | ❌ | ratios, percentages, unit price |
| **Derived** | Calculated from other measures | — | — | margin = revenue − cost |

**Non-additive** measures: store the components (numerator/denominator), not the ratio. Compute ratios in SQL at query time:

```sql
-- Store these (additive)
revenue_amount  decimal(12,2),
cost_amount     decimal(12,2),
quantity        int,

-- Compute at query time (non-additive)
-- margin_pct = sum(revenue_amount - cost_amount) / sum(revenue_amount)
-- unit_price  = sum(revenue_amount) / sum(quantity)
```

---

## Dimension Tables

A dimension provides the **context** for interpreting fact measures. Every dimension row answers "who / what / where / when / how / why" about the fact.

### Structure

```sql
create table dim_customer (
    -- Surrogate key: system-generated, immutable, integer (not the business key)
    customer_sk         bigint        not null,   -- primary key
    -- Natural / business key: from the source system
    customer_id         varchar(20)   not null,   -- source CRM id
    -- Current-value attributes
    full_name           varchar(200),
    email               varchar(200),
    country_code        char(2),
    city                varchar(100),
    customer_segment    varchar(50),
    -- SCD2 metadata columns (when using SCD Type 2)
    effective_from      date          not null,
    effective_to        date,                     -- null = currently active
    is_current          boolean       not null  default true,
    -- Audit
    etl_loaded_at       timestamp     not null,
    primary key (customer_sk)
);
```

### Surrogate Keys

**Always use a surrogate key** (system-generated integer/bigint) as the dimension PK, not the natural/business key.

Reasons:
- Natural keys change (customer numbers get reused, product codes change)
- SCD Type 2 requires multiple rows per natural key — surrogate key distinguishes them
- Integer joins are faster than string joins
- Insulates the DWH from source-system key changes

```sql
-- Good: surrogate key PK, business key as regular column
create table dim_product (
    product_sk      bigint primary key,   -- surrogate
    product_id      varchar(20) not null, -- source natural key
    ...
);

-- Bad: business key as PK — breaks SCD2 and is fragile
create table dim_product (
    product_id      varchar(20) primary key,  -- don't do this
    ...
);
```

**Surrogate key generation**:
- Sequence / identity column in the DWH
- Hash of natural key + effective date: `md5(concat(customer_id, '|', effective_from))` — deterministic, useful for distributed loads
- UUID — works but joins are slower than integers

### Dimension Attributes — Design Rules

1. **Denormalize hierarchies** into the dimension row (customer → city → region → country all in `dim_customer`). Avoids snowflaking and speeds up GROUP BY on hierarchical levels.
2. **Store descriptive labels alongside codes**: `channel_code = 'WEB'` AND `channel_name = 'Web Store'`. BI tools display labels; queries filter on codes.
3. **No nulls in dimension attributes** — use meaningful defaults: `'Unknown'`, `'N/A'`, `'Not Applicable'`. Null in a dimension attribute breaks GROUP BY grouping.
4. **Limit VARCHAR lengths** to realistic maximums — do not use `TEXT` or unlimited `VARCHAR` for dimension attributes.
5. **Flag columns** should be `boolean` or single-char `char(1)` with label column: `is_active boolean` + `active_label varchar(10)`.

---

## Slowly Changing Dimensions (SCD)

### SCD Type 0 — Retain Original

Dimension attributes never change after initial load. Use for truly static reference data.

```sql
-- Dates, historical classifications, immutable reference codes
create table dim_fiscal_calendar (
    date_sk         int          primary key,
    calendar_date   date         not null,
    fiscal_year     int          not null,
    fiscal_quarter  int          not null,
    fiscal_month    int          not null
    -- No update columns — loaded once, never changed
);
```

### SCD Type 1 — Overwrite

The current value replaces the old value. **No history is kept.** Use when history is irrelevant or actively undesirable.

Good for: correcting data errors, updating contact info where history doesn't matter, derived attributes.

```sql
-- DDL: no history columns needed
create table dim_customer_scd1 (
    customer_sk     bigint        primary key,
    customer_id     varchar(20)   not null unique,  -- natural key, unique here
    full_name       varchar(200),
    email           varchar(200),
    country_code    char(2),
    segment         varchar(50),
    etl_updated_at  timestamp     not null
);
```

**DML — MERGE for SCD1 upsert** (Trino/Spark SQL syntax):

```sql
merge into dim_customer_scd1 as target
using (
    select
        customer_id,
        full_name,
        email,
        country_code,
        segment,
        current_timestamp as etl_updated_at
    from staging.stg_customers
) as source
on target.customer_id = source.customer_id
when matched and (
    target.full_name   <> source.full_name   or
    target.email       <> source.email       or
    target.country_code<> source.country_code or
    target.segment     <> source.segment
) then update set
    full_name       = source.full_name,
    email           = source.email,
    country_code    = source.country_code,
    segment         = source.segment,
    etl_updated_at  = source.etl_updated_at
when not matched then insert (
    customer_sk, customer_id, full_name, email, country_code, segment, etl_updated_at
) values (
    next value for seq_customer_sk,
    source.customer_id, source.full_name, source.email,
    source.country_code, source.segment, source.etl_updated_at
);
```

**DML — UPDATE + INSERT split** (when MERGE is unavailable):

```sql
-- Step 1: update changed rows
update dim_customer_scd1
set
    full_name       = s.full_name,
    email           = s.email,
    country_code    = s.country_code,
    segment         = s.segment,
    etl_updated_at  = current_timestamp
from staging.stg_customers s
where dim_customer_scd1.customer_id = s.customer_id
  and (
      dim_customer_scd1.full_name    <> s.full_name   or
      dim_customer_scd1.email        <> s.email        or
      dim_customer_scd1.country_code <> s.country_code or
      dim_customer_scd1.segment      <> s.segment
  );

-- Step 2: insert new rows
insert into dim_customer_scd1
    (customer_sk, customer_id, full_name, email, country_code, segment, etl_updated_at)
select
    next value for seq_customer_sk,
    s.customer_id, s.full_name, s.email, s.country_code, s.segment, current_timestamp
from staging.stg_customers s
where not exists (
    select 1 from dim_customer_scd1 d where d.customer_id = s.customer_id
);
```

### SCD Type 2 — Add New Row (Full History)

A new row is inserted for every change. The old row is closed by setting `effective_to` and `is_current = false`. The fact table FK always points to the surrogate key of the row that was current **at the time of the event**.

Use when: you need to query "what did we know about the customer at the time of the order?", customer segment / geography changes matter historically.

```sql
-- DDL: includes history columns
create table dim_customer_scd2 (
    customer_sk     bigint        primary key,   -- surrogate, unique per version
    customer_id     varchar(20)   not null,      -- natural key, NOT unique (multiple rows)
    full_name       varchar(200),
    email           varchar(200),
    country_code    char(2),
    segment         varchar(50),
    -- SCD2 versioning columns
    effective_from  date          not null,
    effective_to    date,                        -- null = currently active row
    is_current      boolean       not null,
    -- Row hash for fast change detection
    row_hash        varchar(64)   not null,
    -- Audit
    etl_loaded_at   timestamp     not null
);

-- Index for fast lookup of current version by natural key
create index idx_dim_customer_scd2_nk on dim_customer_scd2 (customer_id, is_current);
```

**DML — SCD2 MERGE pattern** (two-statement approach, portable):

```sql
-- Step 1: expire rows that changed
update dim_customer_scd2
set
    effective_to = current_date - interval '1' day,
    is_current   = false
where is_current = true
  and customer_id in (
      select s.customer_id
      from staging.stg_customers s
      join dim_customer_scd2 d
        on d.customer_id = s.customer_id
       and d.is_current  = true
      where d.row_hash <> md5(concat_ws('|',
            s.full_name, s.email, s.country_code, s.segment))
  );

-- Step 2: insert new versions (for changed rows) + new rows
insert into dim_customer_scd2
    (customer_sk, customer_id, full_name, email, country_code, segment,
     effective_from, effective_to, is_current, row_hash, etl_loaded_at)
select
    -- surrogate key generation via hash (deterministic, no sequence needed)
    abs(farm_fingerprint(concat(s.customer_id, '|', cast(current_date as varchar)))) as customer_sk,
    s.customer_id,
    s.full_name,
    s.email,
    s.country_code,
    s.segment,
    current_date            as effective_from,
    null                    as effective_to,
    true                    as is_current,
    md5(concat_ws('|', s.full_name, s.email, s.country_code, s.segment)) as row_hash,
    current_timestamp       as etl_loaded_at
from staging.stg_customers s
where not exists (
    -- no active row with this exact hash
    select 1 from dim_customer_scd2 d
    where d.customer_id = s.customer_id
      and d.is_current  = true
      and d.row_hash    = md5(concat_ws('|', s.full_name, s.email, s.country_code, s.segment))
);
```

**DML — SCD2 using dbt snapshot** (recommended for dbt projects):

```sql
-- snapshots/snp_customers.sql
{% snapshot snp_customers %}
{{
    config(
        unique_key   = 'customer_id',
        strategy     = 'check',
        check_cols   = ['full_name', 'email', 'country_code', 'segment'],
        -- or: strategy='timestamp', updated_at='updated_at'
        invalidate_hard_deletes = true,
    )
}}
select * from {{ source('crm', 'customers') }}
{% endsnapshot %}
```

**Querying SCD2 — join to current version:**

```sql
-- Always filter is_current=true when you want current state
select c.full_name, c.segment, sum(o.extended_amount) as revenue
from fct_order_lines o
join dim_customer_scd2 c
    on o.customer_sk = c.customer_sk
    -- no is_current filter needed in fact joins:
    -- the FK already points to the correct version at event time
group by 1, 2;

-- Get customer current state
select * from dim_customer_scd2 where is_current = true;

-- Reconstruct customer history
select customer_id, segment, effective_from, coalesce(effective_to, date '9999-12-31') as effective_to
from dim_customer_scd2
where customer_id = 'CUST-001'
order by effective_from;
```

### SCD Type 3 — Previous Value Column

Adds one extra column to hold the **previous value** of a changing attribute. Supports exactly one level of history (current + previous). Use when "what changed" is more important than full history.

Good for: reorganisations, territory reassignments, product category reclassifications.

```sql
-- DDL
create table dim_salesperson_scd3 (
    salesperson_sk      bigint        primary key,
    employee_id         varchar(20)   not null unique,
    full_name           varchar(200),
    -- Current territory
    territory_code      varchar(10)   not null,
    territory_name      varchar(100)  not null,
    -- Previous territory (SCD3)
    prev_territory_code varchar(10),
    prev_territory_name varchar(100),
    territory_changed_at date,
    -- Audit
    etl_updated_at      timestamp
);
```

**DML — SCD3 update**:

```sql
update dim_salesperson_scd3
set
    prev_territory_code = territory_code,
    prev_territory_name = territory_name,
    territory_code      = s.territory_code,
    territory_name      = s.territory_name,
    territory_changed_at= current_date,
    etl_updated_at      = current_timestamp
from staging.stg_salespersons s
where dim_salesperson_scd3.employee_id = s.employee_id
  and dim_salesperson_scd3.territory_code <> s.territory_code;
```

### SCD Type 4 — Mini-Dimension

Carves out rapidly changing attributes into a separate **mini-dimension** table to avoid explosive SCD2 row growth on a large dimension.

Scenario: `dim_customer` has 50M rows. Adding age band, income band, and credit score creates SCD2 versions that would triple the table size.

```sql
-- Large, slowly changing core dimension
create table dim_customer (
    customer_sk         bigint primary key,
    customer_id         varchar(20)  not null unique,
    full_name           varchar(200),
    email               varchar(200),
    -- No rapidly-changing attributes here
    etl_loaded_at       timestamp
);

-- Small mini-dimension for rapidly-changing profile attributes
create table dim_customer_profile (
    profile_sk          int          primary key,    -- small integer
    age_band            varchar(20)  not null,       -- '18-24', '25-34', etc.
    income_band         varchar(20)  not null,
    credit_score_band   varchar(20)  not null,
    -- Enumerate all combinations: ~50 rows total
    unique (age_band, income_band, credit_score_band)
);

-- Fact table references both
create table fct_transactions (
    transaction_sk      bigint        primary key,
    transaction_date_sk int           not null,
    customer_sk         bigint        not null,      -- FK to dim_customer
    profile_sk          int           not null,      -- FK to dim_customer_profile (current at event time)
    amount              decimal(12,2) not null
);
```

### SCD Type 6 — Hybrid (1 + 2 + 3)

Combines SCD2 (full history via new rows) with SCD1 (current value always available on every row) and SCD3 (previous value). Every row always has the current attribute value, enabling queries without filtering `is_current`.

```sql
create table dim_customer_scd6 (
    customer_sk         bigint        primary key,
    customer_id         varchar(20)   not null,
    -- Historical value at this version (changes with each new row — SCD2)
    segment_at_version  varchar(50)   not null,
    -- Current value on ALL rows, updated in-place (SCD1 behavior)
    segment_current     varchar(50)   not null,
    -- Previous value (SCD3)
    segment_previous    varchar(50),
    -- SCD2 versioning
    effective_from      date          not null,
    effective_to        date,
    is_current          boolean       not null,
    etl_loaded_at       timestamp     not null
);
```

**When a segment changes:**
1. Expire old row (set `effective_to`, `is_current=false`) — SCD2
2. Insert new row with new `segment_at_version` — SCD2
3. UPDATE ALL rows for this `customer_id` to set `segment_current` = new value — SCD1
4. Set `segment_previous` = old value on the new row — SCD3

**DML — SCD6 load**:

```sql
-- Step 1: expire old rows (SCD2)
update dim_customer_scd6
set effective_to = current_date - interval '1' day,
    is_current   = false
where is_current = true
  and customer_id in (
      select s.customer_id from staging.stg_customers s
      join dim_customer_scd6 d on d.customer_id = s.customer_id and d.is_current = true
      where d.segment_at_version <> s.segment
  );

-- Step 2: insert new version rows (SCD2 + SCD3)
insert into dim_customer_scd6
    (customer_sk, customer_id, segment_at_version, segment_current, segment_previous,
     effective_from, effective_to, is_current, etl_loaded_at)
select
    next value for seq_customer_sk,
    s.customer_id,
    s.segment               as segment_at_version,
    s.segment               as segment_current,
    d_old.segment_at_version as segment_previous,
    current_date, null, true, current_timestamp
from staging.stg_customers s
left join dim_customer_scd6 d_old
    on d_old.customer_id = s.customer_id and d_old.effective_to = current_date - interval '1' day
where not exists (
    select 1 from dim_customer_scd6 d
    where d.customer_id = s.customer_id and d.is_current = true
      and d.segment_at_version = s.segment
);

-- Step 3: propagate current value to ALL historical rows (SCD1 backfill)
update dim_customer_scd6
set segment_current = s.segment
from staging.stg_customers s
where dim_customer_scd6.customer_id = s.customer_id;
```

### SCD Type Comparison

| Type | History | Storage | Complexity | Best For |
|---|---|---|---|---|
| **0** | None (static) | Minimal | Trivial | Reference codes, calendar dates |
| **1** | None (overwrite) | Minimal | Low | Error corrections, non-historical attributes |
| **2** | Full | High | Medium | Analytical accuracy: "what was true at the time?" |
| **3** | One previous value | Low | Low | Single-level before/after reporting |
| **4** | Rapid-change attributes split out | Medium | Medium | Large dimensions with volatile attributes |
| **6** | Full + current always on row | Very high | High | Need both historical and current in same query without filtering `is_current` |

---

## Common Dimension Patterns

### Date Dimension

One row per calendar date. The most universal and most critical dimension. Always pre-build it to cover 10+ years. Never use `current_date - interval X` in fact queries — join to `dim_date` instead.

```sql
create table dim_date (
    date_sk             int           primary key,    -- YYYYMMDD integer (20240115)
    calendar_date       date          not null unique,
    -- Calendar attributes
    day_of_week         int           not null,       -- 1=Mon, 7=Sun
    day_name            varchar(10)   not null,       -- 'Monday'
    day_name_short      char(3)       not null,       -- 'Mon'
    day_of_month        int           not null,
    day_of_year         int           not null,
    week_of_year        int           not null,
    month_number        int           not null,
    month_name          varchar(10)   not null,
    month_name_short    char(3)       not null,
    quarter_number      int           not null,
    quarter_name        char(2)       not null,       -- 'Q1'
    year_number         int           not null,
    -- Convenience
    is_weekend          boolean       not null,
    is_weekday          boolean       not null,
    is_holiday          boolean       not null  default false,
    holiday_name        varchar(100),
    -- Fiscal calendar (if different from calendar year)
    fiscal_year         int,
    fiscal_quarter      int,
    fiscal_month        int,
    fiscal_week         int,
    -- Period start/end flags
    is_month_start      boolean       not null,
    is_month_end        boolean       not null,
    is_quarter_start    boolean       not null,
    is_quarter_end      boolean       not null,
    is_year_start       boolean       not null,
    is_year_end         boolean       not null,
    -- Relative (useful for BI filters)
    days_ago            int,                          -- recomputed daily
    months_ago          int,
    -- YYYYMM integer for month-level joins
    year_month_sk       int           not null        -- 202401
);

-- Generate date dimension rows (Trino / Spark SQL)
insert into dim_date
select
    cast(date_format(d, '%Y%m%d') as int)                          as date_sk,
    d                                                               as calendar_date,
    day_of_week(d)                                                  as day_of_week,
    date_format(d, '%W')                                            as day_name,
    upper(substr(date_format(d, '%W'), 1, 3))                       as day_name_short,
    day(d)                                                          as day_of_month,
    day_of_year(d)                                                  as day_of_year,
    week(d)                                                         as week_of_year,
    month(d)                                                        as month_number,
    date_format(d, '%M')                                            as month_name,
    upper(substr(date_format(d, '%M'), 1, 3))                       as month_name_short,
    quarter(d)                                                      as quarter_number,
    concat('Q', cast(quarter(d) as varchar))                        as quarter_name,
    year(d)                                                         as year_number,
    day_of_week(d) in (6, 7)                                        as is_weekend,
    day_of_week(d) not in (6, 7)                                    as is_weekday,
    false                                                           as is_holiday,
    null                                                            as holiday_name,
    null as fiscal_year, null as fiscal_quarter,
    null as fiscal_month, null as fiscal_week,
    d = date_trunc('month', d)                                      as is_month_start,
    d = last_day_of_month(d)                                        as is_month_end,
    d = date_trunc('quarter', d)                                    as is_quarter_start,
    d = last_day_of_month(d + interval '2' month - interval '1' month
            + interval '2' month)                                   as is_quarter_end,
    month(d) = 1 and day(d) = 1                                     as is_year_start,
    month(d) = 12 and day(d) = 31                                   as is_year_end,
    null as days_ago,
    null as months_ago,
    cast(date_format(d, '%Y%m') as int)                             as year_month_sk
from (
    select date '2020-01-01' + sequence(0, 3650) as dates
) cross join unnest(dates) as t(d);
```

**Using the date dimension in queries:**
```sql
-- Revenue by month and quarter
select
    d.year_number,
    d.quarter_name,
    d.month_name,
    sum(f.extended_amount) as revenue
from fct_order_lines f
join dim_date d on f.order_date_sk = d.date_sk
where d.year_number = 2024
group by 1, 2, 3
order by 1, d.month_number;

-- Last 90 days (use dim_date, not date arithmetic)
select sum(f.extended_amount) from fct_order_lines f
join dim_date d on f.order_date_sk = d.date_sk
where d.days_ago <= 90;
```

### Conformed Dimensions

A **conformed dimension** has the same structure, grain, and values across multiple fact tables. It enables "drill-across" queries that join two fact tables through the shared dimension.

```sql
-- dim_date, dim_product, dim_customer used in both fct_orders and fct_returns
-- Drill-across: compare orders vs returns by product
select
    p.product_name,
    sum(o.extended_amount)   as ordered_amount,
    sum(r.return_amount)     as returned_amount,
    sum(r.return_amount) / nullif(sum(o.extended_amount), 0) as return_rate
from dim_product p
left join fct_order_lines o on o.product_sk = p.product_sk
left join fct_returns     r on r.product_sk = p.product_sk
group by 1;
```

**Rule**: if the same dimension concept exists in multiple facts, it must be the *exact same* dimension table with the same surrogate key. Never create `dim_customer_orders` and `dim_customer_returns` as separate tables.

### Role-Playing Dimensions

The same dimension table is joined multiple times under different aliases to represent different roles in the fact table.

```sql
-- fct_order_lines has order_date_sk, ship_date_sk, delivery_date_sk — all FK to dim_date
select
    order_date.calendar_date    as order_date,
    ship_date.calendar_date     as ship_date,
    delivery_date.calendar_date as delivery_date,
    datediff('day', order_date.calendar_date, ship_date.calendar_date) as days_to_ship,
    f.extended_amount
from fct_order_lines f
join dim_date as order_date    on f.order_date_sk    = order_date.date_sk
left join dim_date as ship_date     on f.ship_date_sk     = ship_date.date_sk
left join dim_date as delivery_date on f.delivery_date_sk = delivery_date.date_sk;
```

### Degenerate Dimension

A dimension attribute stored **directly in the fact table** because it has no other attributes to justify a separate dimension table. Most common: source transaction identifiers.

```sql
-- order_id and order_line_number are degenerate dimensions in fct_order_lines
-- They identify the source record but have no descriptive attributes
create table fct_order_lines (
    ...
    order_id            varchar(20)  not null,    -- degenerate dim
    order_line_number   int          not null,    -- degenerate dim
    ...
);
```

### Junk Dimension

Groups miscellaneous **low-cardinality flags and indicators** into one dimension to avoid many tiny foreign keys in the fact table.

```sql
-- Without junk dim: 5 separate tiny FKs in fact table
-- With junk dim: one FK, all combinations pre-enumerated

create table dim_order_flags (
    flags_sk            int           primary key,
    is_gift_order       boolean       not null,
    is_first_order      boolean       not null,
    is_promotional      boolean       not null,
    payment_method      varchar(20)   not null,   -- cash | card | wallet
    order_channel       varchar(20)   not null,   -- web | mobile | store
    unique (is_gift_order, is_first_order, is_promotional, payment_method, order_channel)
);
-- Total rows = 2 × 2 × 2 × 3 × 3 = 72 combinations — pre-load all of them
```

### Bridge Table (Many-to-Many)

Resolves many-to-many relationships between facts and dimensions. Includes a **weighting factor** when the contribution of each member must be distributed.

```sql
-- Order → Products (multi-product orders)
-- Customer → Accounts (customer has multiple accounts)

create table bridge_customer_account (
    customer_sk         bigint        not null,
    account_sk          bigint        not null,
    weighting_factor    decimal(8,6)  not null default 1.0,  -- share of credit
    primary key (customer_sk, account_sk)
);

-- Query with weight distribution
select
    c.full_name,
    sum(f.balance * b.weighting_factor) as weighted_balance
from fct_account_daily_balance f
join bridge_customer_account b on f.account_sk = b.account_sk
join dim_customer c on b.customer_sk = c.customer_sk
where f.snapshot_date_sk = 20240115
group by 1;
```

---

## Fact Table Load Patterns

### Initial Load (Full Table)

```sql
-- Create the fact table, then load all history
insert into fct_order_lines
    (order_line_sk, order_date_sk, customer_sk, product_sk, channel_sk,
     order_id, order_line_number, quantity, unit_price, extended_amount, etl_loaded_at)
select
    abs(farm_fingerprint(concat(s.order_id, '|', cast(s.line_number as varchar)))) as order_line_sk,
    d.date_sk                                   as order_date_sk,
    c.customer_sk                               as customer_sk,
    p.product_sk                                as product_sk,
    ch.channel_sk                               as channel_sk,
    s.order_id,
    s.line_number                               as order_line_number,
    s.quantity,
    s.unit_price,
    s.quantity * s.unit_price                   as extended_amount,
    current_timestamp                           as etl_loaded_at
from staging.stg_order_lines s
join dim_date     d  on d.calendar_date = cast(s.order_ts as date)
join dim_customer c  on c.customer_id  = s.customer_id  and c.is_current = true
join dim_product  p  on p.product_id   = s.product_id   and p.is_current = true
join dim_channel  ch on ch.channel_code= s.channel_code and ch.is_current = true;
```

**Note**: Always join to the dimension's `is_current = true` row when loading facts from a live source. For historical backfills, join on `effective_from <= event_date <= coalesce(effective_to, '9999-12-31')`.

### Incremental Append Load

```sql
insert into fct_order_lines
    (order_line_sk, order_date_sk, customer_sk, product_sk, channel_sk,
     order_id, order_line_number, quantity, unit_price, extended_amount, etl_loaded_at)
select
    abs(farm_fingerprint(concat(s.order_id, '|', cast(s.line_number as varchar)))),
    d.date_sk, c.customer_sk, p.product_sk, ch.channel_sk,
    s.order_id, s.line_number, s.quantity, s.unit_price,
    s.quantity * s.unit_price,
    current_timestamp
from staging.stg_order_lines s
join dim_date     d  on d.calendar_date = cast(s.order_ts as date)
join dim_customer c  on c.customer_id   = s.customer_id and c.is_current = true
join dim_product  p  on p.product_id    = s.product_id  and p.is_current = true
join dim_channel  ch on ch.channel_code = s.channel_code and ch.is_current = true
where s.order_ts >= (select max(cast(d2.calendar_date as timestamp))
                     from fct_order_lines f2
                     join dim_date d2 on f2.order_date_sk = d2.date_sk)
  and not exists (
      select 1 from fct_order_lines f
      where f.order_id          = s.order_id
        and f.order_line_number = s.line_number
  );
```

### Late-Arriving Facts

Facts that arrive after the relevant dimension version has changed. Resolve by joining to the **historically correct** dimension row:

```sql
-- Find the dimension row that was current when the late-arriving event occurred
select
    s.*,
    c.customer_sk
from staging.stg_late_orders s
join dim_customer_scd2 c
    on c.customer_id   = s.customer_id
   and s.order_date between c.effective_from
                        and coalesce(c.effective_to, date '9999-12-31');
```

### Late-Arriving Dimensions

A dimension record arrives after the facts that reference it. Create a **placeholder** row (with `'Unknown'` attributes) to maintain referential integrity, then update when the real data arrives.

```sql
-- Insert unknown placeholder if natural key not found
insert into dim_customer_scd2
    (customer_sk, customer_id, full_name, email, country_code, segment,
     effective_from, effective_to, is_current, row_hash, etl_loaded_at)
select
    abs(farm_fingerprint(concat('UNKNOWN|', s.customer_id))),
    s.customer_id,
    'Unknown Customer', 'unknown@placeholder.com', 'XX', 'Unknown',
    date '1900-01-01', null, true,
    md5('UNKNOWN'), current_timestamp
from staging.stg_order_lines s
where not exists (
    select 1 from dim_customer_scd2 d where d.customer_id = s.customer_id
);
```

---

## Best Practices

### Design

- **Declare the grain first** — always document it as a one-sentence statement before writing any DDL. If you can't state the grain in one sentence, the model is wrong.
- **Atomic grain** — never aggregate in the fact table itself. Store the most granular available data; aggregations belong in mart views or separate aggregate fact tables.
- **One fact table = one business process** — do not mix grains (order lines and order headers in the same table). If you're tempted, create two fact tables.
- **All foreign keys must resolve** — every FK in the fact table must point to a valid dimension row. Use a special "Unknown" / "Not Applicable" dimension row (surrogate key = -1 or a reserved value) for nulls.
- **Conformed dimensions before building facts** — agree on `dim_date`, `dim_customer`, `dim_product` shared definitions across teams before building the first fact.

### Naming

- Tables: `fct_<business_process>`, `dim_<entity>`, `bridge_<entity1>_<entity2>`, `agg_<process>_<grain>`
- Surrogate keys: `<entity>_sk` (e.g. `customer_sk`)
- Natural keys: `<entity>_id` or `<entity>_code` (e.g. `customer_id`)
- Date FKs: `<role>_date_sk` (e.g. `order_date_sk`, `ship_date_sk`)
- SCD columns: `effective_from`, `effective_to`, `is_current`
- Audit columns: `etl_loaded_at`, `etl_updated_at`, `source_system`

### Surrogate Keys

- Integer (`bigint`) surrogate keys only — no business keys as PKs
- For distributed loads without sequences: deterministic hash (`farm_fingerprint` or `md5` truncated to bigint)
- Reserve surrogate key `-1` (or `0`) for the "Unknown" / "Not Applicable" member in every dimension
- The date dimension is the exception: use `YYYYMMDD` integer as the surrogate key for readability

### NULLs in Dimensions

Never leave dimension attributes as NULL in production. Use meaningful defaults:
- Unknown person: `'Unknown'`
- Unknown date: join to a reserved date_sk row with `calendar_date = '1900-01-01'`
- Unknown code: `'N/A'` or `'XX'`
- Nullable FK in fact (event hasn't happened yet): allowed, but document it

### SCD Strategy Selection

- Use **SCD1** by default — only upgrade to SCD2 when history genuinely changes the analytical answer
- Use **SCD2** when: customer segment analysis must reflect segment at time of purchase, not current segment
- Use **SCD6** when: you need both "what was true then" AND "what is true now" in the same query efficiently
- Use **SCD4** when: a handful of columns change very frequently and would cause explosive SCD2 row counts

### Performance

- **Partition fact tables by date** — almost all fact queries filter by date range
- **Sort/cluster by the most selective non-date dimension** — after partitioning by date, sort by `customer_sk` or `product_sk` to improve file skipping in Iceberg/Parquet
- **Aggregate fact tables** for high-frequency BI queries — create `agg_revenue_daily` as a pre-aggregated summary alongside the atomic `fct_order_lines`
- **Surrogate key lookups** — pre-resolve surrogate keys in the ETL/ELT layer; never do dimension lookups in BI queries

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Using natural/business key as dimension PK | Breaks SCD2 (can't have two rows with same PK); brittle to source changes | Always use a surrogate key PK |
| Mixing grains in one fact table | Aggregations produce wrong results (double-counting) | One fact table per business process and grain |
| NULL dimension FKs | Breaks GROUP BY grouping, BI tools can't handle | Use "Unknown" dimension member (SK = -1) |
| Joining fact to dimension without is_current filter (SCD2) | Returns multiple rows per fact row if not handled | The FK in the fact already points to correct version; no need to filter — or always filter `is_current=true` for current-state queries |
| Non-additive measures stored as measures | SUM gives wrong results (you can't SUM percentages) | Store numerator + denominator; compute ratio at query time |
| `SELECT *` from a wide fact table in BI | Reads all columns, slow on Parquet/Iceberg | Always enumerate needed columns |
| Dimension without date grain (no date dim) | Can't report by week/month/quarter flexibly | Always pre-build dim_date covering your history |
| Snowflaking for normalisation without query justification | Extra joins, slower queries, harder to understand | Keep dimensions denormalized (star schema) |
| Accumulating snapshot without periodic snapshot | Can't answer "what was the pipeline state on date X?" | Add a periodic snapshot for pipeline state if needed |
| SCD2 without row_hash column | Every change requires full attribute comparison in WHERE | Add a `row_hash` column; compare hashes first |

---

## Output Expectations

When designing or implementing a Kimball dimensional model:
- State the grain of every fact table in one sentence before showing any DDL.
- Specify the SCD type for each dimension and justify the choice.
- Show full DDL including surrogate key, natural key, SCD columns, and audit columns.
- For incremental loads, show both the "first run" (full) and "incremental run" path.
- Flag semi-additive and non-additive measures; show how to query them correctly.
- For SCD2, show the two-statement expire + insert pattern and the `is_current` query pattern.
- Recommend partitioning and sort order for Iceberg/Parquet storage when relevant.
