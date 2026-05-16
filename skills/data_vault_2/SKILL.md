---
name: data_vault_2
description: Use when designing or implementing Data Vault 2.0 data models — Hubs, Links, Satellites, Reference Tables, Same-As Links, Multi-Active Satellites, Effectivity Satellites, Point-in-Time tables, Bridge tables, Business Vault patterns, hash key generation, DDL for all entity types, insert-only DML load patterns, pipeline architecture, and constructing Information Marts from the vault.
---

# Data Vault 2.0 Data Modeling

## When to Use

Use this skill when:
- Designing a Raw Vault schema: Hubs, Links, Satellites for a new source system
- Implementing hash key generation and hash diff columns
- Writing insert-only DML load patterns for any vault entity
- Designing performance constructs: PIT tables and Bridge tables
- Implementing Business Vault: computed satellites, derived links, exploration links
- Constructing Information Mart views/tables from the vault
- Building DV 2.0 pipelines in Spark, dbt, Airflow, or pure SQL
- Choosing between Satellite variants: standard, multi-active, effectivity, status-tracking

---

## Architecture Overview

Data Vault 2.0 separates concerns into three layers:

```
SOURCE SYSTEMS
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│ STAGING LAYER                                           │
│  stg_<source>_<entity>  — hash keys + hash diffs added  │
│  load date, record source stamped                       │
└─────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│ RAW VAULT  (insert-only, no business rules)             │
│  Hubs     — business keys                               │
│  Links    — relationships between hubs                  │
│  Satellites — descriptive attributes + history          │
│  Reference tables, Same-As Links                        │
└─────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│ BUSINESS VAULT  (business rules applied)                │
│  Computed Satellites — derived attributes               │
│  Exploration Links  — inferred relationships            │
│  Same-As Links      — deduplication / survivorship      │
│  PIT tables + Bridge tables — query performance         │
└─────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│ INFORMATION MART  (Kimball / flat / wide tables)        │
│  Dimensional views built from vault entities            │
│  Fact tables via joins through PITs + Bridges           │
└─────────────────────────────────────────────────────────┘
```

**Core DV 2.0 rules that never change:**
1. Every table has `ldts` (load date timestamp) and `rsrc` (record source)
2. All raw vault loads are **insert-only** — never UPDATE or DELETE
3. All business keys are **hashed** before joining or storing
4. Business rules live in Business Vault, not Raw Vault
5. Every Hub, Link, Satellite is loaded **independently** — no cross-entity transactions

---

## Mandatory Columns on Every Table

| Column | Type | Description |
|---|---|---|
| `<entity>_hk` | `char(32)` / `binary(16)` | Hash key — MD5 of business key(s) |
| `ldts` | `timestamp` | Load date timestamp — when this row entered the vault |
| `rsrc` | `varchar(255)` | Record source — identifies the feeding system/file |
| `hash_diff` | `char(32)` | Satellite-only: MD5 hash of all descriptive attributes for change detection |

---

## Hash Key Generation

Hash keys are deterministic MD5 (or SHA-256) hashes of business keys. They must be computed **in staging** before loading any vault table.

### Rules for hash key computation

1. **Upper-case** all string business keys before hashing
2. **Trim** leading/trailing whitespace
3. **Use a consistent delimiter** between compound key components: `||` (double pipe) is standard
4. **Handle NULLs** by replacing with a sentinel value: `'^^'` (caret-caret) for null components
5. **Hash the concatenation**, not individual keys
6. **Use the same algorithm everywhere** in the project (MD5 is most common; SHA-256 for higher security)

```sql
-- Single-component business key
md5(upper(trim(customer_id)))

-- Compound business key (order_id + line_number)
md5(concat(
    upper(trim(order_id)),
    '||',
    coalesce(upper(trim(cast(line_number as varchar))), '^^')
))

-- Hub hash key for customer
md5(upper(trim(customer_bk)))
```

### Hash diff for satellites

Hash diff detects whether any descriptive attribute changed — avoids loading duplicate unchanged rows.

```sql
-- Hash diff: hash all satellite attributes concatenated
md5(concat_ws('||',
    coalesce(upper(trim(first_name)),  '^^'),
    coalesce(upper(trim(last_name)),   '^^'),
    coalesce(upper(trim(email)),       '^^'),
    coalesce(upper(trim(country_code)),'^^'),
    coalesce(cast(date_of_birth as varchar), '^^')
))
```

**Order matters**: always concatenate attributes in alphabetical column order so the hash is consistent regardless of SELECT column order changes.

---

## Staging Layer

Staging transforms source data: computes all hash keys and hash diffs before any vault load.

```sql
-- stg_crm_customers
create or replace view staging.stg_crm_customers as
select
    -- Hash keys
    md5(upper(trim(customer_id)))                       as customer_hk,

    -- Business key (preserved as-is for Hub)
    customer_id                                         as customer_bk,

    -- Satellite attributes
    first_name,
    last_name,
    email,
    country_code,
    date_of_birth,
    customer_segment,

    -- Hash diff (alphabetical column order)
    md5(concat_ws('||',
        coalesce(upper(trim(cast(date_of_birth as varchar))), '^^'),
        coalesce(upper(trim(customer_segment)),               '^^'),
        coalesce(upper(trim(country_code)),                   '^^'),
        coalesce(upper(trim(email)),                          '^^'),
        coalesce(upper(trim(first_name)),                     '^^'),
        coalesce(upper(trim(last_name)),                      '^^')
    ))                                                  as customer_sat_hashdiff,

    -- Mandatory metadata
    current_timestamp                                   as ldts,
    'CRM_SYSTEM_V1'                                     as rsrc
from raw.crm_customers_landed;
```

If a single source load contains **multiple entities** (e.g. orders with embedded customer and product), create one staging view per source object — each staging view extracts the relevant fields and computes the appropriate hash keys.

---

## Hubs

A Hub stores the **master list of business keys** for a business concept. Nothing else.

### Rules
- One row per unique business key — never two rows for the same key
- No descriptive attributes (those go in Satellites)
- Insert-only: once a business key is in the Hub, it stays forever
- Multiple source systems feeding the same Hub use the same hash key (MD5 of the normalized business key)

### DDL

```sql
create table raw_vault.hub_customer (
    customer_hk     char(32)        not null,   -- MD5 hash of customer_bk
    customer_bk     varchar(50)     not null,   -- business / natural key
    ldts            timestamp       not null,   -- first time seen
    rsrc            varchar(255)    not null,   -- which source first loaded it
    primary key (customer_hk)
);

create table raw_vault.hub_product (
    product_hk      char(32)        not null,
    product_bk      varchar(50)     not null,
    ldts            timestamp       not null,
    rsrc            varchar(255)    not null,
    primary key (product_hk)
);

create table raw_vault.hub_order (
    order_hk        char(32)        not null,
    order_bk        varchar(50)     not null,
    ldts            timestamp       not null,
    rsrc            varchar(255)    not null,
    primary key (order_hk)
);
```

### DML — Insert-only Hub load

```sql
insert into raw_vault.hub_customer (customer_hk, customer_bk, ldts, rsrc)
select
    s.customer_hk,
    s.customer_bk,
    s.ldts,
    s.rsrc
from staging.stg_crm_customers s
where not exists (
    select 1 from raw_vault.hub_customer h
    where h.customer_hk = s.customer_hk
);
```

**Multi-source Hub load**: when two systems both supply customers, load both into the same hub. The first system to load a key "wins" the `ldts` and `rsrc`. Subsequent loads of the same key are silently skipped.

```sql
-- Merge both sources in one pass
insert into raw_vault.hub_customer (customer_hk, customer_bk, ldts, rsrc)
select customer_hk, customer_bk, ldts, rsrc
from (
    select customer_hk, customer_bk, ldts, rsrc, 1 as priority
    from staging.stg_crm_customers
    union all
    select customer_hk, customer_bk, ldts, rsrc, 2 as priority
    from staging.stg_ecommerce_customers
) combined
where not exists (
    select 1 from raw_vault.hub_customer h
    where h.customer_hk = combined.customer_hk
)
qualify row_number() over (partition by customer_hk order by priority, ldts) = 1;
```

---

## Links

A Link records a **relationship** between two or more Hub entities. It captures the many-to-many association at a point in time.

### Rules
- Contains only hash keys (its own + constituent Hub HKs) — no descriptive attributes
- Insert-only: relationships are never deleted from the Link (use Effectivity Satellite to close them)
- One row per unique combination of constituent Hub HKs
- A Link's own hash key is the MD5 of all constituent Hub HKs concatenated

### DDL

```sql
-- Link: customer ↔ order (customer placed an order)
create table raw_vault.lnk_customer_order (
    customer_order_hk   char(32)    not null,   -- MD5(customer_hk || order_hk)
    customer_hk         char(32)    not null,   -- FK to hub_customer
    order_hk            char(32)    not null,   -- FK to hub_order
    ldts                timestamp   not null,
    rsrc                varchar(255) not null,
    primary key (customer_order_hk)
);

-- Link: order_line — connects order, product, warehouse (3-way)
create table raw_vault.lnk_order_line (
    order_line_hk       char(32)    not null,   -- MD5(order_hk || product_hk || warehouse_hk)
    order_hk            char(32)    not null,
    product_hk          char(32)    not null,
    warehouse_hk        char(32)    not null,
    ldts                timestamp   not null,
    rsrc                varchar(255) not null,
    primary key (order_line_hk)
);
```

### Link hash key generation (in staging)

```sql
-- staging.stg_crm_orders — includes both order and customer context
md5(concat(
    customer_hk, '||', order_hk
)) as customer_order_hk

-- 3-way link
md5(concat(
    order_hk, '||', product_hk, '||', warehouse_hk
)) as order_line_hk
```

### DML — Insert-only Link load

```sql
insert into raw_vault.lnk_customer_order (customer_order_hk, customer_hk, order_hk, ldts, rsrc)
select
    s.customer_order_hk,
    s.customer_hk,
    s.order_hk,
    s.ldts,
    s.rsrc
from staging.stg_crm_orders s
where not exists (
    select 1 from raw_vault.lnk_customer_order l
    where l.customer_order_hk = s.customer_order_hk
);
```

### Non-Historized Link (Transactional Link)

For high-volume transactional data (payments, clicks, log events) where deduplication is by a natural transaction ID and there is no meaningful "relationship history" — use a Non-Historized Link (NHL). Each transaction is its own row; the natural transaction key is part of the hash.

```sql
create table raw_vault.nlnk_payment_transaction (
    payment_hk      char(32)    not null,   -- MD5(payment_transaction_id)
    customer_hk     char(32)    not null,
    account_hk      char(32)    not null,
    payment_bk      varchar(50) not null,   -- source transaction id (degenerate)
    amount          decimal(18,2) not null, -- measure stored directly (DV 2.0 allows this for transactional links)
    ldts            timestamp   not null,
    rsrc            varchar(255) not null,
    primary key (payment_hk)
);
```

---

## Satellites

A Satellite stores **descriptive attributes** and their **full history**. Every change in a source attribute creates a new row; old rows are never updated.

### Rules
- One parent: either a Hub or a Link (never both)
- Insert-only: each new version is a new row with a new `ldts`
- The combination `(parent_hk, ldts)` must be unique — the primary key
- Load only when `hash_diff` changes from the currently latest row
- Split large satellites by rate of change or source system (see below)

### DDL — Hub Satellite

```sql
-- Satellite on hub_customer — demographic attributes
create table raw_vault.sat_customer_details (
    customer_hk     char(32)        not null,   -- FK to hub_customer
    ldts            timestamp       not null,   -- version timestamp
    ldts_end        timestamp,                  -- when this version was superseded (optional, ghost record pattern)
    hash_diff       char(32)        not null,   -- MD5 of all attributes
    rsrc            varchar(255)    not null,
    -- Descriptive attributes
    first_name      varchar(200),
    last_name       varchar(200),
    email           varchar(200),
    country_code    char(2),
    date_of_birth   date,
    customer_segment varchar(50),
    primary key (customer_hk, ldts)
);

-- Satellite on hub_customer — contact preferences (separate satellite, different change rate)
create table raw_vault.sat_customer_contact_prefs (
    customer_hk             char(32)        not null,
    ldts                    timestamp       not null,
    hash_diff               char(32)        not null,
    rsrc                    varchar(255)    not null,
    email_opt_in            boolean,
    sms_opt_in              boolean,
    preferred_language      char(2),
    contact_time_preference varchar(20),
    primary key (customer_hk, ldts)
);
```

### DDL — Link Satellite

```sql
-- Satellite on lnk_customer_order — order attributes
create table raw_vault.sat_customer_order (
    customer_order_hk   char(32)        not null,   -- FK to lnk_customer_order
    ldts                timestamp       not null,
    hash_diff           char(32)        not null,
    rsrc                varchar(255)    not null,
    order_status        varchar(20),
    order_total         decimal(18,2),
    currency_code       char(3),
    payment_method      varchar(30),
    primary key (customer_order_hk, ldts)
);
```

### DML — Insert-only Satellite load

```sql
insert into raw_vault.sat_customer_details
    (customer_hk, ldts, hash_diff, rsrc,
     first_name, last_name, email, country_code, date_of_birth, customer_segment)
select
    s.customer_hk,
    s.ldts,
    s.customer_sat_hashdiff,
    s.rsrc,
    s.first_name,
    s.last_name,
    s.email,
    s.country_code,
    s.date_of_birth,
    s.customer_segment
from staging.stg_crm_customers s
where not exists (
    -- Skip if the latest version for this key has the same hash_diff
    select 1
    from raw_vault.sat_customer_details latest
    where latest.customer_hk  = s.customer_hk
      and latest.hash_diff    = s.customer_sat_hashdiff
      and latest.ldts = (
          select max(inner_sat.ldts)
          from raw_vault.sat_customer_details inner_sat
          where inner_sat.customer_hk = s.customer_hk
      )
);
```

**Simplified version using window functions** (works in Trino/Spark SQL):

```sql
insert into raw_vault.sat_customer_details
    (customer_hk, ldts, hash_diff, rsrc,
     first_name, last_name, email, country_code, date_of_birth, customer_segment)
with latest as (
    select customer_hk, hash_diff,
           row_number() over (partition by customer_hk order by ldts desc) as rn
    from raw_vault.sat_customer_details
)
select
    s.customer_hk, s.ldts, s.customer_sat_hashdiff, s.rsrc,
    s.first_name, s.last_name, s.email, s.country_code, s.date_of_birth, s.customer_segment
from staging.stg_crm_customers s
left join latest l
    on l.customer_hk = s.customer_hk and l.rn = 1
where l.customer_hk is null                      -- new entity: no satellite row yet
   or l.hash_diff <> s.customer_sat_hashdiff;    -- changed entity: hash diff differs
```

### Satellite splitting strategy

Split one physical satellite into multiple when:
- Attributes come from **different source systems** (split by `rsrc`)
- Attributes have **different rates of change** (demographics change rarely; session data changes every load)
- A satellite has **> 20 columns** (wide satellites are harder to maintain and query)

```
hub_customer
   ├── sat_customer_details      (CRM — demographics, slow-changing)
   ├── sat_customer_contact_prefs (CRM — opt-ins, medium-changing)
   ├── sat_customer_risk_profile  (Risk system — credit scores, fast-changing)
   └── sat_customer_web_profile   (Web platform — last login, very fast-changing)
```

---

## Satellite Variants

### Multi-Active Satellite (MAS)

Multiple active rows per parent at the same load date. Used when a source entity has a repeating group (e.g. a customer with multiple phone numbers, a product with multiple barcodes).

```sql
create table raw_vault.mas_customer_phone (
    customer_hk         char(32)        not null,
    ldts                timestamp       not null,
    phone_type          varchar(20)     not null,   -- discriminator: 'MOBILE','HOME','WORK'
    hash_diff           char(32)        not null,
    rsrc                varchar(255)    not null,
    phone_number        varchar(30),
    is_primary          boolean,
    primary key (customer_hk, ldts, phone_type)   -- phone_type is part of PK
);
```

**DML for MAS** — must identify the "set" of rows for the current load and compare against the full prior active set:

```sql
insert into raw_vault.mas_customer_phone
    (customer_hk, ldts, phone_type, hash_diff, rsrc, phone_number, is_primary)
-- New records not yet in the satellite
select s.customer_hk, s.ldts, s.phone_type, s.hash_diff, s.rsrc, s.phone_number, s.is_primary
from staging.stg_customer_phones s
where not exists (
    select 1 from raw_vault.mas_customer_phone m
    where m.customer_hk = s.customer_hk
      and m.phone_type  = s.phone_type
      and m.hash_diff   = s.hash_diff
      and m.ldts = (select max(m2.ldts) from raw_vault.mas_customer_phone m2
                    where m2.customer_hk = s.customer_hk and m2.phone_type = s.phone_type)
);
```

### Effectivity Satellite (EFF SAT)

Tracks **when a Link relationship is open or closed**. Since Links are insert-only, you cannot delete the Link row when a relationship ends — instead, the Effectivity Satellite records the open/close timestamps.

```sql
create table raw_vault.sat_customer_order_eff (
    customer_order_hk   char(32)    not null,   -- FK to lnk_customer_order
    ldts                timestamp   not null,
    ldts_end            timestamp,              -- null = currently active
    hash_diff           char(32)    not null,
    rsrc                varchar(255) not null,
    is_active           boolean     not null,
    primary key (customer_order_hk, ldts)
);
```

**Loading effectivity** — when relationship closes, insert a closing row:

```sql
-- Open the relationship
insert into raw_vault.sat_customer_order_eff
    (customer_order_hk, ldts, ldts_end, hash_diff, rsrc, is_active)
select
    s.customer_order_hk, s.ldts, null,
    md5('ACTIVE'), s.rsrc, true
from staging.stg_crm_orders s
where not exists (
    select 1 from raw_vault.sat_customer_order_eff e
    where e.customer_order_hk = s.customer_order_hk and e.is_active = true
);

-- Close the relationship (order cancelled or returned — new row with is_active=false)
insert into raw_vault.sat_customer_order_eff
    (customer_order_hk, ldts, ldts_end, hash_diff, rsrc, is_active)
select
    s.customer_order_hk, s.ldts, null,
    md5('INACTIVE'), s.rsrc, false
from staging.stg_crm_cancelled_orders s
where exists (
    select 1 from raw_vault.sat_customer_order_eff e
    where e.customer_order_hk = s.customer_order_hk and e.is_active = true
);
```

---

## Reference Tables

For small, relatively stable lookup data (country codes, currency codes, status mappings). Do not model as Hub + Satellite — the overhead is unnecessary.

```sql
create table raw_vault.ref_country (
    country_code    char(2)         not null,
    ldts            timestamp       not null,
    rsrc            varchar(255)    not null,
    country_name    varchar(100)    not null,
    region          varchar(50),
    currency_code   char(3),
    primary key (country_code)
);

-- Load: insert new + update changed in-place (REF tables are the exception to insert-only)
merge into raw_vault.ref_country as target
using staging.stg_ref_country as source on target.country_code = source.country_code
when matched and target.country_name <> source.country_name then
    update set country_name = source.country_name, ldts = source.ldts
when not matched then
    insert (country_code, ldts, rsrc, country_name, region, currency_code)
    values (source.country_code, source.ldts, source.rsrc, source.country_name,
            source.region, source.currency_code);
```

---

## Same-As Link (SAL)

Resolves duplicate business keys across sources. Records that two business keys refer to the same real-world entity.

```sql
create table raw_vault.sal_customer (
    sal_customer_hk     char(32)    not null,   -- MD5(customer_hk_a || customer_hk_b)
    customer_hk_a       char(32)    not null,   -- FK to hub_customer (canonical)
    customer_hk_b       char(32)    not null,   -- FK to hub_customer (duplicate)
    ldts                timestamp   not null,
    rsrc                varchar(255) not null,
    primary key (sal_customer_hk)
);
```

The SAL is populated by a Business Vault process (matching algorithm) and is used to resolve `customer_hk_b` to `customer_hk_a` when building Information Marts.

---

## Performance Constructs

### Point-in-Time (PIT) Table

A PIT table is a **query-performance helper** that pre-computes "what is the latest satellite row for each entity as of each snapshot date?". It eliminates expensive `max(ldts)` lookups when joining multiple satellites.

PIT tables are built once in the Business Vault layer after all Raw Vault loads complete.

```sql
create table business_vault.pit_customer (
    snapshot_date           date            not null,
    customer_hk             char(32)        not null,
    -- One column pair per satellite: (hk, ldts) pointing to the correct version
    sat_customer_details_hk     char(32),
    sat_customer_details_ldts   timestamp,
    sat_customer_contact_hk     char(32),
    sat_customer_contact_ldts   timestamp,
    sat_customer_risk_hk        char(32),
    sat_customer_risk_ldts      timestamp,
    primary key (snapshot_date, customer_hk)
);
```

**Building the PIT** — one row per customer per snapshot date, pointing to the satellite row that was current as of that date:

```sql
insert overwrite business_vault.pit_customer
with
snapshots as (
    -- Generate daily snapshots for the last 90 days
    select date_add(current_date, -seq) as snapshot_date
    from (select sequence(0, 89) as s) cross join unnest(s) as t(seq)
),
all_customers as (
    select distinct customer_hk from raw_vault.hub_customer
),
spine as (
    select a.customer_hk, s.snapshot_date
    from all_customers a cross join snapshots s
)
select
    sp.snapshot_date,
    sp.customer_hk,
    -- sat_customer_details: latest version on or before snapshot_date
    det.customer_hk                     as sat_customer_details_hk,
    det.ldts                            as sat_customer_details_ldts,
    -- sat_customer_contact_prefs
    con.customer_hk                     as sat_customer_contact_hk,
    con.ldts                            as sat_customer_contact_ldts,
    -- sat_customer_risk_profile
    risk.customer_hk                    as sat_customer_risk_hk,
    risk.ldts                           as sat_customer_risk_ldts
from spine sp
left join lateral (
    select customer_hk, ldts from raw_vault.sat_customer_details
    where customer_hk = sp.customer_hk and ldts <= sp.snapshot_date + interval '1' day
    order by ldts desc limit 1
) det on true
left join lateral (
    select customer_hk, ldts from raw_vault.sat_customer_contact_prefs
    where customer_hk = sp.customer_hk and ldts <= sp.snapshot_date + interval '1' day
    order by ldts desc limit 1
) con on true
left join lateral (
    select customer_hk, ldts from raw_vault.sat_customer_risk_profile
    where customer_hk = sp.customer_hk and ldts <= sp.snapshot_date + interval '1' day
    order by ldts desc limit 1
) risk on true;
```

**Querying through the PIT** — fast satellite joins without any max(ldts):

```sql
select
    pit.snapshot_date,
    pit.customer_hk,
    det.first_name,
    det.last_name,
    det.country_code,
    risk.credit_score_band
from business_vault.pit_customer pit
left join raw_vault.sat_customer_details det
    on det.customer_hk = pit.sat_customer_details_hk
   and det.ldts        = pit.sat_customer_details_ldts
left join raw_vault.sat_customer_risk_profile risk
    on risk.customer_hk = pit.sat_customer_risk_hk
   and risk.ldts        = pit.sat_customer_risk_ldts
where pit.snapshot_date = current_date;
```

### Bridge Table

A Bridge pre-joins multiple Links into a flattened structure for fact table construction. Eliminates complex multi-hop join chains at query time.

```sql
create table business_vault.brdg_order_snapshot (
    snapshot_date       date        not null,
    order_hk            char(32)    not null,   -- hub_order
    customer_hk         char(32),               -- via lnk_customer_order
    product_hk          char(32),               -- via lnk_order_line
    warehouse_hk        char(32),               -- via lnk_order_line
    -- Link hash keys for direct joins
    customer_order_hk   char(32),
    order_line_hk       char(32),
    primary key (snapshot_date, order_hk, product_hk)
);
```

---

## Business Vault

Business Vault sits between Raw Vault and Information Mart. It applies business rules while keeping them separate from raw data.

### Computed Satellite

Derives new attributes from raw satellite data with business logic applied.

```sql
create table business_vault.sat_customer_computed (
    customer_hk         char(32)    not null,
    ldts                timestamp   not null,
    hash_diff           char(32)    not null,
    rsrc                varchar(255) not null  default 'BUSINESS_VAULT',
    -- Derived attributes
    full_name           varchar(400),           -- first_name || ' ' || last_name
    age_years           int,                    -- computed from date_of_birth
    age_band            varchar(20),            -- '18-24', '25-34', ...
    is_eu_resident      boolean,                -- derived from country_code
    primary key (customer_hk, ldts)
);

-- Load computed satellite
insert into business_vault.sat_customer_computed
    (customer_hk, ldts, hash_diff, rsrc, full_name, age_years, age_band, is_eu_resident)
with latest_raw as (
    select customer_hk, ldts, first_name, last_name, country_code, date_of_birth,
           row_number() over (partition by customer_hk order by ldts desc) as rn
    from raw_vault.sat_customer_details
),
computed as (
    select
        customer_hk,
        ldts,
        trim(first_name || ' ' || last_name)    as full_name,
        date_diff('year', date_of_birth, current_date) as age_years,
        case
            when date_diff('year', date_of_birth, current_date) < 25 then '18-24'
            when date_diff('year', date_of_birth, current_date) < 35 then '25-34'
            when date_diff('year', date_of_birth, current_date) < 45 then '35-44'
            when date_diff('year', date_of_birth, current_date) < 55 then '45-54'
            else '55+'
        end                                     as age_band,
        country_code in ('DE','FR','IT','ES','PL','NL','BE','SE','AT','DK',
                         'FI','PT','IE','GR','HU','CZ','RO','BG','HR','LT',
                         'LV','EE','SI','SK','LU','MT','CY')  as is_eu_resident
    from latest_raw where rn = 1
)
select
    c.customer_hk, c.ldts,
    md5(concat_ws('||',
        coalesce(c.full_name, '^^'),
        coalesce(cast(c.age_band as varchar), '^^'),
        coalesce(cast(c.is_eu_resident as varchar), '^^')
    )) as hash_diff,
    'BUSINESS_VAULT' as rsrc,
    c.full_name, c.age_years, c.age_band, c.is_eu_resident
from computed c
where not exists (
    select 1 from business_vault.sat_customer_computed prev
    where prev.customer_hk = c.customer_hk
      and prev.ldts = (select max(ldts) from business_vault.sat_customer_computed
                       where customer_hk = c.customer_hk)
      and prev.hash_diff = md5(concat_ws('||',
            coalesce(c.full_name, '^^'),
            coalesce(cast(c.age_band as varchar), '^^'),
            coalesce(cast(c.is_eu_resident as varchar), '^^')))
);
```

---

## Information Mart

The Information Mart translates vault structures into analyst-friendly views or tables (Kimball star schema, wide flat tables, or API-friendly denormalized views).

### Dimension from Hub + Satellites (current state)

```sql
create or replace view information_mart.dim_customer as
select
    h.customer_hk,
    h.customer_bk                           as customer_id,
    det.first_name,
    det.last_name,
    bv.full_name,
    det.email,
    det.country_code,
    r.country_name,
    det.customer_segment,
    bv.age_band,
    bv.is_eu_resident,
    con.email_opt_in,
    con.preferred_language,
    risk.credit_score_band
from raw_vault.hub_customer h
-- Latest version of each satellite using PIT
left join business_vault.pit_customer pit
    on pit.customer_hk   = h.customer_hk
   and pit.snapshot_date = current_date
left join raw_vault.sat_customer_details det
    on det.customer_hk = pit.sat_customer_details_hk
   and det.ldts        = pit.sat_customer_details_ldts
left join raw_vault.sat_customer_contact_prefs con
    on con.customer_hk = pit.sat_customer_contact_hk
   and con.ldts        = pit.sat_customer_contact_ldts
left join raw_vault.sat_customer_risk_profile risk
    on risk.customer_hk = pit.sat_customer_risk_hk
   and risk.ldts        = pit.sat_customer_risk_ldts
left join business_vault.sat_customer_computed bv
    on bv.customer_hk = h.customer_hk
   and bv.ldts = (select max(ldts) from business_vault.sat_customer_computed
                  where customer_hk = h.customer_hk)
left join raw_vault.ref_country r on det.country_code = r.country_code;
```

### Fact Table from Links + Satellites

```sql
create or replace view information_mart.fct_orders as
select
    -- Surrogate keys for Kimball compatibility
    lco.customer_order_hk                   as order_key,
    lco.customer_hk                         as customer_key,
    -- Date key
    cast(date_format(cast(s_ord.ldts as date), '%Y%m%d') as int) as order_date_sk,
    -- Degenerate dimensions
    h_ord.order_bk                          as order_id,
    -- Measures
    s_ord.order_total,
    s_ord.currency_code,
    s_ord.order_status
from raw_vault.lnk_customer_order lco
join raw_vault.hub_order h_ord
    on h_ord.order_hk = lco.order_hk
-- Latest order satellite
join raw_vault.sat_customer_order s_ord
    on s_ord.customer_order_hk = lco.customer_order_hk
   and s_ord.ldts = (select max(ldts) from raw_vault.sat_customer_order
                     where customer_order_hk = lco.customer_order_hk)
-- Only active relationships
join raw_vault.sat_customer_order_eff eff
    on eff.customer_order_hk = lco.customer_order_hk
   and eff.ldts = (select max(ldts) from raw_vault.sat_customer_order_eff
                   where customer_order_hk = lco.customer_order_hk)
   and eff.is_active = true;
```

---

## Pipeline Architecture

### Batch Pipeline (Airflow DAG)

Each batch load follows the same sequence. The order within each group is parallel.

```
Load Group 0: Staging views (SQL or Spark)
    │
    ▼
Load Group 1: Hubs (all in parallel — independent of each other)
    hub_customer, hub_product, hub_order, hub_warehouse
    │
    ▼
Load Group 2: Links (all in parallel — depend only on Hubs existing)
    lnk_customer_order, lnk_order_line
    │
    ▼
Load Group 3: Satellites (all in parallel — depend only on parent Hub/Link)
    sat_customer_details, sat_customer_contact_prefs,
    sat_customer_order, sat_order_line_details, ...
    │
    ▼
Load Group 4: Effectivity Satellites
    sat_customer_order_eff, ...
    │
    ▼
Load Group 5: Business Vault (depends on Raw Vault being complete)
    sat_customer_computed, ...
    │
    ▼
Load Group 6: PIT & Bridge (depends on Business Vault)
    pit_customer, brdg_order_snapshot
```

### Airflow DAG pattern

```python
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime

with DAG('dv2_load', schedule_interval='0 4 * * *', start_date=datetime(2024,1,1)) as dag:

    # Group 1: Hubs — run in parallel
    hub_customer = TrinoOperator(task_id='hub_customer',
        sql='sql/raw_vault/hub_customer.sql', trino_conn_id='trino_dwh')
    hub_product  = TrinoOperator(task_id='hub_product',
        sql='sql/raw_vault/hub_product.sql',  trino_conn_id='trino_dwh')
    hub_order    = TrinoOperator(task_id='hub_order',
        sql='sql/raw_vault/hub_order.sql',    trino_conn_id='trino_dwh')

    # Group 2: Links — after all hubs
    lnk_customer_order = TrinoOperator(task_id='lnk_customer_order',
        sql='sql/raw_vault/lnk_customer_order.sql', trino_conn_id='trino_dwh')
    lnk_order_line = TrinoOperator(task_id='lnk_order_line',
        sql='sql/raw_vault/lnk_order_line.sql',     trino_conn_id='trino_dwh')

    # Group 3: Satellites — after their parent hub/link
    sat_customer_details = TrinoOperator(task_id='sat_customer_details',
        sql='sql/raw_vault/sat_customer_details.sql', trino_conn_id='trino_dwh')
    sat_customer_order = TrinoOperator(task_id='sat_customer_order',
        sql='sql/raw_vault/sat_customer_order.sql',   trino_conn_id='trino_dwh')

    # Group 5: Business Vault
    sat_customer_computed = TrinoOperator(task_id='sat_customer_computed',
        sql='sql/business_vault/sat_customer_computed.sql', trino_conn_id='trino_dwh')

    # Group 6: PIT
    pit_customer = TrinoOperator(task_id='pit_customer',
        sql='sql/business_vault/pit_customer.sql', trino_conn_id='trino_dwh')

    # Dependencies
    [hub_customer, hub_product, hub_order] >> [lnk_customer_order, lnk_order_line]
    hub_customer >> sat_customer_details
    lnk_customer_order >> sat_customer_order
    [sat_customer_details] >> sat_customer_computed >> pit_customer
```

### dbt + automate-dv

[automate-dv](https://automate-dv.readthedocs.io/) (formerly dbtvault) provides Jinja macros for all DV 2.0 entity types.

```yaml
# dbt_project.yml
vars:
  hash: MD5
  concat_string: '||'
  null_placeholder_string: '^^'
```

**Hub model** (`models/raw_vault/hub_customer.sql`):
```sql
{{ automate_dv.hub(
    src_pk    = 'CUSTOMER_HK',
    src_nk    = 'CUSTOMER_BK',
    src_ldts  = 'LDTS',
    src_source= 'RSRC',
    source_model = ['stg_crm_customers', 'stg_ecommerce_customers']
) }}
```

**Link model** (`models/raw_vault/lnk_customer_order.sql`):
```sql
{{ automate_dv.link(
    src_pk     = 'CUSTOMER_ORDER_HK',
    src_fk     = ['CUSTOMER_HK', 'ORDER_HK'],
    src_ldts   = 'LDTS',
    src_source = 'RSRC',
    source_model = 'stg_crm_orders'
) }}
```

**Satellite model** (`models/raw_vault/sat_customer_details.sql`):
```sql
{{ automate_dv.sat(
    src_pk     = 'CUSTOMER_HK',
    src_hashdiff = 'CUSTOMER_SAT_HASHDIFF',
    src_payload  = ['FIRST_NAME', 'LAST_NAME', 'EMAIL', 'COUNTRY_CODE', 'DATE_OF_BIRTH', 'CUSTOMER_SEGMENT'],
    src_ldts   = 'LDTS',
    src_source = 'RSRC',
    source_model = 'stg_crm_customers'
) }}
```

**Staging model** (`models/staging/stg_crm_customers.sql`):
```sql
{{ automate_dv.stage(
    include_source_columns = true,
    source_model           = source('crm', 'customers'),
    hashed_columns = {
        'CUSTOMER_HK':           'CUSTOMER_ID',
        'CUSTOMER_SAT_HASHDIFF': {
            'is_hashdiff': true,
            'columns': ['DATE_OF_BIRTH', 'CUSTOMER_SEGMENT', 'COUNTRY_CODE', 'EMAIL', 'FIRST_NAME', 'LAST_NAME']
        }
    },
    derived_columns = {
        'CUSTOMER_BK': 'CUSTOMER_ID',
        'RSRC':        "!CRM_SYSTEM_V1",
        'LDTS':        'CURRENT_TIMESTAMP'
    }
) }}
```

---

## Best Practices

### Hash Keys
- **One hash algorithm for the entire project** — do not mix MD5 and SHA-256
- **Upper + trim before hashing** — inconsistencies here cause phantom duplicates across sources
- **Document the hash key formula** in a central hash key catalog (a simple table or YAML file)
- **Binary(16) storage** is more efficient than char(32) for hash keys in high-volume tables
- **Never trust hash collisions are impossible** — add a natural key uniqueness check in data quality

### Structural
- **One staging view per source object** — do not mix entities in one staging transform
- **Never put descriptive data in a Hub or Link** — move it to a Satellite immediately
- **Split satellites by rate of change** — fast-changing and slow-changing attributes in separate satellites reduce unnecessary row growth
- **Name satellites by business domain** (`sat_customer_demographics`, `sat_customer_web_behaviour`), not by technical source column count
- **Every satellite must have a hash_diff** — without it you cannot detect changes efficiently

### Loading
- **Load Hubs first, Links second, Satellites third** — in every pipeline run, always in this sequence
- **All loads within the same group run in parallel** — there is no dependency between two Hub loads, two Link loads, or two Satellite loads
- **Use batch timestamps, not `current_timestamp`** — assign a single `ldts` value to all rows in one pipeline execution for consistency
- **Never backfill with `current_timestamp`** — use the source system's event timestamp when loading historical data
- **Idempotent loads** — every load SQL should be safe to re-run without creating duplicates (insert WHERE NOT EXISTS, or hash-diff comparison)

### Business Vault
- **No business rules in Raw Vault** — if you're filtering rows based on business logic, move it to Business Vault or Information Mart
- **Document every Business Vault transformation** — what rule was applied, by whom, when
- **PIT tables are mandatory for production** — querying satellites without PIT using `max(ldts)` correlated subqueries is too slow on large tables
- **Rebuild PIT tables after every Raw Vault load** — they are idempotent and can be truncate-reloaded daily

### Information Mart
- **Information Mart consumers should never query Raw Vault directly** — always go through Business Vault or PIT/Bridge layer
- **Wrap dimensional views as `create or replace view`** — they are always current; materialized tables need refresh scheduling
- **Add surrogate keys to dim/fact views** — use `customer_hk` as the surrogate key; it is already a stable integer-equivalent
- **Date dimension is still required** — build a `dim_date` and use `date_sk` joins just as in Kimball

---

## Raw Vault vs Business Vault — Decision Guide

| Question | Raw Vault | Business Vault |
|---|---|---|
| Does this attribute come directly from the source? | ✅ | ❌ |
| Is this a derived / computed value? | ❌ | ✅ |
| Is this a business rule or filter? | ❌ | ✅ |
| Can the source system change this value tomorrow? | ✅ | |
| Is this a standardization / normalization? | ❌ | ✅ |
| Would two analysts agree on this rule? | | ✅ |
| Could the rule change next year? | | — document the version |

---

## Anti-Patterns

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Updating rows in Hubs or Satellites | Destroys auditability and history | Insert-only always — use new rows for changes |
| Applying business rules in Raw Vault | Contaminates the audit layer; rules change but raw data must not | Move all business logic to Business Vault |
| Inconsistent hash key computation (mixed case, different delimiters) | Same business key hashes differently from different sources → phantom duplicate Hubs | Enforce upper+trim+delimiter in staging; document and test |
| One giant satellite with all attributes | Hard to maintain; every minor change creates a full-row version | Split by rate of change and source system |
| No hash_diff column | Every load inserts every row regardless of change → table bloat | Always include hash_diff; skip insert when hash matches latest |
| Querying satellites with correlated max(ldts) subqueries in production | O(n) query plan per row → extremely slow on large tables | Build and use PIT tables |
| No effectivity satellite on Links | Cannot determine if a relationship is still active | Add EFF SAT to every Link that can be closed |
| Business keys as Hub PKs | Breaks when business keys change or overlap across sources | Always use a computed hash key as the PK |
| Single staging view for all entities | Confusing, hard to test, makes it impossible to reload one entity | One staging view per source entity |
| Rebuilding PIT tables on every query | PIT construction is expensive — done on the fly is slow | Pre-build and persist PIT tables on a schedule |
| Skipping the vault, going directly to mart | Loses auditability and re-processability | Always build Raw Vault first; mart is derived from it |

---

## Output Expectations

When designing or implementing Data Vault 2.0:
- Identify each entity as Hub / Link / Satellite / Reference and justify the choice.
- Show the full DDL including `ldts`, `rsrc`, `hash_diff` on every table.
- Show the hash key formula for every Hub and Link (which columns, delimiter, normalisation).
- Show the insert-only DML with the `WHERE NOT EXISTS` or hash_diff comparison guard.
- State which load group each entity belongs to (0=staging, 1=hubs, 2=links, 3=sats, 4=eff sats, 5=bv, 6=pit).
- For Information Mart queries, show the PIT-based join pattern, not correlated subqueries.
- Flag any business rule and place it in Business Vault, not Raw Vault.
