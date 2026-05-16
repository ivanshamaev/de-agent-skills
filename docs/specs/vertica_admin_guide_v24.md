# Vertica Administrator Guide — v24.3

> Source: [Vertica 24.3.x Administrator's Guide](https://docs.vertica.com/24.3.x/en/admin/)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Operating the Database](#2-operating-the-database)
3. [Configuring the Database](#3-configuring-the-database)
4. [Database Users and Privileges](#4-database-users-and-privileges)
5. [Projections](#5-projections)
6. [Partitioning Tables](#6-partitioning-tables)
7. [Constraints](#7-constraints)
8. [Working with Native Tables](#8-working-with-native-tables)
9. [Transactions and Locking](#9-transactions-and-locking)
10. [Managing Queries](#10-managing-queries)
11. [Resource Management](#11-resource-management)
12. [Managing Storage Locations](#12-managing-storage-locations)
13. [Managing the Database](#13-managing-the-database)
14. [Collecting Database Statistics](#14-collecting-database-statistics)
15. [Analyzing Workloads](#15-analyzing-workloads)
16. [Monitoring](#16-monitoring)
17. [Backup and Restore](#17-backup-and-restore)
18. [Failure Recovery](#18-failure-recovery)
19. [Profiling Database Performance](#19-profiling-database-performance)
20. [Diagnostic Tools](#20-diagnostic-tools)
21. [Managing Client Connections](#21-managing-client-connections)
22. [Quick Reference: Key System Tables](#22-quick-reference-key-system-tables)

---

## 1. Architecture Overview

### 1.1 Core Storage Concepts

Vertica is a **columnar, massively parallel processing (MPP)** analytical database. Data is stored column-by-column rather than row-by-row, enabling high compression ratios and efficient I/O for analytical queries that touch a subset of columns.

**Physical storage layers:**

| Layer | Description |
|---|---|
| **WOS** (Write Optimized Store) | In-memory buffer for newly loaded data. Fast writes, not persistent across crashes. |
| **ROS** (Read Optimized Store) | Columnar on-disk storage, compressed and encoded. The primary storage layer. |
| **Tuple Mover** | Background process that flushes WOS → ROS (moveout) and merges small ROS containers into larger ones (mergeout). |

**ROS containers** are immutable, columnar files. Each partition and projection stores data in one or more ROS containers. A key operational concern is keeping ROS container counts per node within limits (typically ≤ 1024 by default) to avoid **ROS pushback** — the condition where the system throttles or blocks incoming loads.

### 1.2 Projections

Projections are the **physical storage objects** in Vertica. Every table has at least one super projection covering all columns. Additional projections can be created to optimize specific query patterns.

Key properties of a projection:
- **Sort order** (`ORDER BY`): physical on-disk order, enables sort elimination and pipelined GROUP BY.
- **Segmentation** (`SEGMENTED BY HASH(...)`): distributes data across nodes.
- **Encoding** (`ENCODING`): columnar compression per column.
- **Buddy projections**: for K-safe clusters, each segmented projection has K+1 copies on different nodes.

The query optimizer automatically selects the best available projection for each query.

### 1.3 K-Safety and Fault Tolerance

**K-safety** defines the number of simultaneous node failures the database can survive while remaining operational.

- **K=0**: any single node failure shuts down the database.
- **K=1**: the database survives one node failure. Each projection has 2 copies (buddies) on different nodes.
- **K=2**: survives two simultaneous node failures. Each projection has 3 copies.

For Enterprise Mode, K-safety is set at cluster creation time. K-safety requires at least `2*K+1` nodes.

Check current K-safety:

```sql
SELECT current_fault_tolerance FROM v_monitor.system;
```

### 1.4 Epochs and Snapshot Isolation

Vertica uses **epoch-based MVCC** (Multi-Version Concurrency Control). Each committed transaction increments the epoch counter. Read queries see a consistent snapshot at a specific epoch.

Key epoch markers:
- **Current Epoch (CE)**: the epoch of the most recent committed transaction.
- **Ancient History Mark (AHM)**: the oldest epoch retained for historical queries. Data before AHM can be physically purged.
- **Last Good Epoch (LGE)**: used during recovery to identify the most recent consistent state.

```sql
-- Check current epoch state
SELECT * FROM v_monitor.system;
-- Fields: current_epoch, ahm_epoch, last_good_epoch, designed_fault_tolerance
```

### 1.5 Node States

| State | Description |
|---|---|
| `UP` | Node is running and participating in queries |
| `DOWN` | Node is offline |
| `RECOVERING` | Node is rejoining the cluster and catching up on missed data |
| `INITIALIZING` | Node is starting up |
| `STANDBY` | Active standby node, ready to replace a failed node |

```sql
SELECT node_name, node_state FROM v_monitor.nodes;
```

---

## 2. Operating the Database

### 2.1 Starting the Database

**Using admintools (interactive):**

```bash
admintools -t start_db -d <db_name>
```

**Using admintools (non-interactive):**

```bash
admintools -t start_db -d <db_name> -p <password>
```

**From vsql / SQL:**

```sql
-- Restart a specific node
SELECT RESTART_NODE('v_<db>_node0001');

-- Start the full cluster
SELECT START_DB('<db_name>');
```

**Startup sequence:**
1. Each node loads its catalog from disk.
2. Nodes synchronize catalog versions with the initiator.
3. The Tuple Mover starts and processes any pending WOS data.
4. Nodes that were `DOWN` begin recovery from buddy projections.

### 2.2 Stopping the Database

**Graceful shutdown** (waits for active queries to complete):

```bash
admintools -t stop_db -d <db_name>
```

**Forced shutdown** (terminates active connections immediately):

```bash
admintools -t stop_db -d <db_name> -F
```

**From SQL:**

```sql
SELECT SHUTDOWN();         -- graceful
SELECT SHUTDOWN(TRUE);     -- forced (immediate)
```

Graceful shutdown flushes WOS to ROS before stopping, ensuring data durability. Forced shutdown may leave WOS data unwritten — the Tuple Mover recovers this on next startup.

### 2.3 Checking Database Status

```bash
# Quick status check
admintools -t view_cluster

# Detailed node information
admintools -t list_nodes
```

```sql
-- Node status and resource usage
SELECT node_name, node_state, node_address,
       catalog_path, data_path
FROM v_catalog.nodes;

-- Is the database accepting writes?
SELECT is_ok FROM v_monitor.system;

-- Active sessions
SELECT session_id, user_name, client_hostname,
       transaction_id, is_active
FROM v_monitor.sessions;
```

### 2.4 CRC and Sort Order Checks

Vertica provides tools to validate data integrity:

```sql
-- Check for CRC errors in ROS containers
SELECT CHECK_DB_CONSISTENCY('CRC');

-- Check for sort order violations
SELECT CHECK_DB_CONSISTENCY('SORT');

-- Check a specific table
SELECT CHECK_PARTITIONS('schema.table_name');
```

These are typically run after hardware events or unexpected shutdowns.

---

## 3. Configuring the Database

### 3.1 Configuration Parameters

Vertica has configuration parameters at three scopes:

| Scope | Set with | Persistence |
|---|---|---|
| **Database** | `ALTER DATABASE ... SET PARAMETER` | Permanent, cluster-wide |
| **Session** | `ALTER SESSION SET PARAMETER` or `SET` | Current session only |
| **Node** (rare) | `ALTER NODE ... SET PARAMETER` | Permanent, node-specific override |

**View current parameter values:**

```sql
-- All database-level parameters
SELECT parameter_name, current_value, default_value, description
FROM v_monitor.configuration_parameters
ORDER BY parameter_name;

-- Single parameter
SHOW ALL;
SHOW <parameter_name>;
```

**Set a database-level parameter:**

```sql
ALTER DATABASE DEFAULT SET PARAMETER MaxClientSessions = 200;
ALTER DATABASE DEFAULT SET PARAMETER EscapeStringWarning = 0;
```

**Set a session-level parameter:**

```sql
SET TIMEZONE TO 'UTC';
SET DateStyle TO 'ISO, MDY';
ALTER SESSION SET PARAMETER QuerySamplingPercent = 100;
```

**Reset to default:**

```sql
ALTER DATABASE DEFAULT CLEAR PARAMETER <parameter_name>;
```

### 3.2 Key Configuration Parameters

| Parameter | Default | Description |
|---|---|---|
| `MaxClientSessions` | 50 | Maximum simultaneous client connections per node |
| `QuerySamplingPercent` | 0 | Percentage of queries sampled for profiling |
| `EscapeStringWarning` | 1 | Warn on non-standard escape sequences |
| `DefaultIdleSessionTimeout` | 0 (no limit) | Idle session timeout in seconds |
| `JoinDefaultTupleFormat` | `fixed` | Join buffer format: `fixed` or `variable` |
| `MoveOutInterval` | 200 | WOS moveout interval in seconds |
| `DepotMaxFileSize` | varies | Eon Mode: max depot file size |
| `EnableApportionedLoad` | 1 | Distribute COPY load across nodes |
| `CopyFaultTolerantExpressions` | 0 | Allow COPY to continue on expression errors |
| `GlobalQueryProfiling` | 0 | Enable global query profiling |
| `GlobalEEProfiling` | 0 | Enable global execution engine profiling |

### 3.3 Logical Schema Design

```sql
-- Create a schema
CREATE SCHEMA analytics;
CREATE SCHEMA analytics AUTHORIZATION alice;  -- with owner

-- Set default schema for session
SET SEARCH_PATH TO analytics, public;

-- View existing schemas
SELECT schema_name, schema_owner FROM v_catalog.schemata;

-- Move a table between schemas
ALTER TABLE public.fact_orders SET SCHEMA analytics;
```

---

## 4. Database Users and Privileges

### 4.1 User Management

```sql
-- Create a user
CREATE USER alice IDENTIFIED BY 'StrongPassword123!';

-- Create user with no login (used as role owner)
CREATE USER svc_account IDENTIFIED BY 'pass' ACCOUNT LOCK;

-- Modify user
ALTER USER alice IDENTIFIED BY 'NewPassword456!';
ALTER USER alice ACCOUNT LOCK;     -- lock account
ALTER USER alice ACCOUNT UNLOCK;   -- unlock account

-- Password expiry
ALTER USER alice PASSWORD EXPIRE;

-- Connection limit
ALTER USER alice MAXCONNECTIONS 10 ON DATABASE;

-- Drop user
DROP USER alice;
DROP USER alice CASCADE;  -- also drops user's objects
```

```sql
-- View users
SELECT user_name, is_superuser, account_locked, password_change_time
FROM v_catalog.users;
```

### 4.2 Predefined Roles

| Role | Description |
|---|---|
| `dbadmin` | Full database administration privileges (superuser) |
| `pseudosuperuser` | Most superuser privileges except managing other superusers |
| `dbduser` | Can create and drop database designer objects |
| `sysmonitor` | Read-only access to v_monitor system tables |
| `udxdeveloper` | Can create user-defined extensions (UDFs, UDTs) |
| `mlsupervisor` | Manages machine learning models |
| `PUBLIC` | Implicit role granted to every user |

### 4.3 Creating and Managing Custom Roles

```sql
-- Create a role
CREATE ROLE etl_writer;
CREATE ROLE analyst_read;

-- Grant privileges to a role
GRANT USAGE ON SCHEMA analytics TO etl_writer;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA analytics TO etl_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO analyst_read;

-- Grant role to a user
GRANT etl_writer TO alice;
GRANT analyst_read TO bob WITH ADMIN OPTION;  -- bob can re-grant this role

-- Enable role for current session (if not auto-granted)
SET ROLE etl_writer;
SET ROLE ALL;    -- enable all granted roles

-- Auto-grant role on login
ALTER USER alice DEFAULT ROLE etl_writer;

-- Revoke role from user
REVOKE etl_writer FROM alice;

-- Drop role
DROP ROLE etl_writer;
```

```sql
-- View role grants
SELECT granted_role, grantee, admin_option
FROM v_catalog.grants
WHERE object_type = 'ROLE';
```

### 4.4 Object Privileges

Vertica privilege model follows a chain: Database → Schema → Table/View → Column.

**Schema privileges:**

```sql
GRANT USAGE ON SCHEMA analytics TO analyst_read;    -- can see schema objects
GRANT CREATE ON SCHEMA analytics TO etl_writer;     -- can create objects in schema
GRANT ALL ON SCHEMA analytics TO alice;
REVOKE CREATE ON SCHEMA analytics FROM etl_writer;
```

**Table privileges:**

```sql
GRANT SELECT ON TABLE analytics.fact_orders TO analyst_read;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE analytics.fact_orders TO etl_writer;
GRANT ALL ON ALL TABLES IN SCHEMA analytics TO alice;

-- Grant to PUBLIC (all users)
GRANT SELECT ON TABLE analytics.dim_users TO PUBLIC;
```

**Column-level privileges:**

```sql
-- Grant SELECT on specific columns only
GRANT SELECT(order_id, order_date, amount) ON analytics.fact_orders TO analyst_read;
```

**Check effective privileges:**

```sql
SELECT * FROM v_catalog.grants WHERE object_name = 'fact_orders';

-- What privileges does a user have?
SELECT HAS_TABLE_PRIVILEGE('alice', 'analytics.fact_orders', 'SELECT');
```

### 4.5 Row-Level Access Policies

Access policies restrict which rows a user can read or modify:

```sql
-- Allow users to see only their own rows
CREATE ACCESS POLICY ON analytics.fact_orders
FOR ROWS WHERE user_id = (
    SELECT user_id FROM analytics.dim_users
    WHERE username = CURRENT_USER
)
TO analyst_read
GRANT;

-- View existing policies
SELECT table_name, policy_expression, grantee
FROM v_catalog.access_policies;

-- Drop policy
DROP ACCESS POLICY ON analytics.fact_orders FOR ROWS TO analyst_read;
```

---

## 5. Projections

### 5.1 Projection Types

| Type | Description |
|---|---|
| **Super projection** | Auto-created for every table; covers all columns; used when no better projection exists |
| **Query-specific projection** | Created manually to cover specific query patterns |
| **Segmented projection** | Distributes data across all nodes via HASH; for large tables |
| **Unsegmented projection** | Replicates data to all nodes; for small dimension tables |
| **Buddy projection** | Copy of a segmented projection on a different node; created automatically for K-safe clusters |

### 5.2 Creating Projections

**Segmented projection (large tables):**

```sql
CREATE PROJECTION analytics.fact_orders_by_date
(
    order_date    ENCODING DELTAVAL,
    status        ENCODING RLE,
    order_id      ENCODING DELTA,
    user_id       ENCODING DELTA,
    amount        ENCODING AUTO,
    currency      ENCODING AUTO
)
AS SELECT
    order_date, status, order_id, user_id, amount, currency
FROM analytics.fact_orders
ORDER BY order_date, status, order_id
SEGMENTED BY HASH(order_id) ALL NODES;
```

**Unsegmented projection (small dimension tables):**

```sql
CREATE PROJECTION analytics.dim_countries_all
AS SELECT country_id, country_name, region, iso_code
FROM analytics.dim_countries
UNSEGMENTED ALL NODES;
```

**Key design rules:**
- Put the most selective predicate columns **first** in `ORDER BY`.
- Segmentation key should have **high cardinality** (e.g., primary key) for even data distribution.
- Choose the same segmentation key as the tables you frequently join with.
- Avoid sorting on `LONG VARBINARY` and `LONG VARCHAR` columns.
- Use `ENCODING AUTO` as default; apply `RLE` to leading low-cardinality sort columns.

### 5.3 Auto-Projections

When a table is created without an explicit projection, Vertica auto-creates a super projection. You can also trigger automatic projection creation via Database Designer:

```sql
-- Analyze workload and create projections for a schema
SELECT DESIGNER_CREATE_PROJECTIONS('my_schema', 'my_design');
```

### 5.4 Projection Naming

Projections follow naming convention: `table_name_b0`, `table_name_b1`, etc. for buddy projections. Custom projections use whatever name you assign, with buddies getting `_b0`, `_b1` suffixes automatically.

### 5.5 Refreshing Projections

New projections created on existing tables are empty until refreshed:

```sql
-- Synchronous refresh (foreground, blocks until complete)
SELECT REFRESH('analytics.fact_orders');

-- Asynchronous refresh (background)
SELECT START_REFRESH();  -- refreshes all out-of-date projections in current schema

-- Force AHM to advance so purges and historical queries work correctly
SELECT MAKE_AHM_NOW();
```

Check projection status:

```sql
SELECT projection_name, is_up_to_date, refresh_state, refresh_progress
FROM v_monitor.projection_refreshes;

-- Detailed projection info
SELECT GET_PROJECTIONS('analytics.fact_orders');
```

### 5.6 Dropping Projections

```sql
-- Drop a specific projection
DROP PROJECTION analytics.fact_orders_by_date;

-- Drop all non-super projections for a table (keep super)
SELECT DROP_PROJECTIONS('analytics.fact_orders', 'NON_SUPER');

-- Cannot drop the last remaining projection for a table
-- Must have at least one projection (the super projection)
```

### 5.7 Checking Projections

```sql
-- List all projections for a table
SELECT projection_name, is_super_projection, is_up_to_date,
       has_statistics, is_segmented, is_on_all_nodes
FROM v_catalog.projections
WHERE projection_schema = 'analytics'
  AND anchor_table_name = 'fact_orders';

-- Projection column details: sort order, encoding, NULL order
SELECT column_name, sort_position, sort_order, sort_null_order, encoding_type
FROM v_catalog.projection_columns
WHERE projection_name = 'fact_orders_by_date_b0'
ORDER BY sort_position;

-- Storage used per projection
SELECT projection_name, SUM(used_bytes) / 1e9 AS used_gb, SUM(row_count) AS rows
FROM v_monitor.projection_storage
WHERE anchor_table_schema = 'analytics'
GROUP BY projection_name
ORDER BY used_gb DESC;
```

---

## 6. Partitioning Tables

### 6.1 Defining a Partition Clause

Partitioning is a table-level property applied to all projections. The Tuple Mover automatically organizes loaded data into separate ROS containers per partition.

**Basic date partitioning:**

```sql
CREATE TABLE analytics.fact_orders (
    order_id    BIGINT  NOT NULL,
    user_id     BIGINT  NOT NULL,
    order_date  DATE    NOT NULL,
    status      VARCHAR(32),
    amount      NUMERIC(18,2)
)
PARTITION BY order_date::DATE;
```

**Partition by year:**

```sql
PARTITION BY YEAR(order_date)
```

**Partition by year-month:**

```sql
PARTITION BY TO_CHAR(order_date, 'YYYY-MM')
```

### 6.2 Hierarchical Partitioning with CALENDAR_HIERARCHY_DAY

For time-series tables accumulating data over years, daily partitioning creates hundreds or thousands of ROS containers per node, risking pushback. `CALENDAR_HIERARCHY_DAY` groups older partitions automatically:

```sql
CREATE TABLE analytics.fact_events (
    event_id    BIGINT     NOT NULL,
    user_id     BIGINT     NOT NULL,
    event_date  DATE       NOT NULL,
    event_type  VARCHAR(64),
    value       NUMERIC(18,4)
)
PARTITION BY event_date::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(event_date::DATE, 2, 2);
```

**`CALENDAR_HIERARCHY_DAY(expr, active_months, active_years)`:**

| Parameter | Description | Recommended |
|---|---|---|
| `active_months` | Data within this many months stays in daily ROS containers | 2 |
| `active_years` | Data within this many years stays in monthly containers | 2 |

**Resulting hierarchy** (example with current date 2026-05-16, params 2, 2):

| Age | Container granularity |
|---|---|
| Within last 2 months (Mar–May 2026) | Daily containers |
| Within 2024–early 2026 (active years) | Monthly containers |
| Before 2024 | Yearly containers |

The Tuple Mover automatically regroups containers as time advances. This reduced example: 809 daily containers → 40 grouped containers per node.

**Add/change partition clause on existing table:**

```sql
ALTER TABLE analytics.fact_orders
    PARTITION BY order_date::DATE
    GROUP BY CALENDAR_HIERARCHY_DAY(order_date::DATE, 2, 2);
```

### 6.3 Partition Pruning

The query optimizer automatically prunes irrelevant ROS containers when the WHERE clause filters on the partition expression. Each ROS container maintains its min/max partition key values.

**Requirements for partition pruning:**
1. WHERE predicate must use the **same expression** as the PARTITION BY clause.
2. Predicates must be connected with `AND` (not `OR` — OR disables pruning).
3. The partition column must not be wrapped in a function that differs from the partition expression.

```sql
-- Pruning ENABLED: matches PARTITION BY order_date::DATE
WHERE order_date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31'
WHERE order_date = DATE '2026-05-15'
WHERE order_date >= DATE '2026-01-01' AND order_date < DATE '2026-04-01'

-- Pruning DISABLED:
WHERE order_date = '...' OR status = '...'   -- OR disables pruning
WHERE YEAR(order_date) = 2026                -- different expression from partition key
```

Verify pruning in EXPLAIN: look for `Partition Columns:` and reduced container counts.

### 6.4 Managing Partitions

**View partitions:**

```sql
SELECT partition_key, node_name, ros_count, ros_row_count, ros_used_bytes
FROM v_monitor.partitions
WHERE table_schema = 'analytics' AND table_name = 'fact_orders'
ORDER BY partition_key;
```

**Drop partitions** (fast, drops ROS containers directly):

```sql
-- Drop a single partition
SELECT DROP_PARTITIONS('analytics.fact_orders', '2023-01-01', '2023-01-31');

-- Drop a range
SELECT DROP_PARTITIONS('analytics.fact_orders', '2022-01-01', '2022-12-31');

-- SQL syntax (equivalent)
ALTER TABLE analytics.fact_orders
    DROP PARTITION BETWEEN '2022-01-01' AND '2022-12-31';
```

**Swap partitions** (atomic replace — used for bulk partition reload):

```sql
-- 1. Load new data into a staging table with identical schema
CREATE TABLE staging.fact_orders_202601
    LIKE analytics.fact_orders INCLUDING PROJECTIONS;

COPY staging.fact_orders_202601 FROM '/data/orders/2026-01/*.csv' DELIMITER ',' DIRECT;

-- 2. Swap the partition atomically
SELECT SWAP_PARTITIONS_BETWEEN_TABLES(
    'staging.fact_orders_202601',  -- source (new data)
    '2026-01-01',                  -- partition range start
    '2026-01-31',                  -- partition range end
    'analytics.fact_orders'        -- target
);

-- 3. Clean up staging
DROP TABLE staging.fact_orders_202601;
```

**Copy partitions** (archive/move data between tables):

```sql
SELECT COPY_PARTITIONS_TO_TABLE(
    'analytics.fact_orders',       -- source
    '2023-01-01',
    '2023-12-31',
    'archive.fact_orders_2023'     -- destination
);

SELECT MOVE_PARTITIONS_TO_TABLE(
    'analytics.fact_orders',       -- source (partitions removed)
    '2023-01-01',
    '2023-12-31',
    'archive.fact_orders_2023'     -- destination
);
```

**Active vs Inactive partitions:**

```sql
-- View active/inactive partition status
SELECT partition_key, is_active
FROM v_catalog.table_partitions
WHERE table_schema = 'analytics' AND table_name = 'fact_orders';
```

---

## 7. Constraints

### 7.1 Supported Constraint Types

| Type | Creation | Enforcement |
|---|---|---|
| `PRIMARY KEY` | `pk_col PRIMARY KEY` or `CONSTRAINT name PRIMARY KEY (col)` | Optional (ENABLED/DISABLED) |
| `UNIQUE` | `CONSTRAINT name UNIQUE (col)` | Optional |
| `FOREIGN KEY` | `CONSTRAINT name FOREIGN KEY (col) REFERENCES ...` | Optional |
| `NOT NULL` | `col TYPE NOT NULL` | Always enforced |
| `CHECK` | `CONSTRAINT name CHECK (expr)` | Optional |

### 7.2 Constraint Enforcement

Unlike OLTP databases, Vertica constraints are **optional enforcement** by default. This is a deliberate design choice for analytical workloads:

- Enforcing PK/UNIQUE on large fact tables incurs significant overhead on every INSERT/UPDATE/COPY.
- The optimizer can still **use constraint metadata** (without enforcing it) to build better query plans.

Enforcement states:

| State | Meaning |
|---|---|
| `ENABLED` | Constraint is actively checked on writes |
| `DISABLED` | Constraint exists in metadata but is not checked |
| `VALIDATED` | All existing rows comply with the constraint |
| `UNVALIDATED` | Existing rows may violate the constraint |

**Important:** `ENABLED` primary key constraints help the optimizer produce faster join plans because it can assume no duplicate key values exist.

```sql
-- Create table with constraints
CREATE TABLE analytics.fact_orders (
    order_id    BIGINT     NOT NULL,
    user_id     BIGINT     NOT NULL REFERENCES analytics.dim_users(user_id),
    order_date  DATE       NOT NULL,
    status      VARCHAR(32) DEFAULT 'pending',
    amount      NUMERIC(18,2),
    CONSTRAINT pk_orders PRIMARY KEY (order_id) ENABLED
);

-- Add constraint to existing table
ALTER TABLE analytics.fact_orders
    ADD CONSTRAINT pk_orders PRIMARY KEY (order_id) ENABLED;

-- Disable constraint (keep metadata, skip checking)
ALTER TABLE analytics.fact_orders
    ALTER CONSTRAINT pk_orders DISABLED;

-- Enable enforcement
ALTER TABLE analytics.fact_orders
    ALTER CONSTRAINT pk_orders ENABLED;

-- Drop constraint
ALTER TABLE analytics.fact_orders
    DROP CONSTRAINT pk_orders;
```

**Check for violations:**

```sql
SELECT ANALYZE_CONSTRAINTS('analytics.fact_orders');
```

---

## 8. Working with Native Tables

### 8.1 Table Creation

```sql
-- Standard table
CREATE TABLE analytics.fact_orders (
    order_id   BIGINT        NOT NULL,
    user_id    BIGINT        NOT NULL,
    order_date DATE          NOT NULL,
    status     VARCHAR(32)   NOT NULL DEFAULT 'pending',
    amount     NUMERIC(18,2) NOT NULL,
    currency   CHAR(3)       NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP     NOT NULL DEFAULT NOW()
)
ORDER BY order_date, user_id
SEGMENTED BY HASH(order_id) ALL NODES
PARTITION BY order_date::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(order_date::DATE, 2, 2);

-- Temporary table (session-scoped, auto-dropped)
CREATE LOCAL TEMP TABLE tmp_staging (
    order_id BIGINT,
    amount   NUMERIC(18,2)
) ON COMMIT PRESERVE ROWS;

-- Create table from query (CTAS)
CREATE TABLE archive.fact_orders_2023 AS
SELECT * FROM analytics.fact_orders
WHERE order_date BETWEEN DATE '2023-01-01' AND DATE '2023-12-31';

-- Create from existing table structure
CREATE TABLE staging.fact_orders_load
    LIKE analytics.fact_orders INCLUDING PROJECTIONS;
```

### 8.2 Loading Data with COPY

`COPY` is the primary bulk load mechanism. It is faster than INSERT for large datasets.

**Basic file load:**

```sql
COPY analytics.fact_orders (order_id, user_id, order_date, status, amount, currency)
FROM '/data/orders/2026-05-01.csv'
DELIMITER ','
ENCLOSED BY '"'
SKIP 1                         -- skip header row
NULL AS ''                     -- treat empty strings as NULL
REJECTMAX 100                  -- allow up to 100 rejected rows
EXCEPTIONS '/tmp/errors.txt'   -- log rejected rows
REJECTED DATA '/tmp/rejects.csv';
```

**Important COPY modes:**

| Mode | Description |
|---|---|
| *(default)* | Loads to WOS (in-memory buffer); flushed by Tuple Mover |
| `DIRECT` | Bypasses WOS, writes directly to ROS; use for large loads (> a few million rows) |
| `TRICKLE` | Forces WOS even for large loads; use for continuous small-batch streaming |

```sql
-- Direct mode for large loads
COPY analytics.fact_orders FROM '/data/orders/large_file.csv'
DELIMITER ',' SKIP 1 DIRECT;
```

**Load from multiple files / directory:**

```sql
COPY analytics.fact_orders FROM '/data/orders/2026-05-*.csv' DELIMITER ',';
```

**Distributed load across nodes (apportioned load):**

```sql
-- Each node loads a portion of the file set from its local path
COPY analytics.fact_orders FROM '/data/orders/part-*.csv' ON ANY NODE DELIMITER ',';
```

**COPY from STDIN (programmatic loading):**

```sql
COPY analytics.fact_orders FROM STDIN DELIMITER ',';
```

**Supported file formats:**

| Format | Option |
|---|---|
| CSV | `DELIMITER ','` |
| TSV | `DELIMITER '\t'` |
| ORC | `ORC` |
| Parquet | `PARQUET` |
| JSON | `PARSER fjsonparser()` |
| Avro | `PARSER favroparser()` |
| Fixed-width | `FIXED WIDTH` |

### 8.3 Altering Tables

```sql
-- Add column
ALTER TABLE analytics.fact_orders ADD COLUMN region VARCHAR(64);
ALTER TABLE analytics.fact_orders ADD COLUMN is_refunded BOOLEAN DEFAULT FALSE NOT NULL;

-- Drop column
ALTER TABLE analytics.fact_orders DROP COLUMN region;
ALTER TABLE analytics.fact_orders DROP COLUMN region CASCADE; -- drops dependent projections

-- Rename column
ALTER TABLE analytics.fact_orders RENAME COLUMN region TO sales_region;

-- Modify default
ALTER TABLE analytics.fact_orders ALTER COLUMN status SET DEFAULT 'pending';
ALTER TABLE analytics.fact_orders ALTER COLUMN region DROP DEFAULT;

-- NOT NULL constraint
ALTER TABLE analytics.fact_orders ALTER COLUMN region SET NOT NULL;
ALTER TABLE analytics.fact_orders ALTER COLUMN region DROP NOT NULL;

-- Rename table
ALTER TABLE analytics.fact_orders RENAME TO fact_orders_v2;

-- Change schema
ALTER TABLE analytics.fact_orders SET SCHEMA archive;

-- Change owner
ALTER TABLE analytics.fact_orders OWNER TO alice;
```

### 8.4 Removing Table Data

**DELETE** — logical deletion, rows remain until purged:

```sql
DELETE FROM analytics.fact_orders WHERE order_date < DATE '2022-01-01';
DELETE FROM analytics.fact_orders WHERE order_id = 9999;
```

**TRUNCATE** — removes all rows and history immediately:

```sql
TRUNCATE TABLE staging.fact_orders_load;
```

**DROP TABLE:**

```sql
DROP TABLE IF EXISTS staging.fact_orders_load;
DROP TABLE staging.fact_orders_load CASCADE; -- drops dependent views/projections
```

### 8.5 Purging Deleted Data

After DELETE operations, rows are only logically marked. Physical space is reclaimed via purge:

```sql
-- Purge entire table
SELECT PURGE_TABLE('analytics.fact_orders');

-- Purge a specific partition range
SELECT PURGE_PARTITION('analytics.fact_orders', '2023-01-01', '2023-12-31');

-- Purge deleted data across the whole database
SELECT PURGE();
```

**When to purge:**
- After deleting ≥ 10% of a table's rows — query performance degrades due to delete vectors.
- After large partition drops.
- To reclaim disk space before a storage limit is reached.
- Before cluster recovery — large unprocessed delete markers slow recovery.

**Set an automatic purge policy:**

```sql
ALTER TABLE analytics.fact_orders SET PURGE POLICY INTERVAL '30 days';
```

### 8.6 Tuple Mover Operations

The Tuple Mover (TM) runs continuously in the background:

| Operation | Description |
|---|---|
| **Moveout** | Flushes WOS data to ROS containers on disk |
| **Mergeout** | Merges many small ROS containers into fewer larger ones |
| **Reprojection** | Refreshes data in out-of-date projections |
| **Partition grouping** | Applies CALENDAR_HIERARCHY_DAY grouping to aging data |

Manually trigger Tuple Mover operations:

```sql
-- Trigger moveout on current node
SELECT DO_TM_TASK('moveout');

-- Trigger mergeout for a specific table
SELECT DO_TM_TASK('mergeout', 'analytics.fact_orders');

-- Trigger reprojection
SELECT DO_TM_TASK('reprojection', 'analytics.fact_orders');
```

Monitor Tuple Mover activity:

```sql
SELECT * FROM v_monitor.tuple_mover_operations
ORDER BY start_time DESC LIMIT 20;

SELECT * FROM v_monitor.tuple_mover_pool_status;
```

---

## 9. Transactions and Locking

### 9.1 Transaction Model

Vertica implements standard ACID transactions with implicit transaction start. You do not need an explicit `BEGIN` in most cases.

```sql
BEGIN;                        -- explicit start (optional)
INSERT INTO t VALUES (1, 'a');
UPDATE t SET col = 'b' WHERE id = 2;
COMMIT;

BEGIN;
DELETE FROM t WHERE id = 3;
ROLLBACK;                     -- undo changes

-- Savepoints
BEGIN;
INSERT INTO t VALUES (10, 'x');
SAVEPOINT sp1;
INSERT INTO t VALUES (11, 'y');
ROLLBACK TO SAVEPOINT sp1;    -- undo after sp1
COMMIT;                       -- commit changes before sp1
```

**Auto-commit behavior:**
- `COPY` automatically commits the current transaction (except for temporary tables).
- DDL statements (`CREATE`, `ALTER`, `DROP`) are auto-committed.
- Internal Tuple Mover and refresh operations run at `SERIALIZABLE` isolation.

### 9.2 Isolation Levels

| Level | Dirty reads | Non-repeatable reads | Phantom reads |
|---|---|---|---|
| `READ COMMITTED` (default) | No | Yes | Yes |
| `SERIALIZABLE` | No | No | No |

Notes:
- `READ UNCOMMITTED` is treated as `READ COMMITTED`.
- `REPEATABLE READ` is treated as `SERIALIZABLE`.
- Isolation level can be set per transaction or per session.

```sql
-- Set for session
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;

-- Set for single transaction
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- Set database default
ALTER DATABASE DEFAULT SET PARAMETER TransactionIsolationLevel = 'SERIALIZABLE';
```

### 9.3 Locks

Vertica uses locks to maintain data concurrency:

| Lock type | Who holds it | Compatibility |
|---|---|---|
| **S (Shared)** | Read queries | Compatible with other S locks |
| **X (Exclusive)** | DDL, DELETE, UPDATE, COPY | Incompatible with S and X |
| **O (Owner)** | Table owner operations | Special DDL operations |
| **U (Update)** | UPDATE targeting specific rows | Intermediate before upgrading to X |

```sql
-- View active locks
SELECT * FROM v_monitor.locks
ORDER BY lock_mode, object_name;

-- View lock attempts (blocked queries)
SELECT * FROM v_monitor.lock_attempts
ORDER BY request_timestamp DESC;
```

**Key locking behaviors:**
- `SELECT` acquires S locks; multiple reads run concurrently.
- `DELETE` and `UPDATE` acquire X locks on the table — only one can run at a time per table.
- DDL operations (ALTER TABLE, DROP TABLE) require X locks and wait for active queries to finish.
- `COPY` with default mode acquires an S lock on the table (allowing concurrent reads).

**Lock timeouts:**

```sql
-- Set lock wait timeout (seconds)
ALTER SESSION SET PARAMETER LockTimeout = 30;
```

---

## 10. Managing Queries

### 10.1 EXPLAIN Command

```sql
-- Standard plan
EXPLAIN SELECT u.country, SUM(o.amount)
FROM analytics.fact_orders o
JOIN analytics.dim_users u ON o.user_id = u.user_id
WHERE o.order_date >= DATE '2026-01-01'
GROUP BY u.country;

-- JSON format
EXPLAIN JSON SELECT ...;

-- Verbose (includes variable-length format details)
EXPLAIN VERBOSE SELECT ...;

-- Local plan only (single-node view)
EXPLAIN LOCAL SELECT ...;
```

**Key EXPLAIN signals:**

| Signal | Meaning |
|---|---|
| `JOIN HASH` | Hash join — no sorted projection on join key |
| `JOIN MERGEJOIN(inputs presorted)` | Merge join — optimal |
| `RESEGMENT` | Cross-node data redistribution — check segmentation |
| `BROADCAST` | Small table broadcast to all nodes — expected for small dims |
| `GROUPBY HASH` | Hash aggregation — check projection ORDER BY |
| `GROUPBY PIPELINED` | Sort-based streaming aggregation — optimal |
| `GROUPBY PIPELINED (RESEGMENT GROUPS)` | Pipelined but with resegmentation — fix segmentation |
| `Top-K` | LIMIT optimization active |

### 10.2 Query Events

```sql
-- View optimizer warnings and suggestions for recent queries
SELECT event_timestamp, event_type, event_description, suggested_action
FROM v_monitor.query_events
WHERE event_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY event_timestamp DESC;
```

### 10.3 Monitoring Active Queries

```sql
-- Active queries right now
SELECT session_id, user_name, current_statement,
       query_start, DATEDIFF('second', query_start, NOW()) AS elapsed_s
FROM v_monitor.sessions
WHERE current_statement IS NOT NULL
ORDER BY elapsed_s DESC;

-- Query history
SELECT start_timestamp, end_timestamp, user_name,
       DATEDIFF('millisecond', start_timestamp, end_timestamp) AS duration_ms,
       request_label, query_duration_us
FROM v_monitor.query_requests
WHERE start_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY duration_ms DESC
LIMIT 50;
```

### 10.4 Killing Queries

```sql
-- Kill a specific session (closes connection)
SELECT CLOSE_SESSION('<session_id>');

-- Kill all sessions for a user
SELECT CLOSE_USER_SESSIONS('alice');

-- Cancel a running statement without closing connection
SELECT INTERRUPT_STATEMENT('<session_id>', '<statement_id>');
```

### 10.5 Directed Queries

Directed queries save an optimized query plan and reuse it for subsequent executions, bypassing the optimizer:

```sql
-- 1. Generate an annotated plan
SELECT EXPLAIN /*+verbatim*/ SELECT u.country, SUM(o.amount)
FROM analytics.fact_orders o
JOIN analytics.dim_users u ON o.user_id = u.user_id
GROUP BY u.country;

-- 2. Save the directed query
CREATE DIRECTED QUERY CUSTOM 'country_revenue_plan'
SELECT /*+verbatim*/ u.country, SUM(o.amount)
FROM analytics.fact_orders o
JOIN analytics.dim_users u ON o.user_id = u.user_id
GROUP BY u.country;

-- 3. Activate it (optimizer uses it for matching queries)
ALTER DIRECTED QUERY 'country_revenue_plan' ACTIVATE;

-- View directed queries
SELECT query_name, is_active
FROM v_catalog.directed_queries;
```

---

## 11. Resource Management

### 11.1 Resource Pools

Resource pools control how much memory and concurrency each category of query receives.

**Built-in pools:**

| Pool | Purpose |
|---|---|
| `general` | Default pool for user queries |
| `wos` | Memory for WOS (write buffer) |
| `recovery` | Node recovery operations |
| `refresh` | Projection refresh operations |
| `tm` | Tuple Mover operations |
| `jvm` | Java-based UDFs |

### 11.2 Pool Parameters

| Parameter | Description |
|---|---|
| `MEMORYSIZE` | Guaranteed memory for queries in this pool (e.g., `'2G'`) |
| `MAXMEMORYSIZE` | Maximum memory (can borrow from `GENERAL` up to this) |
| `PLANNEDCONCURRENCY` | Expected number of concurrent queries |
| `MAXCONCURRENCY` | Hard limit on simultaneous queries |
| `PRIORITY` | Scheduling priority (higher = more resources) |
| `RUNTIMECAP` | Maximum runtime per query (e.g., `'5 minutes'`) |
| `QUEUETIMEOUT` | How long a query can queue before timing out |
| `EXECUTIONPARALLELISM` | Number of execution threads per query |
| `SINGLEINITIATOR` | Force single-node execution |

### 11.3 Creating and Managing Pools

```sql
-- Create a resource pool
CREATE RESOURCE POOL analytics_pool
    MEMORYSIZE '4G'
    MAXMEMORYSIZE '8G'
    PLANNEDCONCURRENCY 5
    MAXCONCURRENCY 10
    PRIORITY 0
    RUNTIMECAP '30 minutes'
    QUEUETIMEOUT '5 minutes';

-- Modify an existing pool
ALTER RESOURCE POOL analytics_pool
    RUNTIMECAP '1 hour'
    MAXCONCURRENCY 15;

-- Drop a pool
DROP RESOURCE POOL analytics_pool;

-- Assign a user to a resource pool
ALTER USER alice RESOURCE POOL analytics_pool;

-- Assign a role to a resource pool
ALTER ROLE analyst_read RESOURCE POOL analytics_pool;

-- Session-level pool override
SET SESSION RESOURCE_POOL = analytics_pool;
```

### 11.4 Monitoring Resource Pools

```sql
-- Current resource pool allocation and usage
SELECT pool_name, memory_size_kb, max_memory_size_kb,
       memory_inuse_kb, running_query_count, queued_query_count
FROM v_monitor.resource_pool_status
ORDER BY pool_name;

-- View user → pool assignments
SELECT user_name, resource_pool
FROM v_catalog.users
WHERE resource_pool != 'general';

-- Active queries per pool
SELECT pool_name, COUNT(*) AS active_queries
FROM v_monitor.sessions
GROUP BY pool_name;
```

---

## 12. Managing Storage Locations

### 12.1 Storage Location Types

Each Vertica node requires at least two storage locations:
- One for **data** (ROS files, catalog)
- One for **temporary** files (intermediate query results, sort spill)

**Usage types:**

| Usage | Description |
|---|---|
| `DATA,TEMP` | Stores both ROS data and temp files (default) |
| `DATA` | Data only |
| `TEMP` | Temporary files only |

### 12.2 Creating Storage Locations

```sql
-- Add a storage location to all nodes
ALTER LOCATION '/data/vertica_storage2' ALL NODES
    USAGE 'DATA';

-- Add to specific node
ALTER LOCATION '/fast_ssd/vertica_temp' NODE v_mydb_node0002
    USAGE 'TEMP';

-- View storage locations
SELECT node_name, location_path, location_usage,
       disk_space_free_mb, disk_space_used_mb
FROM v_catalog.storage_locations;
```

### 12.3 Storage Policies

Storage policies control which data lands in which storage location (e.g., recent data on fast SSD, old data on slow HDD):

```sql
-- Label a storage location
ALTER LOCATION '/slow_hdd/archive' ALL NODES LABEL 'cold_storage';
ALTER LOCATION '/fast_ssd/recent' ALL NODES LABEL 'hot_storage';

-- Create a storage policy for a table
ALTER TABLE analytics.fact_orders
    SET POLICY 'hot_storage' WHERE (order_date >= ADD_MONTHS(CURRENT_DATE, -3));
ALTER TABLE analytics.fact_orders
    SET POLICY 'cold_storage' WHERE (order_date < ADD_MONTHS(CURRENT_DATE, -3));
```

### 12.4 Retiring a Storage Location

```sql
-- Mark as retired (Vertica stops writing here)
ALTER LOCATION '/old_disk/data' ALL NODES RETIRE;

-- Move data away from retired location
SELECT REALIGN_CONTROL_PROJECTION('analytics.fact_orders');

-- After data is moved, drop the location
ALTER LOCATION '/old_disk/data' ALL NODES DROP;
```

---

## 13. Managing the Database

### 13.1 Node Management

```sql
-- View node status
SELECT node_name, node_state, node_address, is_primary
FROM v_catalog.nodes;

-- Restart a specific node
SELECT RESTART_NODE('v_mydb_node0003');

-- Start all DOWN nodes
SELECT START_DB('mydb');
```

**Adding a node (Enterprise Mode):**

1. Install Vertica on the new host.
2. Run `admintools` → Add Host to Cluster.
3. Run `admintools` → Expand Cluster.
4. Rebalance data across the new node count.

**Rebalancing after adding nodes:**

```sql
-- Rebalance data via SQL
SELECT REBALANCE_CLUSTER();

-- Monitor rebalance progress
SELECT * FROM v_monitor.rebalance_projection_status;
```

### 13.2 Disk Space Management

```sql
-- Check disk space per node
SELECT node_name, location_path,
       disk_space_total_mb,
       disk_space_used_mb,
       ROUND(100.0 * disk_space_used_mb / disk_space_total_mb, 1) AS pct_used
FROM v_catalog.storage_locations
ORDER BY node_name, location_path;

-- Check table storage
SELECT anchor_table_schema, anchor_table_name,
       SUM(used_bytes) / 1e9 AS used_gb,
       SUM(ros_count) AS ros_containers
FROM v_monitor.projection_storage
GROUP BY 1, 2
ORDER BY used_gb DESC;

-- Check for large delete vectors (unreclaimed space)
SELECT table_schema, table_name, deleted_row_count, ros_count
FROM v_monitor.delete_vectors
WHERE deleted_row_count > 0
ORDER BY deleted_row_count DESC;
```

**Reclaim disk space:**

```sql
-- Purge all deleted data in the database
SELECT PURGE();

-- Trim memory allocations on all nodes
SELECT TRIM_MEMORY();
```

### 13.3 Managing the Tuple Mover

```sql
-- View recent TM operations
SELECT node_name, operation, is_running, start_time, duration_ms
FROM v_monitor.tuple_mover_operations
ORDER BY start_time DESC LIMIT 20;

-- Current TM pool status
SELECT * FROM v_monitor.tuple_mover_pool_status;

-- Manually trigger operations
SELECT DO_TM_TASK('moveout');           -- flush WOS to ROS
SELECT DO_TM_TASK('mergeout');          -- consolidate ROS containers
SELECT DO_TM_TASK('mergeout', 'analytics.fact_orders');  -- for specific table
```

**ROS container count monitoring** (to prevent pushback):

```sql
SELECT node_name, SUM(ros_count) AS total_ros_containers
FROM v_monitor.projection_storage
GROUP BY node_name
ORDER BY total_ros_containers DESC;

-- Per-table ROS count
SELECT anchor_table_schema, anchor_table_name, node_name, SUM(ros_count) AS ros_count
FROM v_monitor.projection_storage
GROUP BY 1, 2, 3
HAVING SUM(ros_count) > 100
ORDER BY ros_count DESC;
```

If ROS container count approaches the limit (typically 1024 per node), trigger mergeout or review partition strategy.

---

## 14. Collecting Database Statistics

### 14.1 ANALYZE_STATISTICS

The query optimizer uses statistics to make planning decisions (projection selection, join ordering, aggregation strategy). Statistics become stale as data changes.

**The optimizer checks statistics in this order:**
1. Partition-level statistics (for partitioned tables).
2. Table-level statistics.
3. If neither exists: assumes uniform distribution and equal storage.

Stale or missing statistics → suboptimal query plans.

```sql
-- Full table statistics
SELECT ANALYZE_STATISTICS('analytics.fact_orders');

-- Single column (faster for targeted refresh)
SELECT ANALYZE_STATISTICS('analytics.fact_orders', 'order_date');

-- Full schema
SELECT ANALYZE_STATISTICS('analytics');

-- Using a sample (faster, approximate)
SELECT ANALYZE_STATISTICS('analytics.fact_orders', 50);   -- 50% sample
```

### 14.2 When to Run Statistics

| Trigger | Action |
|---|---|
| After initial data load | Full `ANALYZE_STATISTICS` |
| After adding a new projection | `ANALYZE_STATISTICS` on the table |
| After large INSERT/COPY (≥ 10% change) | Full or column-level refresh |
| After partition operations | `ANALYZE_STATISTICS` on affected partitions |
| Regularly scheduled | Weekly or after each major ETL batch |

### 14.3 Statistics Metadata

```sql
-- Check when statistics were last collected
SELECT table_name, column_name, statistics_updated_timestamp
FROM v_catalog.column_statistics
WHERE table_schema = 'analytics'
ORDER BY statistics_updated_timestamp ASC;

-- Check projection statistics status
SELECT projection_name, has_statistics
FROM v_catalog.projections
WHERE anchor_table_schema = 'analytics';

-- Validate statistics
SELECT VALIDATE_STATISTICS('analytics.fact_orders');
```

---

## 15. Analyzing Workloads

### 15.1 Workload Analyzer

The `ANALYZE_WORKLOAD` function analyzes system tables to detect root causes of poor query performance and returns actionable recommendations:

```sql
-- Run workload analysis (analyzes last hour by default)
SELECT ANALYZE_WORKLOAD('');

-- Analyze specific time range
SELECT ANALYZE_WORKLOAD('', INTERVAL '6 hours');

-- View recommendations
SELECT time, event_type, event_description, suggested_action
FROM v_monitor.workload_events
ORDER BY time DESC;
```

**Common recommendation categories:**
- Missing or stale statistics.
- Projections not covering frequent queries.
- Resegmentation in hot query paths.
- Resource pool tuning.
- Encoding improvements.

### 15.2 Query Pattern Analysis

```sql
-- Most frequent queries
SELECT request_label, COUNT(*) AS execution_count,
       AVG(query_duration_us / 1000.0) AS avg_ms,
       MAX(query_duration_us / 1000.0) AS max_ms
FROM v_monitor.query_requests
WHERE start_timestamp > CURRENT_DATE - 7
GROUP BY request_label
ORDER BY execution_count DESC
LIMIT 20;

-- Slowest queries
SELECT user_name, start_timestamp,
       query_duration_us / 1000.0 AS duration_ms,
       request_label, LEFT(request, 200) AS query_snippet
FROM v_monitor.query_requests
WHERE start_timestamp > CURRENT_DATE - 1
ORDER BY query_duration_us DESC
LIMIT 20;

-- Queries by resource pool
SELECT pool_name,
       COUNT(*) AS query_count,
       AVG(query_duration_us / 1000.0) AS avg_ms
FROM v_monitor.query_requests
WHERE start_timestamp > CURRENT_DATE - 1
GROUP BY pool_name
ORDER BY avg_ms DESC;
```

---

## 16. Monitoring

### 16.1 Node and Cluster Health

```sql
-- Overall cluster health
SELECT is_ok, node_count, current_epoch, ahm_epoch,
       designed_fault_tolerance, current_fault_tolerance
FROM v_monitor.system;

-- Node states
SELECT node_name, node_state, node_address,
       catalog_revision_number, is_primary
FROM v_monitor.nodes;

-- Active sessions per node
SELECT node_name, COUNT(*) AS active_sessions
FROM v_monitor.sessions
WHERE is_active = TRUE
GROUP BY node_name;
```

### 16.2 Resource Usage

```sql
-- Memory usage per node
SELECT node_name, memory_size_kb, memory_inuse_kb,
       ROUND(100.0 * memory_inuse_kb / memory_size_kb, 1) AS pct_used
FROM v_monitor.resource_usage;

-- CPU and I/O per node
SELECT node_name, cpu_usage_pct, io_read_kbytes_per_sec,
       io_write_kbytes_per_sec, net_rx_kbytes_per_sec, net_tx_kbytes_per_sec
FROM v_monitor.host_resources;

-- Resource pool status
SELECT pool_name, memory_size_kb, memory_inuse_kb,
       running_query_count, queued_query_count
FROM v_monitor.resource_pool_status;
```

### 16.3 Disk Space Monitoring

```sql
-- Disk usage per storage location
SELECT node_name, location_path,
       disk_space_total_mb,
       disk_space_used_mb,
       disk_space_free_mb,
       ROUND(100.0 * disk_space_used_mb / disk_space_total_mb, 1) AS pct_used
FROM v_catalog.storage_locations
ORDER BY pct_used DESC;

-- Database total storage
SELECT SUM(used_bytes) / 1e12 AS total_used_tb
FROM v_monitor.projection_storage;

-- Top tables by storage
SELECT anchor_table_schema, anchor_table_name,
       SUM(used_bytes) / 1e9 AS used_gb
FROM v_monitor.projection_storage
GROUP BY 1, 2
ORDER BY used_gb DESC
LIMIT 20;
```

### 16.4 Query Monitoring

```sql
-- Currently running queries
SELECT session_id, user_name, start_timestamp,
       DATEDIFF('second', start_timestamp, NOW()) AS running_s,
       LEFT(current_statement, 300) AS query
FROM v_monitor.sessions
WHERE current_statement IS NOT NULL
ORDER BY running_s DESC;

-- Queued queries (waiting for resources)
SELECT session_id, user_name, pool_name, queue_entry_timestamp
FROM v_monitor.sessions
WHERE query_start IS NULL AND current_statement IS NOT NULL;
```

### 16.5 Log Files

Vertica maintains logs in the database catalog directory:

| Log file | Location | Content |
|---|---|---|
| `vertica.log` | `<catalog_dir>/<db>/<node>/` | Main database log: startup, shutdown, errors, TM activity |
| `UDxLogs/` | Same | User-defined function logs |
| `debuglog` | Same | Detailed debug output (usually disabled) |

```bash
# View recent errors in vertica.log
grep -i "error\|fatal\|panic" /home/dbadmin/mydb/v_mydb_node0001_catalog/vertica.log | tail -100

# Watch log in real time
tail -f /home/dbadmin/mydb/v_mydb_node0001_catalog/vertica.log
```

### 16.6 Events and Alerts

```sql
-- View system events (errors, warnings, notices)
SELECT event_timestamp, node_name, event_severity,
       event_category, message
FROM v_monitor.system_events
WHERE event_timestamp > NOW() - INTERVAL '1 hour'
ORDER BY event_timestamp DESC;

-- Resource pool events (throttling, timeouts)
SELECT * FROM v_monitor.resource_rejections
ORDER BY rejected_at DESC LIMIT 50;
```

---

## 17. Backup and Restore

### 17.1 vbr Tool Overview

`vbr` (Vertica Backup and Restore) is the primary backup utility. It uses an `.ini` configuration file for all backup settings.

**Backup types:**

| Type | Description |
|---|---|
| Full backup | Complete snapshot of the entire database |
| Incremental backup | Only changed data since last backup |
| Object-level backup | Individual schemas or tables |
| Hard-link local backup | Optimized local backup using hard links |

**Supported destinations:**

| Destination | Example |
|---|---|
| Local filesystem | `/backup/vertica/mydb` |
| Remote filesystem | NFS mount |
| AWS S3 | `s3://my-bucket/vertica-backup/` |
| S3-compatible | MinIO, Ceph |
| Google Cloud Storage | `gs://my-bucket/vertica-backup/` |
| Azure Blob Storage | Azure connection string |

### 17.2 Backup Configuration File

```ini
; mydb_backup.ini

[Misc]
snapshotName = mydb_daily_backup
verticaConfig = True              ; include catalog/config in backup

[Database]
dbName = mydb
dbUser = dbadmin
dbPassword = secret

[Transmission]
; For local/NFS backup:
backupDir = /backup/vertica/mydb
; For S3:
; cloudProvider = S3
; bucketName = my-s3-bucket
; region = us-east-1
; keyID = <AWS_ACCESS_KEY>
; secretKey = <AWS_SECRET>

[Backup]
; Full backup settings
objects =                         ; empty = full database backup
retainedCopies = 3                ; keep last 3 full backups

[Restore]
restorePointLimit = 5             ; keep 5 restore points
```

### 17.3 Backup Commands

```bash
# Full database backup
vbr --task backup --config-file mydb_backup.ini

# Incremental backup (only changed data)
vbr --task backup --config-file mydb_backup.ini --is-incremental

# Object-level backup (specific schema)
vbr --task backup --config-file mydb_backup.ini \
    --backup-objects "analytics"

# Object-level backup (specific table)
vbr --task backup --config-file mydb_backup.ini \
    --backup-objects "analytics.fact_orders"

# List available backup restore points
vbr --task listbackup --config-file mydb_backup.ini
```

### 17.4 Restore Commands

```bash
# Full database restore
vbr --task restore --config-file mydb_backup.ini

# Restore from a specific restore point
vbr --task restore --config-file mydb_backup.ini \
    --restore-point-id <backup_snapshot_id>

# Object-level restore (specific table)
vbr --task restore --config-file mydb_backup.ini \
    --restore-objects "analytics.fact_orders"

# Restore to a different cluster (database copy/clone)
vbr --task copycluster --config-file mydb_copy.ini
```

### 17.5 Key Constraints

- You **cannot** backup an Enterprise Mode database and restore it as Eon Mode (and vice versa).
- You **cannot** transfer backups between cloud providers (e.g., GCS → S3).
- Backup security: restrict backup directory access to users with full database access — an unencrypted backup is equivalent to full database access.

---

## 18. Failure Recovery

### 18.1 K-Safety and Node Failures

With K-safety = 1:
- **One node fails**: database continues normally. Surviving nodes handle twice their normal query workload. The failed node's projections are served from buddy copies on other nodes.
- **Two nodes fail simultaneously**: database shuts down (K-safety exceeded).

With K-safety = 0:
- Any single node failure shuts down the database.

### 18.2 Recovery Scenarios

**Scenario 1: Node fails, database stays up (K ≥ 1)**

The failed node shows as `DOWN`. When it restarts, it enters `RECOVERING` state:

```bash
# Restart the failed node
admintools -t restart_node -d mydb -s <node_ip>
```

During recovery, the node replays missed transactions from buddy projections. It returns to `UP` status when fully synchronized. Active queries are not interrupted during node recovery (except briefly at the end).

**Scenario 2: Database was shut down after node failures**

Restart normally — Vertica reads the Last Good Epoch and reconstructs state from there:

```bash
admintools -t start_db -d mydb
```

**Scenario 3: Unclean shutdown (crash/power failure)**

```bash
# Allow manual recovery if K nodes are still DOWN
admintools -t start_db -d mydb --no-recovery
# (use carefully — may start with incomplete data if down nodes had unique data)

# Force recovery from a specific epoch
admintools -t start_db -d mydb --force
```

**Scenario 4: Eon Mode Read-Only recovery**

When primary nodes are lost in Eon Mode, the database enters read-only mode. Write operations fail until primary nodes are restored or a new primary set is established.

### 18.3 Checking Recovery Status

```sql
-- Node recovery progress
SELECT node_name, node_state, recovery_status
FROM v_monitor.nodes;

-- Detailed recovery status
SELECT * FROM v_monitor.recovery_status;

-- Active recovery operations
SELECT node_name, start_time, is_running, phase_name
FROM v_monitor.tuple_mover_operations
WHERE operation = 'Recovery'
ORDER BY start_time DESC;
```

### 18.4 Last Good Epoch

Vertica uses the **Last Good Epoch (LGE)** to identify the most recent consistent database state after an unclean shutdown. Data committed after LGE may be lost in a crash scenario.

```sql
SELECT last_good_epoch, ahm_epoch, current_epoch
FROM v_monitor.system;
```

When recovering manually, Vertica rolls back any uncommitted transactions (those after LGE) to restore consistency.

---

## 19. Profiling Database Performance

### 19.1 Profiling Levels

| Level | Command | Data stored in |
|---|---|---|
| **Statement** | `PROFILE SELECT ...` | `QUERY_PROFILES`, `QUERY_PLAN_PROFILES`, `EXECUTION_ENGINE_PROFILES` |
| **Session** | `SELECT ENABLE_PROFILING('query')` | Same tables, filtered by session |
| **Global** | `ALTER DATABASE ... SET GlobalQueryProfiling = 1` | Same tables, all sessions |

### 19.2 Enabling Profiling

```sql
-- Statement profiling (prefix query with PROFILE)
PROFILE SELECT u.country, SUM(o.amount)
FROM analytics.fact_orders o
JOIN analytics.dim_users u ON o.user_id = u.user_id
GROUP BY u.country;

-- Session profiling
SELECT ENABLE_PROFILING('query');         -- query plan and timing
SELECT ENABLE_PROFILING('session');       -- session-level stats
SELECT ENABLE_PROFILING('ee');            -- execution engine (memory, CPU per operator)

SELECT DISABLE_PROFILING('query');        -- disable when done

-- Global profiling (all sessions)
ALTER DATABASE DEFAULT SET PARAMETER GlobalQueryProfiling = 1;
ALTER DATABASE DEFAULT SET PARAMETER GlobalEEProfiling = 1;

-- Disable global profiling
ALTER DATABASE DEFAULT SET PARAMETER GlobalQueryProfiling = 0;
```

### 19.3 Reading Profile Data

```sql
-- Query duration and basic stats
SELECT transaction_id, statement_id, user_name,
       query_duration_us / 1000.0 AS duration_ms,
       query_start, query_type
FROM v_monitor.query_profiles
ORDER BY query_start DESC LIMIT 10;

-- Query plan with runtime statistics
SELECT node_name, path_id, path_line, counter_name, counter_value
FROM v_monitor.query_plan_profiles
WHERE transaction_id = <txn_id>
  AND statement_id   = <stmt_id>
ORDER BY path_id, path_line;

-- Execution engine metrics per operator (memory, rows, CPU)
SELECT node_name, operator_name, execution_time_ns,
       rows_produced, memory_reserved_kb, memory_allocated_kb
FROM v_monitor.execution_engine_profiles
WHERE transaction_id = <txn_id>
  AND statement_id   = <stmt_id>
ORDER BY execution_time_ns DESC;

-- Resource consumption summary
SELECT node_name, statement_id,
       memory_allocated_kb, thread_count,
       rows_read, network_bytes_received
FROM v_monitor.query_consumption
WHERE transaction_id = <txn_id>;
```

### 19.4 Profiling Precedence

Statement profiling > Session profiling > Global profiling. The highest applicable level wins.

---

## 20. Diagnostic Tools

### 20.1 Scrutinize

`scrutinize` collects comprehensive diagnostic data from all cluster nodes into a single archive:

```bash
# Full diagnostic collection
/opt/vertica/bin/scrutinize

# Custom collection window
/opt/vertica/bin/scrutinize --log-ages 2h

# Limit to specific nodes
/opt/vertica/bin/scrutinize --nodes v_mydb_node0001,v_mydb_node0002

# Output to specific location
/opt/vertica/bin/scrutinize --output-file /tmp/mydb_diagnostics.tar.gz
```

Scrutinize collects: logs, configuration files, system table snapshots, resource usage data, and catalog information. Useful for escalating issues to Vertica Support.

### 20.2 Data Integrity Checks

```sql
-- Full consistency check
SELECT CHECK_DB_CONSISTENCY();

-- Check only specific aspects
SELECT CHECK_DB_CONSISTENCY('CRC');          -- storage block checksums
SELECT CHECK_DB_CONSISTENCY('SORT');         -- projection sort order
SELECT CHECK_DB_CONSISTENCY('CATALOG');      -- catalog integrity

-- Check specific table
SELECT CHECK_PARTITIONS('analytics.fact_orders');

-- Verify cluster metadata
SELECT VERIFY_CLUSTER_METADATA();

-- Verify node catalog version
SELECT VERIFY_NODE_METADATA('v_mydb_node0001');
```

### 20.3 Getting Vertica Version

```sql
SELECT VERSION();
SELECT * FROM v_catalog.license_audits LIMIT 1;
```

```bash
/opt/vertica/bin/vertica --version
```

### 20.4 Catalog Export

```bash
# Export database catalog (for support analysis)
/opt/vertica/bin/admintools -t export_catalog -d mydb -p <password>
```

---

## 21. Managing Client Connections

### 21.1 Connection Limits

```sql
-- Set max connections per node (database-level)
ALTER DATABASE DEFAULT SET PARAMETER MaxClientSessions = 200;

-- Set per-user connection limit
ALTER USER alice MAXCONNECTIONS 10 ON DATABASE;
ALTER USER alice MAXCONNECTIONS 5 ON NODE;  -- per-node limit

-- Remove connection limit
ALTER USER alice MAXCONNECTIONS DEFAULT;
```

### 21.2 Idle Connection Timeout

```sql
-- Disconnect idle sessions after 10 minutes
ALTER DATABASE DEFAULT SET PARAMETER DefaultIdleSessionTimeout = 600;

-- Per-user idle timeout
ALTER USER alice IDLESESSIONTIMEOUT INTERVAL '30 minutes';
```

### 21.3 Connection Load Balancing

Vertica can distribute connections across cluster nodes:

```sql
-- Enable native connection load balancing
ALTER DATABASE DEFAULT SET PARAMETER ConnectionLoadBalancing = 1;

-- View load balancing policies
SELECT * FROM v_catalog.load_balance_groups;

-- Create a load balancing policy
CREATE LOAD BALANCE GROUP analytics_lb
    WITH ADDRESS 'node1_ip','node2_ip','node3_ip'
    POLICY 'ROUNDROBIN';
```

### 21.4 TCP Keepalive

```sql
-- Detect unresponsive clients
ALTER DATABASE DEFAULT SET PARAMETER SocketKeepaliveIdle = 60;    -- seconds idle before probing
ALTER DATABASE DEFAULT SET PARAMETER SocketKeepaliveInterval = 10; -- probe interval
ALTER DATABASE DEFAULT SET PARAMETER SocketKeepaliveCount = 5;     -- probes before disconnect
```

### 21.5 HBA and Authentication

Vertica uses `pg_hba.conf`-style host-based authentication:

```bash
# Location (on each node)
<catalog_dir>/<db>/<node>/pg_hba.conf
```

```
# Format: method   address   database   user   auth-method
host    all        all        dbadmin    md5
host    analytics  analyst    10.0.0.0/8 ldap
local   all        all        all        trust
```

Supported authentication methods: `trust`, `reject`, `md5`, `sha512`, `ldap`, `kerberos`, `gss`, `ident`.

---

## 22. Quick Reference: Key System Tables

### v_catalog (schema/metadata)

| Table | Content |
|---|---|
| `v_catalog.tables` | All user tables |
| `v_catalog.columns` | Column definitions |
| `v_catalog.projections` | Projection metadata |
| `v_catalog.projection_columns` | Per-column sort, encoding, NULL order |
| `v_catalog.schemata` | Schemas |
| `v_catalog.users` | User accounts |
| `v_catalog.roles` | Defined roles |
| `v_catalog.grants` | Privilege grants |
| `v_catalog.nodes` | Cluster node definitions |
| `v_catalog.storage_locations` | Storage paths per node |
| `v_catalog.table_partitions` | Partition metadata |
| `v_catalog.directed_queries` | Saved query plans |
| `v_catalog.column_statistics` | Statistics metadata |

### v_monitor (runtime monitoring)

| Table | Content |
|---|---|
| `v_monitor.system` | Cluster-level health, epoch state |
| `v_monitor.nodes` | Node states and addresses |
| `v_monitor.sessions` | Active sessions and running queries |
| `v_monitor.query_requests` | Query history with timing |
| `v_monitor.query_events` | Optimizer warnings and suggestions |
| `v_monitor.query_profiles` | Query duration and type |
| `v_monitor.query_plan_profiles` | Real-time plan step metrics |
| `v_monitor.execution_engine_profiles` | Per-operator CPU, memory, rows |
| `v_monitor.query_consumption` | Resource use per statement |
| `v_monitor.resource_pool_status` | Memory/concurrency per pool |
| `v_monitor.resource_rejections` | Queries rejected by resource pools |
| `v_monitor.projection_storage` | ROS storage per projection |
| `v_monitor.delete_vectors` | Unreclaimed deleted rows |
| `v_monitor.partitions` | Partition ROS stats |
| `v_monitor.tuple_mover_operations` | TM operation history |
| `v_monitor.tuple_mover_pool_status` | TM pool status |
| `v_monitor.locks` | Active database locks |
| `v_monitor.lock_attempts` | Blocked lock requests |
| `v_monitor.host_resources` | CPU, I/O, network per node |
| `v_monitor.resource_usage` | Memory usage per node |
| `v_monitor.projection_refreshes` | Refresh status per projection |
| `v_monitor.rebalance_projection_status` | Rebalance progress |
| `v_monitor.recovery_status` | Node recovery details |
| `v_monitor.workload_events` | Workload Analyzer recommendations |
| `v_monitor.system_events` | System-level errors and notices |

### Data Collector (dc_*)

| Table | Content |
|---|---|
| `dc_requests_issued` | All SQL statements with session/node context |
| `dc_requests_completed` | Completed statements with row counts |
| `dc_session_starts` | Session open events |
| `dc_session_ends` | Session close events |

**Critical rule for DC tables:** always join on both `session_id` AND `node_name` to avoid RESEGMENT; always filter on `time` with static `::TIMESTAMPTZ` literals.

```sql
-- Template: slow query diagnostics from DC tables
SELECT
    ri.user_name,
    ri.session_id,
    DATEDIFF('millisecond', ri.time, rc.time) AS duration_ms,
    rc.processed_row_count,
    ri.request
FROM dc_requests_issued  ri
JOIN dc_requests_completed rc
  ON  ri.session_id = rc.session_id
  AND ri.node_name  = rc.node_name        -- always include node_name
WHERE ri.time > '2026-05-15 00:00:00'::TIMESTAMPTZ   -- static literal
  AND ri.time < '2026-05-16 00:00:00'::TIMESTAMPTZ
ORDER BY duration_ms DESC
LIMIT 50;
```
