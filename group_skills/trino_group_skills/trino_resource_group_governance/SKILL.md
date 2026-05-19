---
name: trino-resource-group-governance
description: Trino resource group workload governance — resource-groups.properties configuration (maxQueued/hardConcurrencyLimit/softMemoryLimit/hardCpuLimit/schedulingPolicy/schedulingWeight), hierarchical group trees, selector rules (user/source/queryType/clientTags/regex), multi-tenant tenant isolation patterns, per-user dynamic sub-groups (${USER} template), scheduling policies (fair/weighted_fair/weighted/query_priority), CPU quota periods, database-backed configuration (MySQL/PostgreSQL), JMX monitoring of group utilization
---

# Trino Resource Group Governance

## When to Use

- Multiple teams share one Trino cluster and need query isolation
- Some workloads (ETL pipelines) should not crowd out interactive (BI/adhoc) queries
- Enforcing memory limits or concurrency caps per team or query type
- Setting SLA tiers for critical vs non-critical workloads
- Preventing a single user from monopolizing cluster resources

---

## Configuration Files

```properties
# etc/config.properties — point to resource groups config file
resource-groups.config-file=etc/resource-groups.json

# Optional: use database for dynamic reconfiguration (reloads every ~1s)
# resource-groups.config-file=etc/resource-groups.json    # file-based
```

---

## Complete Multi-Tenant Resource Groups Config

```json
{
  "rootGroups": [
    {
      "name": "global",
      "softMemoryLimit": "80%",
      "hardConcurrencyLimit": 200,
      "maxQueued": 500,
      "schedulingPolicy": "weighted",
      "subGroups": [

        {
          "name": "admin",
          "softMemoryLimit": "100%",
          "hardConcurrencyLimit": 20,
          "maxQueued": 10,
          "schedulingPolicy": "query_priority",
          "schedulingWeight": 10,
          "jmxExport": true
        },

        {
          "name": "pipeline",
          "softMemoryLimit": "60%",
          "hardConcurrencyLimit": 60,
          "maxQueued": 200,
          "schedulingPolicy": "weighted_fair",
          "schedulingWeight": 3,
          "softCpuLimit": "2h",
          "hardCpuLimit": "3h",
          "jmxExport": true,
          "subGroups": [
            {
              "name": "pipeline_${USER}",
              "softMemoryLimit": "20%",
              "hardConcurrencyLimit": 8,
              "maxQueued": 50,
              "schedulingPolicy": "fair"
            }
          ]
        },

        {
          "name": "adhoc",
          "softMemoryLimit": "30%",
          "hardConcurrencyLimit": 80,
          "maxQueued": 300,
          "schedulingPolicy": "weighted_fair",
          "schedulingWeight": 2,
          "jmxExport": true,
          "subGroups": [
            {
              "name": "adhoc_${USER}",
              "softMemoryLimit": "10%",
              "hardConcurrencyLimit": 3,
              "maxQueued": 20,
              "schedulingPolicy": "fair"
            }
          ]
        },

        {
          "name": "batch",
          "softMemoryLimit": "50%",
          "hardConcurrencyLimit": 20,
          "maxQueued": 100,
          "schedulingPolicy": "fair",
          "schedulingWeight": 1,
          "softCpuLimit": "4h",
          "hardCpuLimit": "8h",
          "jmxExport": true
        },

        {
          "name": "reporting",
          "softMemoryLimit": "40%",
          "hardConcurrencyLimit": 30,
          "maxQueued": 100,
          "schedulingPolicy": "weighted_fair",
          "schedulingWeight": 2,
          "jmxExport": true,
          "subGroups": [
            {
              "name": "reporting_${USER}",
              "softMemoryLimit": "15%",
              "hardConcurrencyLimit": 5,
              "maxQueued": 20
            }
          ]
        }

      ]
    }
  ],

  "selectors": [
    {
      "user": "admin",
      "group": "global.admin"
    },
    {
      "source": ".*airflow.*",
      "queryType": "DATA_DEFINITION",
      "group": "global.batch"
    },
    {
      "source": ".*airflow.*",
      "group": "global.pipeline.pipeline_${USER}"
    },
    {
      "source": ".*dbt.*",
      "group": "global.pipeline.pipeline_${USER}"
    },
    {
      "source": ".*superset.*",
      "group": "global.reporting.reporting_${USER}"
    },
    {
      "clientTags": ["batch"],
      "group": "global.batch"
    },
    {
      "clientTags": ["adhoc"],
      "group": "global.adhoc.adhoc_${USER}"
    },
    {
      "user": ".*",
      "group": "global.adhoc.adhoc_${USER}"
    }
  ],

  "cpuQuotaPeriod": "1h"
}
```

---

## Resource Group Properties Reference

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | string | ✓ | Group identifier; supports `${USER}` and `${SOURCE}` templates |
| `hardConcurrencyLimit` | int | ✓ | Max running queries (hard limit — queries fail above this) |
| `maxQueued` | int | ✓ | Max queued queries (new queries rejected when full) |
| `softMemoryLimit` | string | | Memory queue threshold (% or absolute, e.g. `10%`, `50GB`) |
| `softConcurrencyLimit` | int | | Running query threshold for weighted peer selection |
| `schedulingPolicy` | string | | `fair`, `weighted_fair`, `weighted`, `query_priority` |
| `schedulingWeight` | int | | Priority multiplier for weighted policies (default: 1) |
| `softCpuLimit` | duration | | CPU time threshold; requires `hardCpuLimit` |
| `hardCpuLimit` | duration | | Max CPU time per `cpuQuotaPeriod`; queries queued above limit |
| `hardPhysicalDataScanLimit` | string | | Max physical bytes scanned per query |
| `jmxExport` | bool | | Expose metrics via JMX |

---

## Scheduling Policies Explained

| Policy | Behavior | Best For |
|--------|----------|---------|
| `fair` | FIFO within group, round-robin across sub-groups | Simple queue, equal treatment |
| `weighted` | Stochastic proportional to `schedulingWeight` | Prioritizing one workload class |
| `weighted_fair` | Based on weight + concurrent query ratio | Multi-tenant fair share |
| `query_priority` | Strict priority from query's `priority` attribute | SLA tiers where admin must always win |

```sql
-- Set query priority (used by query_priority policy)
SET SESSION query_priority = 10;   -- higher = more important
```

---

## Selector Rules

Selectors match queries to resource groups. All conditions in a selector use AND logic. First matching selector wins.

```json
"selectors": [
  {
    "user": "etl_service",                     -- exact user match
    "group": "global.pipeline.pipeline_etl_service"
  },
  {
    "source": ".*dbt-cloud.*",                 -- regex on source string
    "queryType": "SELECT",
    "group": "global.adhoc.adhoc_${USER}"
  },
  {
    "clientTags": ["high-priority", "etl"],   -- ALL tags must be present
    "group": "global.pipeline.pipeline_${USER}"
  },
  {
    "queryText": ".*iceberg\\.gold\\..*",      -- regex on SQL text
    "group": "global.reporting.reporting_${USER}"
  }
]
```

**Query type values**: `SELECT`, `INSERT`, `DELETE`, `UPDATE`, `ANALYZE`, `DATA_DEFINITION`, `EXPLAIN`.

**Setting client tags from JDBC:**

```java
Properties props = new Properties();
props.setProperty("user", "my_user");
props.setProperty("clientTags", "high-priority,etl");   // comma-separated
Connection conn = DriverManager.getConnection("jdbc:trino://coordinator:8080", props);
```

**Setting source via CLI:**

```bash
trino --server http://coordinator:8080 \
      --user dbt_user \
      --source "dbt-cloud-production"
```

---

## CPU Quota Enforcement

CPU quotas throttle groups that exceed CPU time budgets per period:

```json
{
  "cpuQuotaPeriod": "1h",
  "subGroups": [
    {
      "name": "batch",
      "softCpuLimit": "30m",     // start queuing above 30 min CPU/hour
      "hardCpuLimit": "60m"      // hard stop at 60 min CPU/hour
    }
  ]
}
```

- Queries above `softCpuLimit` are deprioritized (other groups get preference)
- Above `hardCpuLimit`, queries queue until next `cpuQuotaPeriod` resets

---

## Database-Backed Configuration (Dynamic)

For dynamic reconfiguration without restarts (Starburst Enterprise / large deployments):

```properties
# etc/config.properties
resource-groups.config-file=etc/resource-groups.json

# For database-backed (reloads every ~1s from DB)
# Supported: MySQL, PostgreSQL, Oracle
resource-groups.config-db.url=jdbc:postgresql://db:5432/trinoconfig
resource-groups.config-db.user=trino_admin
resource-groups.config-db.password=${ENV:DB_PASSWORD}
```

Tables required: `resource_groups`, `selectors`, `exact_match_source_selectors`.

---

## Monitoring Resource Groups

```bash
# Active group state via JMX (groups with jmxExport: true)
curl -s http://coordinator:8080/v1/jmx/mbean/trino.execution:name=QueryManager \
  | jq .

# Query resource group assignment
curl -s http://coordinator:8080/v1/query/<query_id> \
  | jq '.resourceGroupId'
```

```sql
-- Check current group utilization via JMX connector
SELECT node_id, name, value
FROM jmx.current."trino.execution:name=QueryManager"
WHERE name LIKE '%Running%' OR name LIKE '%Queued%';
```

---

## Typical Isolation Patterns

### Pattern 1: ETL vs Interactive

```json
"subGroups": [
  {"name": "etl",         "hardConcurrencyLimit": 20, "schedulingWeight": 1},
  {"name": "interactive", "hardConcurrencyLimit": 50, "schedulingWeight": 5}
]
```

Interactive queries get 5× more scheduler priority than ETL.

### Pattern 2: Per-Team Isolation

```json
"subGroups": [
  {"name": "team_orders",   "softMemoryLimit": "20%", "hardConcurrencyLimit": 15},
  {"name": "team_marketing","softMemoryLimit": "15%", "hardConcurrencyLimit": 10},
  {"name": "team_finance",  "softMemoryLimit": "20%", "hardConcurrencyLimit": 15}
]
```

Each team has a guaranteed memory budget and max concurrency.

### Pattern 3: Rate Limiting a User

```json
{
  "name": "adhoc_${USER}",
  "hardConcurrencyLimit": 3,       -- user can run at most 3 queries simultaneously
  "maxQueued": 10,                  -- max 10 waiting in queue before rejection
  "softMemoryLimit": "5%",          -- user queues if consuming > 5% cluster memory
  "hardPhysicalDataScanLimit": "1TB"  -- reject queries scanning > 1TB
}
```

---

## Anti-Patterns

1. **Single flat resource group (no hierarchy)** — all queries compete equally; ETL jobs crowd out interactive queries; always build at least two tiers.
2. **`hardConcurrencyLimit` set too high** — allows more queries than workers can handle, causing memory pressure; set based on worker count × 5 (rule of thumb starting point).
3. **Missing catch-all selector** — queries that don't match any selector fail with a routing error; always add a `"user": ".*"` catch-all at the end.
4. **Not using `schedulingWeight`** — with `weighted` policy and all weights = 1, groups compete equally regardless of SLA tier; high-priority groups must have higher weight.
5. **Exporting all groups via JMX** — high cardinality from `${USER}` groups creates thousands of JMX beans; only export named tier-level groups.

---

## References

- Resource groups docs: `trino.io/docs/current/admin/resource-groups.html`
- Starburst resource groups: `docs.starburst.io/latest/admin/resource-groups.html`
- Related skills: `[[trino-admin-cluster-health]]`, `[[trino-memory-and-spill-tuning]]`, `[[trino-production-readiness-review]]`
