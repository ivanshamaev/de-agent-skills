# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repository Is

A collection of **Claude Code agent skills** for data engineering topics. Skills are loaded by the Claude Agent SDK harness at runtime to give Claude specialized domain knowledge and behavioral rules for a specific technology or task. There is no build system, test runner, or application code — the repository is purely documentation-as-skills.

## Repository Layout

```
skills/<name>/SKILL.md      — skill definitions consumed by the agent harness
docs/specs/<name>.md        — detailed enterprise specs referenced by skills
guides/<name>.md            — tutorial-style narrative guides (Russian, for humans)
```

Skills, specs, and guides are related but serve different audiences:
- `skills/` — concise, prescriptive instruction files loaded directly into the agent context.
- `docs/specs/` — verbose enterprise specifications with deep rationale; skills cross-reference these.
- `guides/` — human-readable tutorials written in Russian that synthesize the specs.

## Skill File Format

Every skill is a single `SKILL.md` with YAML frontmatter:

```markdown
---
name: <kebab-case-slug>
description: <one-sentence trigger description used by the harness to decide when to load this skill>
---

# <Title>

## When to Use
...
```

The `description` field is the most critical part — the harness uses it to match user intent to the right skill. Keep it specific and keyword-rich so it activates on the right requests and not on unrelated ones.

A skill must be self-contained: it cannot assume other skills are loaded simultaneously. Cross-references to `docs/specs/` are allowed in a "References to Consult When Needed" section at the bottom.

## Adding a New Skill

1. Create `skills/<name>/SKILL.md` — follow the structure of an existing skill such as `skills/spark_sql/SKILL.md`.
2. Include these sections in order: **When to Use**, **Core Workflow** (or equivalent), technology-specific content sections, **Anti-Patterns**, **Output Expectations**, optionally **References to Consult When Needed**.
3. Write in English.
4. Make examples production-quality — not toy snippets.

## Existing Skills

| Skill | Topic |
|-------|-------|
| `spark_sql` | Spark SQL for Hive/HDFS/lakehouse — queries, DDL, writes, performance |
| `pyspark_etl` | PySpark DataFrame pipelines — transforms, joins, writes, Spark performance |
| `vertica` | Vertica SQL — DDL, DML, CRUD, projections, segmentation, update strategies |
| `vertica_query_optimization` | Vertica 11.x query performance — EXPLAIN, projection design, join/GROUP BY/ORDER BY tuning, encoding, RLE, Data Collector diagnostics |
| `airflow_dag_factory` | Airflow DAG Factory (dag-factory v1.0+) — YAML DAG authoring, defaults hierarchy, dynamic mapping, datasets, callbacks, TaskFlow, env vars, DRY anchors, large-scale generation, CI/CD linting |
| `trino_iceberg` | Trino + Apache Iceberg — table DDL, partition transforms, sorted tables, DML/MERGE, EXPLAIN plan reading, join optimization, ANALYZE, table maintenance (optimize/expire_snapshots/remove_orphan_files), schema evolution, time travel, metadata table diagnostics |

## Existing Specs

| Spec | Topic |
|------|-------|
| `vertica_query_optimization_v11.md` | Vertica 11.x query optimization — EXPLAIN tokens, projection design, join/GROUP BY/ORDER BY, encoding, dc_* tables |
| `vertica_admin_guide_v24.md` | Vertica 24.3.x administration — architecture, users/roles, projections, partitioning, COPY, resource pools, backup, monitoring |
| `trino_iceberg_performance_optimization.md` | Trino + Iceberg — optimizer, CBO, join strategies, pushdown, partitioning, sorted tables, maintenance, time travel, schema evolution |

## Docs/Specs Relationship

Skills reference specs when detailed rationale would bloat the skill file. For example, `skills/spark_sql/SKILL.md` references `docs/specs/spark_sql_hdfs_hive_operations.md` for HDFS/Hive DDL detail. When editing a skill, check whether the underlying spec needs updating too.
