# CLAUDE.md

Real-time Event-driven Data Lakehouse for an Omnichannel Apparel Retail Chain.
Frontend Clickstream (Kafka) + Backend ERP/Sales (MySQL) -> Spark -> Delta Lake on S3.
Query layer: Trino + AWS Glue Data Catalog. Orchestration: Apache Airflow.

See `.claude/rules/` for detailed rules:

- `project-context.md` — Role, goals, scope
- `architecture.md` — Medallion Architecture (Bronze/Silver/Gold), event schema, data flow
- `tech-stack.md` — Versions: Kafka, Spark, Delta Lake, Trino, Airflow, Python, Docker
- `docker-infrastructure.md` — Docker services, ports, Dockerfile strategy
- `kafka-development.md` — Rules for Kafka producer/consumer code
- `spark-delta.md` — Rules for Spark jobs with Delta Lake + S3 + Glue
- `workflow.md` — Startup steps, operation, debug
- `coding-style.md` — Python/Spark/Docker code conventions
- `debugging-workflow.md` — Debug process: diagnose before patch
- `lakehouse-design.md` — Silver/Gold design philosophy: Smart Key, granularity, pre-aggregation

---

## Agent Behavior

- Reply in Vietnamese, write all code and comments in English.
- Do not over-explain unless asked — just do the task, then summarize briefly (what changed, why, root cause if a bug fix).
- Prefer editing existing files over creating new ones.
- When fixing a bug: diagnose root cause first, never guess-and-patch.
- Python style: compact — no unnecessary blank lines, short args stay on one line.
