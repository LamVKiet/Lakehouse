---
description: "Medallion Architecture — Bronze/Silver/Gold on Delta Lake + S3, event schema, data flow"
---

# System Architecture

Medallion Architecture (Bronze / Silver / Gold) on Delta Lake + S3.
Two sources: Kafka clickstream (streaming) + MySQL ERP (T-1 batch).

## Sub-files

- [overview.md](architecture/overview.md) — Data flow diagram, Kafka layer, event schema (14 types), traffic ratio
- [bronze-layer.md](architecture/bronze-layer.md) — Bronze schemas: behavior_events + 6 MySQL batch tables
- [silver-layer.md](architecture/silver-layer.md) — Silver schemas: 8 tables (events, nou, transactions, dims)
- [gold-layer.md](architecture/gold-layer.md) — Gold schemas: 5 tables (events_discovery/cart/checkout, sales, aging)
