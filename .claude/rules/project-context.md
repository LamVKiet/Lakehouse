---
description: "Project role, goals, and scope — Medallion Architecture on AWS S3 + Delta Lake"
---

# Project Context

## Role
Data Engineer designing a data pipeline system with streaming architecture and lakehouse patterns.

## Goals
Build a Real-time Event-driven Data Lakehouse for an Omnichannel Apparel Retail Chain:
- Two data sources: Frontend Clickstream (Kafka streaming) + Backend ERP/Sales (MySQL batch).
- Apache Kafka as the transport backbone for user behavior events.
- Spark Structured Streaming writing to Bronze layer (always-on, 30s trigger).
- Spark Batch processing T-1 day: Bronze -> Silver (dedup + clean) -> Gold (aggregation).
- Delta Lake on AWS S3 as the Lakehouse storage layer.
- AWS Glue Data Catalog as managed metadata store.
- Trino as distributed SQL query engine for Data Analysts.
- Apache Airflow scheduling batch pipeline at 02:00 daily.
- FastAPI data-simulator generating fake clickstream events, decoupled from the producer.

## Business Objective
Unify behavioral intent data (clickstream) with financial truth data (ERP transactions) to enable:
- **Cart Abandonment Rate**: Where do users drop off in the purchase funnel?
- **True Conversion Rate**: What % of browsing sessions result in actual orders?
- **Omnichannel Analysis**: POS (in-store) vs. Online performance comparison.
- **NOU / Retention / Resurrected**: Customer lifecycle segmentation.

## Scope
End-to-end pipeline simulation:
- **Ingestion**: FastAPI simulator (:8000) generates fake clickstream events -> producer.py sends to Kafka.
- **Streaming**: spark-bronze-job runs continuously, writes Bronze Delta tables to S3 every 30s.
- **Batch MySQL->Bronze**: Airflow DAG (02:00 daily) loads T-1 data from MySQL (customers, products, branches, transactions, transaction_details) into Bronze.
- **Batch Bronze->Silver->Gold**: Airflow DAGs transform and aggregate data through medallion layers.
- **Query**: Trino connects to Glue Catalog, DA queries Gold/Silver Delta tables via SQL.

## Design Principles
- Bronze: immutable raw landing zone, preserves JSON metadata string, stores _kafka_offset for reprocessing.
- Silver: cleaned, deduplicated via Delta MERGE INTO, types parsed, nulls filtered out.
- Gold: pre-aggregated, overwrites T-1 partition (idempotent), optimized for DA queries.

## Language
- All code and documentation written in English.
- Conversations with the user conducted in Vietnamese.
