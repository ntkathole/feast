# 2. Offline and Online Store Abstraction Layer

Date: 2026-02-17

## Status

Accepted (retroactive documentation of existing architecture)

## Context

Feast needs to support multiple data backends (BigQuery, Snowflake, Redshift, Spark,
Postgres, DynamoDB, Redis, Bigtable, SQLite, etc.) while providing a consistent API
to ML engineers. Teams should be able to switch infrastructure without rewriting
feature engineering pipelines.

## Decision

Feast uses an abstract provider/store pattern:
- `OfflineStore` base class (`sdk/python/feast/infra/offline_stores/`) defines the
  interface for historical feature retrieval
- `OnlineStore` base class (`sdk/python/feast/infra/online_stores/`) defines the
  interface for low-latency feature serving
- `Provider` orchestrates materialization between offline and online stores
- Each backend implements these interfaces independently

## Consequences

- Adding a new backend requires implementing a well-defined interface
- ML engineers interact with a single `FeatureStore` API regardless of backend
- Testing can run against SQLite/file-based stores locally while production uses
  cloud-scale backends
- Some advanced backend-specific features may not fit the generic interface cleanly
